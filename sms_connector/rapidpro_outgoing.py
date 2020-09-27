import itertools
import json
import time

from requests.exceptions import HTTPError
from temba_client.exceptions import TembaBadRequestError, TembaRateExceededError

from lib.pubsub_util import Subscriber, MessageSequencer
from lib.simple_logger import Logger
from lib.utils import utcnow

log = None
phone_number_uuid_table = None
rapidpro_client = None
rapidpro_lock = None
subscriber = None
sequencer = None
counter = None

# There are times that rapidpro responds with an error, but has actually sent the SMS.
# Because of this, we retry slowly so that a human can intervene before too many SMS have been sent.
retry_wait_times = [4, 16, 32]

# This contains timestamps for each of the last rapidpro call failures.
last_failure_tokens = []


def init(crypto_token_path, rp_client, rp_lock, lookup_table, topic_name = "sms-outgoing"):
    global log, rapidpro_client, rapidpro_lock, phone_number_uuid_table, subscriber, sequencer, counter

    if log is None:
        log = Logger(__name__)

    log.info("Init outgoing")
    rapidpro_client = rp_client
    rapidpro_lock = rp_lock
    phone_number_uuid_table = lookup_table
    sequencer = MessageSequencer(process_message_impl)
    subscriber = Subscriber(crypto_token_path, topic_name, f"{topic_name}-subscription", sequencer.process_message)


def check_exception():
    """If there is a message processing exception, raise it."""
    if sequencer.last_exception is not None:
        raise sequencer.last_exception


def teardown():
    log.info("canceling outgoing subscription")
    subscriber.cancel()
    log.info("teardown complete")


def process_message_impl(message):
    """Called on a background thread once for each message.
    It is the responsibility of the caller to gracefully handle exceptions"""
    log.debug(f"Processing: {message}")

    data_map = json.loads(message.data)['payload']
    log.notify(f"pubsub: processing {json.dumps(data_map)}")

    assert "action" in data_map.keys()
    action = data_map["action"]

    if action == "send_messages":
        assert "ids" in data_map.keys()
        assert "messages" in data_map.keys()

        log.audit(f"rapidpro: send_messages {json.dumps(data_map)}")

        # {
        #   "action" : "send_messages"
        #   "ids" : [ "nook-uuid-23dsa" ],
        #   "messages" : [ "🐱" ]
        # }

        # TODO: Handle lookup failures
        mappings = phone_number_uuid_table.uuid_to_data_batch(data_map["ids"])

        # HACK: Filter out urns that don't start with "tel:+" as
        # RapidPro sometimes crashes on sending messages to them
        # These are working phone numbers though, and we can receive
        # messages from them, so the issue has been raised with RapidPro
        dirty_urns = list(mappings.values())

        urns = []

        for urn in dirty_urns:
            if not urn.find("tel:+") >= 0:
                print (f"WARNING: SKIPPING SEND TO bad {urn}")
                continue
            urns.append(urn)

        # Break into groups of 100
        urn_groups = []
        group_start = 0
        group_end = 100
        while group_end < len(urns):
            urn_groups.append(urns[group_start:group_end])
            group_start = group_end
            group_end += 100
        urn_groups.append(urns[group_start:])

        # Assert that groups contain all of the original urns
        assert set(urns) == set(itertools.chain.from_iterable(urn_groups))

        group_num = 0
        while len(urn_groups) > 0:
            group_num += 1
            urns = urn_groups.pop(0)
            for text in data_map["messages"]:
                retry_count = 0
                while True:
                    log.debug(f"sending group {group_num}: {len(urns)} sms")
                    try:
                        with rapidpro_lock:
                            rapidpro_client.send_message_to_urns(text, urns, interrupt=True)
                        log.debug(f"sent {len(urns)} sms")
                        # in addition to notifying about the send_message command
                        # notify for each URN so we can get a view of how many people are being messaged
                        # send successful - exit loop
                        break
                    except HTTPError as e:
                        retry_exception = e
                        # fall through to retry
                    except TembaRateExceededError as e:
                        retry_exception = e
                        # fall through to retry
                    except TembaBadRequestError as e:
                        # recast underlying exception so that the underlying details can be logged
                        raise Exception(f"Exception sending sms: {e.errors}") from e

                    last_failure_tokens.append(utcnow())

                    # expire any tokens that are more than 5 minutes old
                    expired_tokens = []
                    now = utcnow()
                    for token in last_failure_tokens:
                        if (now - token).total_seconds() > (5 * 60):
                            expired_tokens.append(token)

                    for token in expired_tokens:
                        log.warning(f"Removing failure token: {token.isoformat()}")
                        last_failure_tokens.remove(token)

                    # Do not retry large batch send-multis
                    # or there are more than 10 exceptions in 5 min ... prefer to crash and cause a page
                    if len(urns) <= 15 and retry_count < len(retry_wait_times) and len(last_failure_tokens) < 10:
                        wait_time_sec = retry_wait_times[retry_count]
                        log.warning(f"Send failed: {retry_exception}")
                        log.warning(f"  will retry send after {wait_time_sec} seconds")
                        time.sleep(wait_time_sec)
                        retry_count += 1
                        continue

                    log.warning(f"Failing after {retry_count} retries, failure_tokens: {last_failure_tokens}")
                    raise retry_exception

        log.debug(f"Acking message")
        message.ack()
        log.info(f"Done send_messages")
        return

    raise Exception(f"Unknown action: {action}")
