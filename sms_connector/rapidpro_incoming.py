import json
import requests
import time

from requests.exceptions import ReadTimeout
from temba_client.utils import request
from temba_client.exceptions import TembaConnectionError, TembaHttpError

from lib.pubsub_util import Publisher
from lib.simple_logger import Logger


log = None

rapidpro_client = None
rapidpro_lock = None
firebase_client = None
phone_number_uuid_table = None
publisher = None
counter = None
retry_wait_times = [0.1, 0.5, 2, 4, 8, 16, 32]


def init(crypto_token_path, rp_client, rp_lock, lookup_table, topic_name ="sms-channel-topic", is_mock_rp=False):
    global log, rapidpro_client, rapidpro_lock, phone_number_uuid_table, publisher, counter

    if not is_mock_rp:
        # HACK: Rewrite the request method used by the temba_client to provide a timeout
        # First check that the original method's code hasn't changed by verifying its bytecode
        assert request.__code__.co_code == original_request_function.__code__.co_code
        # Then replace the code with the timeout method
        request.__code__ = request_with_default_timeout.__code__

    if log is None:
        if crypto_token_path is None:
            raise AssertionError("Must pass in crypto token or initialize log first")
        log = Logger(__name__)

    log.info("Init incoming")
    rapidpro_client = rp_client
    rapidpro_lock = rp_lock
    phone_number_uuid_table = lookup_table
    publisher = Publisher(crypto_token_path, topic_name)
    log.info("Done")


def transfer_messages(created_after_inclusive=None):
    log.info(f"Get messages")

    new_messages = None
    retry_count = 0
    while True:
        try:
            with rapidpro_lock:
                new_messages = rapidpro_client.get_raw_messages(created_after_inclusive=created_after_inclusive)
            break
        except (TembaConnectionError, TembaHttpError, ReadTimeout) as e:
            retry_exception = e
            # fall through to retry

        if retry_count < len(retry_wait_times):
            wait_time_sec = retry_wait_times[retry_count]
            log.warning(f"Get messages failed: {retry_exception}")
            log.warning(f"  will retry after {wait_time_sec} seconds")
            time.sleep(wait_time_sec)
            retry_count += 1
            continue
        raise retry_exception

    process_count = 0
    for message in new_messages:
        created_on = message.created_on
        urn = message.urn
        direction = message.direction
        text = message.text
        process_message(created_on, urn, direction, text)
        process_count += 1
    log.info(f"Processed {process_count} messages")
    return process_count


def process_message(created_on, urn, direction, text):
    log.info (f'Processing: {created_on}: {urn}, {direction},\t {text}')

    id = phone_number_uuid_table.data_to_uuid(urn)
    # print (f'URN mapping: {urn} => {id}')

    publisher.publish({
        "action": "sms_from_rapidpro",
        "sms_raw": {
            "deidentified_phone_number": id,
            "created_on": created_on.isoformat(),
            "text": text,
            "direction": direction,
        }
    })


def original_request_function(method, url, **kwargs):  # pragma: no cover
    """
    For the purposes of testing, all calls to requests.request go through here before JSON bodies are encoded. It's
    easier to mock this and verify request data before it's encoded.
    """
    if "data" in kwargs:
        kwargs["data"] = json.dumps(kwargs["data"])

    return requests.request(method, url, **kwargs)


def request_with_default_timeout(method, url, **kwargs):
    if "data" in kwargs:
        kwargs["data"] = json.dumps(kwargs["data"])
    if "timeout" not in kwargs:
        kwargs["timeout"] = 600  # 10 mins

    return requests.request(method, url, **kwargs)
