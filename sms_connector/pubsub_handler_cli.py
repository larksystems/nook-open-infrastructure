import datetime
import json
import os
import sys

from lib import message_util
from lib.simple_logger import Logger
from lib import pubsub_util
from lib.pubsub_util import Subscriber, MessageSequencer, Publisher
from lib.utils import utcnow
import lib.opinion_handlers


log = None
subscriber = None
subscription = None

# The publisher used to send outgoing sms requests to the rapidpro adapter
rapidpro_publisher = None

KNOWN_NAMESPACES_FOR_OPINIONS = set([
    "nook_conversations/add_tags",
    "nook_conversations/remove_tags",
    "nook_conversations/set_notes",
    "nook_conversations/set_unread",
    "nook_messages/add_tags",
    "nook_messages/remove_tags",
    "nook_messages/set_translation",
])


def init_logger(crypto_token_path):
    global log
    log = Logger(__name__)
    pubsub_util.log = Logger(pubsub_util.__name__)

def process_message_impl(message):
    """Called on a background thread once for each message.
    It is the responsibility of the caller to gracefully handle exceptions"""
    log.debug(f"Processing: {message}")

    data_map = json.loads(message.data)['payload']
    log.notify(f"pubsub: processing {json.dumps(data_map)}")

    assert "action" in data_map.keys()
    action = data_map["action"]

    if action == "send_to_multi_ids":

        assert "ids" in data_map.keys()
        assert "message" in data_map.keys()

        # {
        # "action" : "send_to_multi_ids"
        # "ids" : [ "nook-uuid-23dsa" ],
        # "message" : "🐱"
        # "_authenticatedUserEmail": "who@where.com",
        # "_authenticatedUserDisplayName": "someone"
        # }

        sms_datetime = datetime.datetime.utcnow()
        text = data_map["message"]
        log.audit(f"pubsub: send_sms {json.dumps(data_map)}")

        rapidpro_publisher.publish({
            "action": "send_messages",
            "ids": data_map["ids"],
            "messages": [ text ],
        })

        log.debug(f"Acking message {message}")
        message.ack()
        log.info(f"Done send_to_multi_ids")
        return

    if action == "send_messages_to_ids":

        assert "ids" in data_map.keys()
        assert "messages" in data_map.keys()

        # {
        # "action" : "send_messages_to_ids"
        # "ids" : [ "nook-uuid-23dsa" ],
        # "message" : [ "🐱" ]
        # "_authenticatedUserEmail": "who@where.com",
        # "_authenticatedUserDisplayName": "someone"
        # }

        sms_datetime = datetime.datetime.utcnow()
        messages = data_map["messages"]
        ids = data_map["ids"]

        log.audit(f"pubsub: send_sms {json.dumps(data_map)}")

        rapidpro_publisher.publish({
            "action": "send_messages",
            "ids": ids,
            "messages": messages,
        })

        log.debug(f"Acking message {message}")
        message.ack()
        log.info(f"Done send_messages_to_ids")
        return

    if action == "add_opinion":
        log.audit(f"pubsub: add_opinion: {json.dumps(data_map)}")


        assert "namespace" in data_map.keys()
        namespace = data_map['namespace']

        assert "opinion" in data_map.keys()
        opinion = data_map['opinion']

        assert "source" in data_map.keys()
        source = data_map['source']

        assert "_authenticatedUserEmail" in data_map.keys()
        assert "_authenticatedUserDisplayName" in data_map.keys()

        assert "_authenticatedUserEmail" not in data_map['opinion']
        assert "_authenticatedUserDisplayName" not in data_map['opinion']
        opinion['_authenticatedUserEmail'] = data_map['_authenticatedUserEmail']
        opinion['_authenticatedUserDisplayName'] = data_map['_authenticatedUserDisplayName']

        if namespace not in KNOWN_NAMESPACES_FOR_OPINIONS:
            raise Exception(f"Opinion write for unknown namespace: {namespace}")

        lib.opinion_handlers.add_opinion(namespace, opinion)

        log.debug(f"Acking message {message}")
        message.ack()
        log.info(f"Done add_opinion")
        return

    if action == "sms_from_rapidpro":
        assert "sms_raw" in data_map.keys()

        lib.opinion_handlers.add_opinion("sms_raw_msg", data_map["sms_raw"])

        message.ack()
        log.info(f"Done sms_from_rapidpro")
        return


    raise Exception(f"Unknown action: {action}")


def run():
    r = subscriber.wait()  # blocks until ctrl-c or exception
    log.info(r)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"usage: python {sys.argv[0]} crypto_token")
        exit(1)

    crypto_token_file = sys.argv[1]


    init_logger(crypto_token_file)
    sequencer = MessageSequencer(process_message_impl)
    subscriber = Subscriber(crypto_token_file, "sms-channel-topic", "sms-channel-subscription", sequencer.process_message)
    rapidpro_publisher = Publisher(crypto_token_file, "sms-outgoing")
    log.info("Setup complete")

    try:
        run()
    except KeyboardInterrupt:
        print("")
        log.info("Keyboard interrupt")
    finally:
        subscriber.cancel()
        close_journal()
        log.info("Cleanup complete")
