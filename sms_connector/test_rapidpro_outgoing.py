import datetime
import json
import sys
import threading
import time
import unittest

import rapidpro_adapter_cli
import rapidpro_outgoing

from lib import firestore_uuid_table
from lib import test_util
from lib.mock_firebase import MockFirestoreClient
from lib.mock_rapidpro import MockRapidProClient
from lib.pubsub_util import Publisher, MessageSequencer

mock_payload = {
    "action": "send_messages",
    "ids": [
        "nook-phone-uuid-91755b17-3f7e-429c-934c-9ed402d715f7",
        "nook-phone-uuid-c002522a-4005-454f-b3db-3e161a778576",
    ],
    "messages": [
        "1/2 this is message one",
        "2/2 and here's the rest of the message"
    ]
}


class RapidProOutgoingTestCase(unittest.TestCase):
    def test_process_messages_impl(self):
        self.setup_rapidpro_adapter()

        self.process_message_impl(mock_payload)

        outgoing = rapidpro_outgoing.rapidpro_client.outgoing
        self.assertEqual(outgoing, [
            (["tel:+0123456789-10", "tel:+0123456789-11"], "1/2 this is message one"),
            (["tel:+0123456789-10", "tel:+0123456789-11"], "2/2 and here's the rest of the message"),
        ])

    def test_process_messages(self):
        self.setup_rapidpro_adapter()

        self.process_message(mock_payload)

        outgoing = rapidpro_outgoing.rapidpro_client.outgoing
        self.assertEqual(outgoing, [
            (["tel:+0123456789-10", "tel:+0123456789-11"], "1/2 this is message one"),
            (["tel:+0123456789-10", "tel:+0123456789-11"], "2/2 and here's the rest of the message"),
        ])

    def test_process_messages_impl_retry(self):
        self.setup_rapidpro_adapter()

        self.process_message_impl(mock_payload, retry_count=2)

        outgoing = rapidpro_outgoing.rapidpro_client.outgoing
        self.assertEqual(outgoing, [
            (["tel:+0123456789-10", "tel:+0123456789-11"], "1/2 this is message one"),
            (["tel:+0123456789-10", "tel:+0123456789-11"], "2/2 and here's the rest of the message"),
        ])

    def test_process_messages_retry(self):
        self.setup_rapidpro_adapter()

        self.process_message(mock_payload, retry_count=2)

        outgoing = rapidpro_outgoing.rapidpro_client.outgoing
        self.assertEqual(outgoing, [
            (["tel:+0123456789-10", "tel:+0123456789-11"], "1/2 this is message one"),
            (["tel:+0123456789-10", "tel:+0123456789-11"], "2/2 and here's the rest of the message"),
        ])

    def test_process_messages_impl_fail(self):
        self.setup_rapidpro_adapter()

        try:
            self.process_message_impl(mock_payload, retry_count=2, retry_wait_times=[0.1])
            self.fail("Expected exception")
        except Exception:
            pass # expected exception

        outgoing = rapidpro_outgoing.rapidpro_client.outgoing
        self.assertEqual(outgoing, [])

    def test_process_messages_fail(self):
        self.setup_rapidpro_adapter()

        try:
            self.process_message(mock_payload, retry_count=2, retry_wait_times=[0.1])
            self.fail("Expected exception")
        except Exception:
            pass # expected exception

        last_exception = rapidpro_outgoing.sequencer.last_exception
        rapidpro_outgoing.sequencer.last_exception = None
        self.assertIsNotNone(last_exception)

        outgoing = rapidpro_outgoing.rapidpro_client.outgoing
        self.assertEqual(outgoing, [])

    def test_process_messages_impl_many(self):
        self.setup_rapidpro_adapter()

        ids = []
        for count in range(0, 250):
            id = f"nook-phone-uuid-837601-473126-{count}"
            tel = f"tel:+0123456789037-{count}"
            doc = self.firebase_client.document(f"tables/uuid-table/mappings/{tel}")
            doc.set({ "uuid": id, "__id": tel })
            ids.append(id)

        self.process_message_impl({
            "action": "send_messages",
            "ids": ids,
            "messages": [
                "1/2 this is message one",
                "2/2 and here's the rest of the message"
            ]
        })

        outgoing = rapidpro_outgoing.rapidpro_client.outgoing

        # 250 ids should be broken down into groups of 100 with 2 message sent to each group
        self.assertEqual(len(outgoing), 6)
        self.assertEqual(len(outgoing[0][0]), 100)
        self.assertEqual(outgoing[0][1], "1/2 this is message one")
        self.assertEqual(len(outgoing[1][0]), 100)
        self.assertEqual(outgoing[1][1], "2/2 and here's the rest of the message")

        self.assertEqual(len(outgoing[2][0]), 100)
        self.assertEqual(outgoing[2][1], "1/2 this is message one")
        self.assertEqual(len(outgoing[3][0]), 100)
        self.assertEqual(outgoing[3][1], "2/2 and here's the rest of the message")

        self.assertEqual(len(outgoing[4][0]), 50)
        self.assertEqual(outgoing[4][1], "1/2 this is message one")
        self.assertEqual(len(outgoing[5][0]), 50)
        self.assertEqual(outgoing[5][1], "2/2 and here's the rest of the message")

    def test_process_message_order_live(self):
        if not self.setup_rapidpro_adapter_live(): return

        # simulate multiple payloads sent by a nook client via pub/sub
        num_messages = 30
        self.log.info(f"Sending {num_messages} messages ...")
        for count in range(0, num_messages):
            payload = {
                "action": "send_messages",
                "ids": [
                    "nook-phone-uuid-c002522a-4005-454f-b3db-3e161a778576",
                ],
                "messages": [
                    f"test outgoing sms {count}"
                ],
            }
            self.publisher.publish(payload)
        self.log.info("done sending message")
        print("")

        # wait for messages
        self.log.info("waiting for messages...")
        payloads = self.pre_collector.wait_for_messages(num_messages)
        self.log.info("done waiting for messages")
        print("")

        # check order of messages
        print(f"messages in order that they were processed:")
        out_of_order = False
        outgoing = rapidpro_outgoing.rapidpro_client.outgoing

        for index in range(0, len(outgoing)):
            msg_text = outgoing[index][1]
            value = int(msg_text.split()[-1])
            if value == index:
                print(msg_text)
            else:
                out_of_order = True
                print(f"{msg_text}   <<< out of order")
        print("")

        self.assertFalse(out_of_order, msg="The messages were processed out of order")
        self.assertEqual(len(payloads), num_messages)

        # discard any remaining messages
        self.log.info("discarding unprocessed messages...")
        discarded_payloads = self.discard_messages(rapidpro_outgoing.subscriber)
        self.log.info("done discarding unprocessed messages")
        self.assertEqual(len(discarded_payloads), 0)

    def test_exception_handling_live(self):
        if not self.setup_rapidpro_adapter_live(): return

        num_messages = 5
        test_time = f"{datetime.datetime.now().time()}"
        self.log.info(f"sending {num_messages * 2 + 1} messages...")
        for count in range(0, num_messages):
            self.publisher.publish({
                "test_time": test_time,
                "action": "send_messages",
                "ids": [
                    "nook-phone-uuid-c002522a-4005-454f-b3db-3e161a778576",
                ],
                "messages": [
                    f"message {count} before unknown-action",
                ],
            })
        self.publisher.publish({"action": "unknown-action"})
        for count in range(0, num_messages):
            self.publisher.publish({
                "test_time": test_time,
                "action": "send_messages",
                "ids": [
                    "nook-phone-uuid-c002522a-4005-454f-b3db-3e161a778576",
                ],
                "messages": [
                    f"message {count} after unknown-action",
                ],
            })
        self.log.info("done sending messages")
        print("")

        # simulate pub/sub handler main
        self.log.info("processing messages...")
        actual_exception = None
        try:
            rapidpro_outgoing.subscriber.wait()
        except Exception as e:
            actual_exception = e
        self.log.info("done processing messages...")
        print("")

        # discard any remaining messages
        self.log.info("discarding unprocessed messages...")
        processed_payloads = self.post_collector.payloads
        discarded_payloads = self.discard_messages(rapidpro_outgoing.subscriber)
        self.log.info("done discarding unprocessed messages")
        print("")

        self.log.info(f"num     acked messages: {self.pre_collector.ack_count}")
        self.log.info(f"num    nacked messages: {self.pre_collector.nack_count}")
        self.log.info(f"num processed messages: {len(processed_payloads)}")
        self.log.info(f"num discarded messages: {len(discarded_payloads)}")

        last_exception = rapidpro_outgoing.sequencer.last_exception
        rapidpro_outgoing.sequencer.last_exception = None
        self.assertIsNotNone(actual_exception)
        self.assertEqual(last_exception, actual_exception)
        self.assertEqual(self.pre_collector.ack_count, 5)
        for payload in processed_payloads:
            if payload["action"] == "unknown-action":
                continue
            self.assertEqual(payload["action"], "send_messages")
            self.assertIn("before unknown-action", payload["messages"][0])
        for payload in discarded_payloads:
            if payload["action"] == "unknown-action":
                continue
            self.assertEqual(payload["action"], "send_messages")
            self.assertIn("after unknown-action", payload["messages"][0])

    ############ Test Helper Methods ############################################################

    def setup_rapidpro_adapter(self):
        test_util.print_test_header()
        self.setup_rapidpro_adapter_impl()

    def setup_rapidpro_adapter_live(self):
        if not test_util.setup_live_test(): return False
        self.setup_rapidpro_adapter_impl()

        topic_name = f"rapidpro-{test_util.name_of_test_method()}"

        rapidpro_lock = threading.Lock()
        lookup_table = rapidpro_adapter_cli.new_uuid_table(test_util.crypto_token_path, self.firebase_client)
        rapidpro_outgoing.init(test_util.crypto_token_path, rapidpro_outgoing.rapidpro_client, rapidpro_lock, lookup_table,
                               topic_name=topic_name)

        # Add pre and post counters in the normal message flow
        # publisher[message] --> pre_collector --> sequencer --> post_collector --> process_message_impl
        self.post_collector = test_util.MessageCollector(rapidpro_outgoing.process_message_impl)
        rapidpro_outgoing.sequencer.process_message_funct = self.post_collector.process_message
        self.pre_collector = test_util.MessageCollector(rapidpro_outgoing.sequencer.process_message)
        rapidpro_outgoing.subscriber.process_message_funct = self.pre_collector.process_message
        self.publisher = Publisher(test_util.crypto_token_path, topic_name)

        # discard old messages
        self.log.info("discarding old messages...")
        self.discard_messages(rapidpro_outgoing.subscriber)
        self.log.info("done discarding old messages")
        print("")

        return True

    def discard_messages(self, subscriber, wait_time_in_seconds=3):
        collector = test_util.MessageCollector()
        original = subscriber.process_message_funct
        subscriber.process_message_funct = collector.process_message
        payloads = collector.wait_for_messages(1000, wait_time_in_seconds=wait_time_in_seconds)
        subscriber.process_message_funct = original
        return payloads

    def setup_rapidpro_adapter_impl(self):
        self.log = test_util.TestLogger(test_util.name_of_test_method())
        self.firebase_client = MockFirestoreClient('testdata/uuid_mappings.json')

        # Simulate normal message flow
        # publisher[message] --> sequencer --> process_message_impl
        rapidpro_outgoing.sequencer = MessageSequencer(rapidpro_outgoing.process_message_impl)

        firestore_uuid_table.log = self.log
        rapidpro_outgoing.log = self.log

        lookup_table = rapidpro_adapter_cli.new_uuid_table(test_util.crypto_token_path, self.firebase_client)
        rapidpro_outgoing.phone_number_uuid_table = lookup_table
        rapidpro_outgoing.rapidpro_client = MockRapidProClient()
        rapidpro_outgoing.rapidpro_lock = threading.Lock()

    def process_message(self, payload, retry_wait_times=None, retry_count=0):
        if retry_wait_times is None:
            rapidpro_outgoing.retry_wait_times = [0.1, 0.1, 0.1]
        else:
            rapidpro_outgoing.retry_wait_times = retry_wait_times
        rapidpro_outgoing.last_failure_tokens = [] # Reset the failure tokens
        rapidpro_outgoing.rapidpro_client.retry_count = retry_count
        message = test_util.MockPubSubMessage(json.dumps({"payload": payload}))
        self.assertEqual(message.acked, False)
        try:
            rapidpro_outgoing.sequencer.process_message(message)
            self.assertEqual(message.acked, True)
        except Exception as e:
            self.assertEqual(message.acked, False)
            raise

    def process_message_impl(self, payload, retry_wait_times=None, retry_count=0):
        if retry_wait_times is None:
            rapidpro_outgoing.retry_wait_times = [0.1, 0.1, 0.1]
        else:
            rapidpro_outgoing.retry_wait_times = retry_wait_times
        rapidpro_outgoing.last_failure_tokens = [] # Reset the failure tokens
        rapidpro_outgoing.rapidpro_client.retry_count = retry_count
        message = test_util.MockPubSubMessage(json.dumps({"payload": payload}))
        self.assertEqual(message.acked, False)
        try:
            rapidpro_outgoing.process_message_impl(message)
            self.assertEqual(message.acked, True)
        except Exception as e:
            self.assertEqual(message.acked, False)
            raise

    def tearDown(self):
        if rapidpro_outgoing.subscriber is not None:
            rapidpro_outgoing.subscriber.cancel()
            rapidpro_outgoing.subscriber = None
        rapidpro_outgoing.check_exception()


if __name__ == '__main__':
    test_util.setup_all_unittests(sys.argv)
