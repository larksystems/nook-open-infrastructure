import datetime
import sys
import threading
import time
import unittest

import rapidpro_adapter_cli
import rapidpro_incoming
import rapidpro_outgoing

from lib import firestore_uuid_table
from lib import test_util
from lib.mock_firebase import MockFirestoreClient
from lib.mock_rapidpro import MockRapidProClient, MockRapidProMessage
from lib.pubsub_util import Publisher, Subscriber

newly_created_uuid = "nook-phone-uuid-NEWLY-CREATED"

mock_incoming_messages = [
    MockRapidProMessage("2019-10-02T06:47:14.267126+00:00", "tel:+0123456789-10", "in",
                        "Some client message"),
    MockRapidProMessage("2019-10-02T06:48:14.267126+00:00", "tel:+0123456789-11", "in",
                        "Message from another client\nSecond line"),
]
mock_outgoing_payload = {
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


class RapidProAdapterCliTestCase(unittest.TestCase):
    def test_read_write_last_update_time(self):
        test_util.print_test_header()
        sync_token_path = test_util.path_for_temp_test_file("test_read_write_last_update_time.json")

        actual_value = rapidpro_adapter_cli.read_last_update_time(sync_token_path)
        self.assertIsNone(actual_value)

        with open(sync_token_path, 'w'):
            pass # touch the file

        actual_value = rapidpro_adapter_cli.read_last_update_time(sync_token_path)
        self.assertIsNone(actual_value)

        expected_value = datetime.datetime.utcnow()
        rapidpro_adapter_cli.write_last_update_time(sync_token_path, expected_value)
        actual_value = rapidpro_adapter_cli.read_last_update_time(sync_token_path)
        self.assertEqual(actual_value, expected_value)

    def test_adapter_live(self):
        if not self.setup_adapter_live(): return
        start_time = datetime.datetime.now(datetime.timezone.utc)
        sync_token_path = test_util.path_for_temp_test_file(f"{test_util.name_of_test_method()}.json")

        # test simultaneous incoming and outgoing

        self.rapidpro_client.incoming.extend(mock_incoming_messages)
        self.publisher.publish(mock_outgoing_payload)
        self.log.info("finished publishing")
        print("")

        wait_time_in_seconds = 5
        self.log.info(f"Waiting up to {wait_time_in_seconds} seconds to process messages...")
        end_time = time.time() + wait_time_in_seconds

        def idle_stop():
            if len(self.incoming_collector.payloads) < len(mock_incoming_messages) or len(self.rapidpro_client.outgoing) < 4:
                if time.time() < end_time:
                    time.sleep(0.5)
                    return
                rapidpro_adapter_cli.log.warning("timeout waiting for messages")
            rapidpro_adapter_cli.process_messages = False

        rapidpro_adapter_cli.run_inbound_polling(sync_token_path, idle_stop)

        # wait a bit more for the message ack to be sent
        time.sleep(1)

        incoming_payloads = self.incoming_collector.payloads
        self.assertEqual(len(incoming_payloads), len(mock_incoming_messages))
        self.assert_payload(incoming_payloads[0],
                            "nook-phone-uuid-91755b17-3f7e-429c-934c-9ed402d715f7",
                            "2019-10-02T06:47:14.267126+00:00", "in", "Some client message")
        self.assert_payload(incoming_payloads[1],
                            "nook-phone-uuid-c002522a-4005-454f-b3db-3e161a778576",
                            "2019-10-02T06:48:14.267126+00:00", "in", "Message from another client\nSecond line")

        outgoing_messages = self.rapidpro_client.outgoing
        self.assertEqual(outgoing_messages, [
            (["tel:+0123456789-10", "tel:+0123456789-11"], "1/2 this is message one"),
            (["tel:+0123456789-10", "tel:+0123456789-11"], "2/2 and here's the rest of the message"),
        ])

        last_time = rapidpro_adapter_cli.read_last_update_time(sync_token_path)
        self.assertIsNotNone(last_time)
        delta = last_time - start_time
        self.assertTrue(delta.total_seconds() > 0)

    def test_outgoing_process_exception_live(self):
        if not self.setup_adapter_live(): return
        start_time = datetime.datetime.now(datetime.timezone.utc)
        sync_token_path = test_util.path_for_temp_test_file(f"{test_util.name_of_test_method()}.json")

        self.publisher.publish({"action": "unknown-action"})

        actual_exception = None
        try:
            rapidpro_adapter_cli.run_inbound_polling(sync_token_path)
        except Exception as e:
            # Exception expected
            actual_exception = e
        self.log.info("finished running")
        print("")

        # wait a bit more for the message ack to be sent
        time.sleep(1)
        self.log.info("finished waiting")
        print("")

        # discard any remaining messages
        self.log.info("discarding unprocessed messages...")
        discarded_payloads = self.discard_messages(rapidpro_outgoing.subscriber)
        self.log.info("done discarding unprocessed messages")
        rapidpro_adapter_cli.teardown()

        self.assertIsNotNone(actual_exception)
        self.assertEqual(actual_exception, rapidpro_outgoing.sequencer.last_exception)
        rapidpro_outgoing.sequencer.last_exception = None

        self.assertEqual(len(self.incoming_collector.payloads), 0)
        self.assertEqual(len(self.rapidpro_client.outgoing), 0)

        last_time = rapidpro_adapter_cli.read_last_update_time(sync_token_path)
        self.assertIsNotNone(last_time)
        delta = last_time - start_time
        self.assertTrue(delta.total_seconds() > 0)

    ############ Test Helper Methods ############################################################

    def setup_adapter_live(self):
        if not test_util.setup_live_test(): return False

        incoming_topic_name = f"rapidpro-incoming-{test_util.name_of_test_method()}"
        outgoing_topic_name = f"rapidpro-outgoing-{test_util.name_of_test_method()}"

        self.log = test_util.TestLogger(test_util.name_of_test_method())
        self.rapidpro_client = MockRapidProClient()
        self.firebase_client = MockFirestoreClient('testdata/uuid_mappings.json')

        firestore_uuid_table.log = self.log
        rapidpro_incoming.log = self.log
        rapidpro_outgoing.log = self.log
        rapidpro_adapter_cli.log = self.log
        lookup_table = rapidpro_adapter_cli.new_uuid_table(test_util.crypto_token_path, self.firebase_client)
        lock = threading.Lock()
        rapidpro_incoming.init(test_util.crypto_token_path, self.rapidpro_client, lock, lookup_table,
                               topic_name=incoming_topic_name, is_mock_rp=True)
        rapidpro_outgoing.init(test_util.crypto_token_path, self.rapidpro_client, lock, lookup_table,
                               topic_name=outgoing_topic_name)

        # Add pre and post counters in the normal message flow
        # publisher[message] --> pre_collector --> sequencer --> post_collector --> process_message_impl
        self.post_collector = test_util.MessageCollector(rapidpro_outgoing.process_message_impl)
        rapidpro_outgoing.sequencer.process_message_funct = self.post_collector.process_message
        self.pre_collector = test_util.MessageCollector(rapidpro_outgoing.sequencer.process_message)
        rapidpro_outgoing.subscriber.process_message_funct = self.pre_collector.process_message
        self.publisher = Publisher(test_util.crypto_token_path, outgoing_topic_name)

        self.incoming_collector = test_util.MessageCollector()
        self.incoming_subscriber = Subscriber(
            test_util.crypto_token_path,
            incoming_topic_name,
            incoming_topic_name,
            self.incoming_collector.process_message,
        )

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

    def setUp(self):
        self.incoming_subscriber = None

    def tearDown(self):
        if rapidpro_outgoing.subscriber is not None:
            rapidpro_outgoing.subscriber.cancel()
            rapidpro_outgoing.subscriber = None
        if self.incoming_subscriber is not None:
            self.incoming_subscriber.cancel()
        if rapidpro_outgoing.sequencer is not None:
            rapidpro_outgoing.check_exception()

    def assert_payload(self, payload, uuid, created_on, direction, text):
        self.assertEqual(payload["action"], "sms_from_rapidpro")
        sms_raw = payload["sms_raw"]

        if uuid == newly_created_uuid:
            new_uuid = sms_raw["deidentified_phone_number"]
            self.assertTrue(new_uuid.startswith("nook-phone-uuid-"), msg=new_uuid)
            self.assertTrue(len(new_uuid) > 25, msg=new_uuid)
        else:
            self.assertEqual(sms_raw["deidentified_phone_number"], uuid)

        self.assertEqual(sms_raw["created_on"], created_on)
        self.assertEqual(sms_raw["direction"], direction)
        self.assertEqual(sms_raw["text"], text)


if __name__ == '__main__':
    test_util.setup_all_unittests(sys.argv)
