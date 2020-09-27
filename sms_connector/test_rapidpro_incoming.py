import sys
import threading
import unittest

from temba_client.exceptions import TembaConnectionError

import rapidpro_adapter_cli
import rapidpro_incoming

from lib import firestore_uuid_table
from lib import test_util
from lib.mock_firebase import MockFirestoreClient
from lib.mock_rapidpro import MockRapidProClient, MockRapidProMessage
from lib.pubsub_util import Subscriber

newly_created_uuid = "nook-phone-uuid-NEWLY-CREATED"


class RapidProIncomingTestCase(unittest.TestCase):
    def test_transfer_messages_live(self):
        if not self.setup_transfer_messages_live(): return

        mock_messages = [
            MockRapidProMessage("2019-10-02T06:47:14.267126+00:00", "tel:+0123456789-10", "in", "Some client message"),
            MockRapidProMessage("2019-10-02T06:48:14.267126+00:00", "tel:+0123456789-11", "in",
                        "Message from another client\nSecond line"),
            MockRapidProMessage("2019-10-02T06:49:14.267126+00:00", "tel:+0123456789-10-new", "in", "Hey! I'm new"),
        ]
        self.rapidpro_client.incoming.extend(mock_messages)

        process_count = rapidpro_incoming.transfer_messages()
        payloads = self.incoming_collector.wait_for_messages(len(mock_messages))

        self.assertEqual(process_count, len(mock_messages))
        self.assertEqual(len(payloads), len(mock_messages))
        self.assert_payload(payloads[0],
                            "nook-phone-uuid-91755b17-3f7e-429c-934c-9ed402d715f7",
                            "2019-10-02T06:47:14.267126+00:00", "in", "Some client message")
        self.assert_payload(payloads[1],
                            "nook-phone-uuid-c002522a-4005-454f-b3db-3e161a778576",
                            "2019-10-02T06:48:14.267126+00:00", "in", "Message from another client\nSecond line")
        self.assert_payload(payloads[2],
                            newly_created_uuid,
                            "2019-10-02T06:49:14.267126+00:00", "in", "Hey! I'm new")

        changes = self.firebase_client.changes()
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0][0], "tables/uuid-table/mappings/tel:+0123456789-10-new")
        self.assertEqual(changes[0][1]["uuid"], payloads[2]["sms_raw"]["deidentified_phone_number"])

    def test_transfer_messages_retry_live(self):
        if not self.setup_transfer_messages_live(): return

        mock_messages = [
            MockRapidProMessage("2019-10-02T06:51:14.267126+00:00", "tel:+0123456789-10", "in", "Some client message 2"),
        ]
        self.rapidpro_client.incoming.extend(mock_messages)
        self.rapidpro_client.retry_count = 2

        process_count = rapidpro_incoming.transfer_messages()
        payloads = self.incoming_collector.wait_for_messages(len(mock_messages))

        self.assertEqual(process_count, len(mock_messages))
        self.assertEqual(len(payloads), len(mock_messages))
        self.assert_payload(payloads[0],
                            "nook-phone-uuid-91755b17-3f7e-429c-934c-9ed402d715f7",
                            "2019-10-02T06:51:14.267126+00:00", "in", "Some client message 2")

        changes = self.firebase_client.changes()
        self.assertEqual(len(changes), 0)

    def test_transfer_messages_retry_fail_live(self):
        if not self.setup_transfer_messages_live(): return

        mock_messages = [
            MockRapidProMessage("2019-10-02T06:51:14.267126+00:00", "tel:+0123456789-10", "in", "Some client message 2"),
        ]
        self.rapidpro_client.incoming.extend(mock_messages)
        self.rapidpro_client.retry_count = 2

        original_wait_times = rapidpro_incoming.retry_wait_times
        rapidpro_incoming.retry_wait_times = [0.1]
        try:
            rapidpro_incoming.transfer_messages()
            self.fail("Expected exception")
        except TembaConnectionError as e:
            print(f"Expected exception occurred: {e}")
        finally:
            rapidpro_incoming.retry_wait_times = original_wait_times

        changes = self.firebase_client.changes()
        self.assertEqual(len(changes), 0)

    ############ Test Helper Methods ############################################################

    def setUp(self):
        self.incoming_subscriber = None

    def setup_transfer_messages_live(self):
        if not test_util.setup_live_test(): return False

        topic_name = f"rapidpro-incoming-{test_util.name_of_test_method()}"

        self.rapidpro_client = MockRapidProClient()
        self.firebase_client = MockFirestoreClient('testdata/uuid_mappings.json')

        self.incoming_collector = test_util.MessageCollector()
        self.incoming_subscriber = Subscriber(
            test_util.crypto_token_path,
            topic_name,
            topic_name,
            self.incoming_collector.process_message,
        )

        self.log = test_util.TestLogger(__name__)
        firestore_uuid_table.log = self.log
        rapidpro_incoming.log = self.log
        lock = threading.Lock()
        lookup_table = rapidpro_adapter_cli.new_uuid_table(test_util.crypto_token_path, self.firebase_client)
        rapidpro_incoming.init(test_util.crypto_token_path, self.rapidpro_client, lock, lookup_table,
                               topic_name=topic_name, is_mock_rp=True)
        return True

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

    def tearDown(self):
        if self.incoming_subscriber is not None:
            self.incoming_subscriber.cancel()


if __name__ == '__main__':
    test_util.setup_all_unittests(sys.argv)
