import sys
import unittest

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from lib import firestore_uuid_table
from lib import mock_firebase
from lib import test_util


live_firebase_client = None

class FirestoreUuidTableTestCase(unittest.TestCase):
    def test_new_uuid_for_data_existing(self):
        self.setup_firebase_uuid_table()
        uuid = self.uuid_table.new_or_existing_uuid_for_data('tel:+0123456789-2')
        self.assertEqual(uuid, 'nook-phone-uuid-125a04d0-24d3-4dc2-b40a-56d576583991')
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 1)

    def test_new_uuid_for_data_unknown(self):
        self.setup_firebase_uuid_table()
        telegram_data = 'tel:+0123456789-2-unknown'

        uuid_1 = self.uuid_table.new_or_existing_uuid_for_data(telegram_data)
        self.assertTrue(uuid_1.startswith("nook-phone-uuid-"), msg=uuid_1)
        self.assertTrue(len(uuid_1) > len("nook-phone-uuid-") + 5, msg=uuid_1)
        changes = self.firebase_client.changes()
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0][0], f"tables/uuid-table/mappings/{telegram_data}")
        self.assertEqual(changes[0][1], { "uuid": uuid_1 })
        changes.clear()
        transactions = self.firebase_client.completed_transactions()
        self.assertEqual(len(transactions), 1)
        transactions.clear()

        uuid_2 = self.uuid_table.new_or_existing_uuid_for_data(telegram_data)
        self.assertEqual(uuid_1, uuid_2)
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 1)

    def test_new_uuid_for_data_live(self):
        if not self.setup_firebase_uuid_table_live(): return
        telegram_data = 'tel:+0123456789-2'

        self.uuid_table._data_to_uuid_collection().document(telegram_data).delete()

        uuid_1 = self.uuid_table.new_or_existing_uuid_for_data(telegram_data)
        self.assertTrue(uuid_1.startswith("my-uuid-"), msg=uuid_1)
        self.assertTrue(len(uuid_1) > len("my-uuid-") + 5, msg=uuid_1)

        uuid_2 = self.uuid_table.new_or_existing_uuid_for_data(telegram_data)
        self.assertEqual(uuid_1, uuid_2)

    def test_data_to_uuid_existing(self):
        self.setup_firebase_uuid_table()
        telegram_data = 'tel:+0123456789-2'
        uuid = self.uuid_table.data_to_uuid(telegram_data)
        self.assertEqual(uuid, 'nook-phone-uuid-125a04d0-24d3-4dc2-b40a-56d576583991')
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 0)

    def test_data_to_uuid_existing_cached(self):
        self.setup_firebase_uuid_table()
        self.uuid_table.cache_uuid_table()
        telegram_data = 'tel:+0123456789-2'
        uuid = self.uuid_table.data_to_uuid(telegram_data)
        self.assertEqual(uuid, 'nook-phone-uuid-125a04d0-24d3-4dc2-b40a-56d576583991')
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 0)

    def test_data_to_uuid_new(self):
        self.setup_firebase_uuid_table()
        telegram_data = 'tel:+0123456789-2-not-in-uuid-table-yet'
        uuid = self.uuid_table.data_to_uuid(telegram_data)
        self.assertTrue(uuid.startswith('nook-phone-uuid-'), msg=uuid)
        self.assertTrue(len(uuid) > 25, msg=uuid)
        changes = self.firebase_client.changes()
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0][0], f"tables/uuid-table/mappings/{telegram_data}")
        self.assertEqual(changes[0][1]['uuid'], uuid)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 1)

    def test_data_to_uuid_batch_existing(self):
        self.setup_firebase_uuid_table()
        telegram_data_1 = 'tel:+0123456789-2'
        telegram_data_2 = 'tel:+0123456789-12'
        telegram_list = [telegram_data_1, telegram_data_2]
        uuid_dict = self.uuid_table.data_to_uuid_batch(telegram_list)
        self.assertEqual(len(uuid_dict), 2)
        self.assertEqual(uuid_dict[telegram_data_1], 'nook-phone-uuid-125a04d0-24d3-4dc2-b40a-56d576583991')
        self.assertEqual(uuid_dict[telegram_data_2], 'nook-phone-uuid-f58f1a88-68d7-402f-b0b3-2d2dbc67c91f')
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 0)

    def test_data_to_uuid_batch_new(self):
        self.setup_firebase_uuid_table()
        telegram_data_1 = 'tel:+0123456789-2-not-in-uuid-table-yet'
        telegram_data_2 = 'tel:+0123456789-12'
        telegram_list = [telegram_data_1, telegram_data_2]
        uuid_dict = self.uuid_table.data_to_uuid_batch(telegram_list)
        self.assertEqual(len(uuid_dict), 2)
        self.assertEqual(uuid_dict[telegram_data_2], 'nook-phone-uuid-f58f1a88-68d7-402f-b0b3-2d2dbc67c91f')
        uuid_new = uuid_dict[telegram_data_1]
        self.assertTrue(uuid_new.startswith('nook-phone-uuid-'), msg=uuid_new)
        self.assertTrue(len(uuid_new) > 25, msg=uuid_new)
        changes = self.firebase_client.changes()
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0][0], f"tables/uuid-table/mappings/{telegram_data_1}")
        self.assertEqual(changes[0][1]['uuid'], uuid_new)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 1)

    def test_data_to_uuid_batch_new_cached(self):
        self.setup_firebase_uuid_table()
        # Explicitly cache the uuid table before calling data_to_uuid_batch
        self.uuid_table.cache_uuid_table()
        telegram_data_1 = 'tel:+0123456789-2-not-in-uuid-table-yet'
        telegram_data_2 = 'tel:+0123456789-12'
        telegram_list = [telegram_data_1, telegram_data_2]
        uuid_dict = self.uuid_table.data_to_uuid_batch(telegram_list)
        self.assertEqual(len(uuid_dict), 2)
        self.assertEqual(uuid_dict[telegram_data_2], 'nook-phone-uuid-f58f1a88-68d7-402f-b0b3-2d2dbc67c91f')
        uuid_new = uuid_dict[telegram_data_1]
        self.assertTrue(uuid_new.startswith('nook-phone-uuid-'), msg=uuid_new)
        self.assertTrue(len(uuid_new) > 25, msg=uuid_new)
        changes = self.firebase_client.changes()
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0][0], f"tables/uuid-table/mappings/{telegram_data_1}")
        self.assertEqual(changes[0][1]['uuid'], uuid_new)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 1)

    def test_uuid_to_data_existing(self):
        self.setup_firebase_uuid_table()
        uuid = 'nook-phone-uuid-125a04d0-24d3-4dc2-b40a-56d576583991'
        telegram_data = self.uuid_table.uuid_to_data(uuid)
        self.assertEqual(telegram_data, 'tel:+0123456789-2')
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 0)

    def test_uuid_to_data_unknown(self):
        self.setup_firebase_uuid_table()
        uuid = 'nook-phone-uuid-125a04d0-24d3-4dc2-b40a-56d576583991-does-not-exist'
        try:
            self.uuid_table.uuid_to_data(uuid)
            self.fail("expected exception")
        except LookupError:
            # expected exception
            pass
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 0)

    def test_uuid_to_data_batch_existing(self):
        self.setup_firebase_uuid_table()
        uuid_1 = 'nook-phone-uuid-125a04d0-24d3-4dc2-b40a-56d576583991'
        uuid_2 = 'nook-phone-uuid-f58f1a88-68d7-402f-b0b3-2d2dbc67c91f'
        telegram_dict = self.uuid_table.uuid_to_data_batch([uuid_1, uuid_2])
        self.assertEqual(len(telegram_dict), 2)
        self.assertEqual(telegram_dict[uuid_1], 'tel:+0123456789-2')
        self.assertEqual(telegram_dict[uuid_2], 'tel:+0123456789-12')
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 0)

    def test_uuid_to_data_batch_unknown(self):
        self.setup_firebase_uuid_table()
        uuid_1 = 'nook-phone-uuid-125a04d0-24d3-4dc2-b40a-56d576583991-unknown'
        uuid_2 = 'nook-phone-uuid-f58f1a88-68d7-402f-b0b3-2d2dbc67c91f'
        try:
            self.uuid_table.uuid_to_data_batch([uuid_1, uuid_2])
            self.fail("expected exception")
        except LookupError:
            # expected exception
            pass
        self.assertEqual(len(self.firebase_client.changes()), 0)
        self.assertEqual(len(self.firebase_client.completed_transactions()), 0)

    def test_regression_test_read_after_write_fail(self):
        self.setup_firebase_uuid_table()
        telegram_data = 'tel:+0123456789-2-not-in-uuid-table-yet'
        uuid = self.uuid_table.data_to_uuid(telegram_data)
        self.assertTrue(self.uuid_table.uuid_to_data(uuid), telegram_data)

        self.assertEqual(len(self.firebase_client.completed_transactions()), 1)


    ############ Test Helper Methods ############################################################

    def setup_firebase_uuid_table(self):
        test_util.print_test_header()
        firestore_uuid_table.log = test_util.TestLogger(__name__)
        self.firebase_client = mock_firebase.MockFirestoreClient('testdata/uuid_mappings.json')
        self.uuid_table = firestore_uuid_table.FirestoreUuidTable(self.firebase_client, 'uuid-table', 'nook-phone-uuid-', None)

    def setup_firebase_uuid_table_live(self):
        if not test_util.setup_live_test(): return False
        firestore_uuid_table.log = test_util.TestLogger(__name__)
        self.firebase_client = live_firebase_client
        self.uuid_table = firestore_uuid_table.FirestoreUuidTable(self.firebase_client, 'test-uuid-table', 'my-uuid-', None)
        return True

    @classmethod
    def setUpClass(cls):
        print("")
        print(f"---- setUp for all tests -----------------------------------------------------------")
        global live_firebase_client
        if test_util.crypto_token_path is not None:
            firebase_cred = credentials.Certificate(test_util.crypto_token_path)
            firebase_admin.initialize_app(firebase_cred)
            live_firebase_client = firestore.client()
        print(f"setUp for all tests complete")

    @classmethod
    def tearDownClass(cls):
        print("")
        print(f"---- tearDown for all tests -----------------------------------------------------------")
        global live_firebase_client
        if live_firebase_client is not None:
            live_firebase_client = None
        print(f"tearDown for all tests complete")


if __name__ == '__main__':
    test_util.setup_all_unittests(sys.argv)
