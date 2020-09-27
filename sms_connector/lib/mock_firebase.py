import json
import re
import time

# regex: leading `^` means "not" any of these valid characters
# regex: the character sequence `\\-` translates to `-` which cannot be directly represented
_invalid_firebase_path_regex = re.compile('[^A-Za-z0-9/+:_\\-\ #]')


class _MockChangeType:
    def __init__(self, name):
        self.name = name


_MOCK_CHANGE_TYPE_ADDED = _MockChangeType('ADDED')


class MockFirestoreClient:
    def __init__(self, json_file_path=None):
        self.num_batch_commits = 0
        if json_file_path is not None:
            with open(json_file_path, 'r') as f:
                self.data = json.load(f)
        else:
            self.data = {}
        # the active uncommitted batch or None
        self._batch = None
        # the active uncompleted transaction or None
        self._transaction = None
        # a list of document change tuples (doc path, doc data) for test assertions
        self._changes = []
        self._completed_transactions = []

    def assert_in_transaction_scope(self, transaction):
        self.assert_is_current_transaction(transaction)
        if transaction is not None:
            transaction.assert_in_transaction_scope()

    def assert_is_current_transaction(self, transaction):
        if self._transaction != transaction:
            if transaction is None:
                raise AssertionError("missing transaction")
            else:
                raise AssertionError(f"unknown transaction {transaction}")

    def batch(self):
        if self._transaction is not None:
            raise AssertionError("transaction already in process")
        if self._batch is None:
            self._batch = MockBatch(self)
        return self._batch

    def batch_committed(self, batch):
        if self._batch != batch:
            raise Exception("unknown batch committed")
        self._batch = None
        self.num_batch_commits += 1

    def changes(self):
        if self._transaction is not None:
            raise AssertionError("current transaction has not completed")
        if self._batch is not None:
            raise AssertionError("current batch has not been committed")
        return self._changes

    def completed_transactions(self):
        if self._transaction is not None:
            raise AssertionError("current transaction has not completed")
        if self._batch is not None:
            raise AssertionError("current batch has not been committed")
        return self._completed_transactions

    def collection(self, collection_root):
        return MockFirestoreCollection(self, collection_root)

    def document(self, path):
        return MockFirestoreRef(self, path)

    def set_doc(self, collection_path, doc_id, new_doc_data):
        reference_path = "/".join([collection_path, doc_id])
        self._changes.append((reference_path, new_doc_data))
        self.set_doc_data(collection_path, doc_id, new_doc_data)

    def set_doc_data(self, collection_path, doc_id, new_doc_data):
        reference_path = "/".join([collection_path, doc_id])
        _check_collection_path(collection_path)
        _check_document_id(doc_id)
        doc_data = new_doc_data.copy()
        doc_data["__id"] = doc_id
        doc_data["__reference_path"] = reference_path
        doc_data["__subcollections"] = []
        doc_data_list = _raw_doc_list(self.data, collection_path, add_if_absent=True)
        for doc_index in range(0, len(doc_data_list)):
            if doc_data_list[doc_index]["__id"] == doc_id:
                doc_data_list[doc_index] = doc_data
                return doc_data
        doc_data_list.append(doc_data)
        return doc_data

    def transaction(self):
        if self._batch is not None:
            raise AssertionError("batch already in process")
        if self._transaction is not None:
            raise AssertionError("transaction already in process")
        self._transaction = MockTransaction(self)
        return self._transaction

    def transaction_complete(self, transaction):
        self.assert_is_current_transaction(transaction)
        self._completed_transactions.append(transaction)
        self._transaction = None


class MockFirestoreCollection:
    def __init__(self, client, collection_root):
        _check_collection_path(collection_root)
        self.client = client
        self.collection_root = collection_root

    def on_snapshot(self, callback):
        return MockSnapshotSubscription(self, callback)

    def get(self):
        docs = []
        for doc_data in _raw_doc_list(self.client.data, self.collection_root):
            docs.append(MockFirestoreDoc.from_data(self.client, doc_data))
        return docs

    def get_doc(self, doc_id):
        for doc_data in _raw_doc_list(self.client.data, self.collection_root):
            if doc_data["__id"] == doc_id:
                return MockFirestoreDoc.from_data(self.client, doc_data)
        return MockFirestoreDoc.does_not_exist(self.client, self.collection_root, doc_id)

    def document(self, doc_id):
        return self.client.document(f"{self.collection_root}/{doc_id}")

    def stream(self):
        return self.get()

    def where(self, key, comparison, value):
        return MockFirestoreQuery(self, key, comparison, value)


class MockSnapshotSubscription(object):
    def __init__(self, collection, callback):
        self.collection = collection
        self.callback = callback
        changes = []
        for doc in self.collection.get():
            changes.append(MockChange(doc))
        self.callback(None, changes, None)

    def push_change(self, doc_id, new_doc_data):
        client = self.collection.client
        doc_data = client.set_doc_data(self.collection.collection_root, doc_id, new_doc_data)
        changes = [MockChange(MockFirestoreDoc.from_data(client, doc_data))]
        self.callback(None, changes, None)

    def unsubscribe(self):
        pass


class MockBatch(object):
    def __init__(self, client):
        self._client = client
        self._change_count = 0

    def set(self, doc_ref, doc_data):
        doc_ref.set(doc_data)
        self._change_count += 1

    def update(self, doc_ref, data_to_merge):
        doc_ref.update(data_to_merge)
        self._change_count += 1

    def commit(self):
        if self._change_count > 500:
            raise Exception(f"Max 500 changes per batch, but found {self._change_count}")
        self._client.batch_committed(self)
        self._change_count = None


class MockChange(object):
    def __init__(self, document):
        self.document = document
        self.type = _MOCK_CHANGE_TYPE_ADDED


class MockFirestoreDoc:
    @classmethod
    def from_data(cls, client, doc_data):
        data = doc_data.copy()
        id = data.pop("__id")
        reference_path = data.pop("__reference_path")
        # TODO populate subcollections
        data.pop("__subcollections")
        return MockFirestoreDoc(client, id, reference_path, data)

    @classmethod
    def does_not_exist(cls, client, collection_root, id):
        return MockFirestoreDoc(client, id, "/".join([collection_root, id]), None)

    def __init__(self, client, id, reference_path, data):
        _check_document_id(id)
        _check_document_path(reference_path)
        self.client = client
        self.id = id
        self.reference = MockFirestoreRef(client, reference_path)
        self.data = data
        self.exists = data is not None

    def get(self, key):
        if self.exists:
            return self.data[key]
        raise Exception(f"document does not exist: {self.reference.path}")

    def to_dict(self):
        if self.exists:
            return self.data
        raise Exception(f"document does not exist: {self.reference.path}")


class MockFirestoreRef:
    def __init__(self, client, path):
        _check_document_path(path)
        self.client = client
        self.path = path
        self.id = path.split("/")[-1]

    def __str__(self):
        return f"MockFirestoreRef({self.path})"

    def get(self, transaction=None):
        self.client.assert_in_transaction_scope(transaction)
        path_segments = self.path.split("/")
        collection_path = "/".join(path_segments[0:-1])
        doc_id = path_segments[-1]
        return self.client.collection(collection_path).get_doc(doc_id)

    def set(self, new_doc_data):
        path_segments = self.path.split("/")
        collection_path = "/".join(path_segments[0:-1])
        doc_id = path_segments[-1]
        self.client.set_doc(collection_path, doc_id, new_doc_data)

    def update(self, modifications):
        doc_data = self.get().to_dict()
        for field_path, value in modifications.items():
            field_path_segments = field_path.split('/')
            field = doc_data
            while len(field_path_segments) > 1:
                field = field[self._key_or_index(field_path_segments.pop(0))]
            key_or_index = self._key_or_index(field_path_segments[0])
            if type(value).__name__ == "ArrayUnion":
                new_value = field[key_or_index]
                for elem in value.values:
                    if elem not in new_value:
                        new_value.append(elem)
                field[key_or_index] = new_value
            else:
                field[key_or_index] = value
        self.set(doc_data)

    def _key_or_index(self, keyString):
        try:
            return int(keyString)
        except ValueError:
            return keyString


class MockFirestoreQuery:
    def __init__(self, collection, key, comparison, value):
        self.collection = collection
        self.key = key
        self.comparison = comparison
        self.value = value

    def get(self):
        docs = []
        for doc in self.collection.get():
            if self.comparison == u"==":
                if doc.data[self.key] == self.value:
                    docs.append(doc)
            else:
                raise Exception(f"comparison not supported: {self.comparison}")
        return docs


class MockTransaction(object):
    def __init__(self, client):
        self.client = client
        self._max_attempts = 3
        self._id = f"mock-transaction-{time.process_time_ns()}"
        self._changes = None

    def _begin(self, retry_id):
        self._changes = []

    def _clean_up(self):
        pass

    def _commit(self):
        for change_funct in self._changes:
            change_funct()
        self._changes = None
        self.client.transaction_complete(self)

    def _rollback(self):
        self._changes = None
        self.client.transaction_complete(self)

    def assert_in_transaction_scope(self):
        if self._changes is None:
            raise AssertionError("operation outside @firestore.transactional scope")

    def set(self, doc_ref, new_data):
        self.assert_in_transaction_scope()

        def change_funct():
            doc_ref.set(new_data)

        self._changes.append(change_funct)


def _check_collection_path(collection_path):
    _check_firestore_path(collection_path)
    if len(collection_path.split("/")) % 2 != 1:
        raise Exception(f"Invalid collection path: {collection_path}")


def _check_document_id(doc_id):
    _check_firestore_path(doc_id)
    if doc_id.find("/") != -1:
        raise Exception(f"Invalid document id: {doc_id}")


def _check_document_path(reference_path):
    _check_firestore_path(reference_path)
    if len(reference_path.split("/")) % 2 != 0:
        raise Exception(f"Invalid document reference path: {reference_path}")


def _check_firestore_path(path: str):
    if path.split('/').count('nook_conversations') > 0:
        raise Exception(f"Invalid path to old conversations: {path}")
    if _invalid_firebase_path_regex.search(path) is not None:
        raise Exception(f"Invalid character in firebase path: {path}")


def _raw_doc_list(client_data, collection_path, add_if_absent=False):
    # HACK there is one nested collection that is stored in the top level
    if collection_path == "tables/uuid-table/mappings":
        return client_data[collection_path]

    data = client_data
    depth = 0
    for key in collection_path.split('/'):
        if depth % 2 == 0:
            # looking for collection (list of documents) in top level dictionary or nested document data
            if key not in data:
                if not add_if_absent:
                    raise Exception(f"Missing id '{key}' in {collection_path}")
                if depth > 0:
                    data["__subcollections"].append(key)
                data[key] = []
            data = data[key]
        else:
            # looking for document in list with doc_id == key
            found = False
            for doc_data in data:
                if doc_data["__id"] == key:
                    data = doc_data
                    found = True
                    break
            if not found:
                if not add_if_absent:
                    raise Exception(f"Missing id '{key}' in {collection_path}")
                doc_data = {
                    "__id": key,
                    "__subcollections": []
                }
                data.append(doc_data)
                data = doc_data
        depth += 1
    return data
