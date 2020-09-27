import uuid

from firebase_admin import firestore

from lib.simple_logger import Logger

BATCH_SIZE = 500
_UUID_KEY_NAME = "uuid"

log = None


class FirestoreUuidTable(object):
    """
    Mapping table between a string and a random UUID backed by Firestore
    """
    def __init__(self, firebase_client, table_name, uuid_prefix, crypto_token_path):
        global log
        if log is None:
            log = Logger(__name__)

        self.firebase_client = firebase_client
        self._table_name = table_name
        self._uuid_prefix = uuid_prefix
        self._data_to_uuid = None
        self._uuid_to_data = None

    def _data_to_uuid_collection(self):
        return self.firebase_client.collection(f"tables/{self._table_name}/mappings")

    def cache_uuid_table(self):
        """If the UUID table lookup has not been cached locally, then this method will block and cache
        the entire UUID table from firestore... which may take a while.
        This will only happen once per instance of FirestoreUuidTable
        """
        if self._data_to_uuid is not None:
            return

        self._data_to_uuid = dict()
        self._uuid_to_data = dict()
        count = 0

        # We cannot be sure that stream() will return all of the entries in the firebase UUID table
        # but new_or_existing_uuid_for_data() ensures that any entries that are not loaded will not be overwritten.
        # TODO consider adding exception handling to gracefully degrade if an exception occurs (e.g. network failure)
        # TODO consider adding field containing # of mappings so that we know if stream fails to return all mappings
        log.info(f"Loading uuid mappings...")
        for doc in self._data_to_uuid_collection().stream():
            data = doc.id
            uuid = doc.to_dict()[_UUID_KEY_NAME]
            self._data_to_uuid[data] = uuid
            self._uuid_to_data[uuid] = data
            count += 1
        log.info(f"Loaded {count} uuid mappings")

    def data_to_uuid_batch(self, list_of_data_requested):
        """
        Return a mapping of data items to UUIDs, creating and storing UUIDs if necessary

        :param list_of_data_requested: a list of data itmes
        :return: a mapping of data item to UUID
        """
        self.cache_uuid_table()

        log.info(f"Sourcing uuids for {len(list_of_data_requested)} data items...")
        ret = dict()
        for data_requested in set(list_of_data_requested):
            # If large #s of new data-to-uuid mappings need to be created and the code below becomes a bottleneck,
            # then `data_to_uuid` and `new_or_existing_uuid_for_data` can be inlined in this method
            # and the transaction optimized for this situation.
            ret[data_requested] = self.data_to_uuid(data_requested)
        return ret

    def data_to_uuid(self, data):
        """
        Return the UUID for the specified data, creating and storing the UUID if necessary

        :param data: the data associated with the UUID
        :return: the UUID
        """
        self.cache_uuid_table()

        # If the cache has not been initialized, then just lookup and return the UUID
        if self._data_to_uuid is None:
            return self.new_or_existing_uuid_for_data(data)

        # Return the cached UUID if it exists
        if data in self._data_to_uuid:
            return self._data_to_uuid[data]

        # Generate, store, cache, and return a new UUID
        new_uuid = self.new_or_existing_uuid_for_data(data)
        self._data_to_uuid[data] = new_uuid
        self._uuid_to_data[new_uuid] = data
        return new_uuid

    def uuid_to_data(self, uuid_to_lookup):
        """Return the data associated with the specified UUID,
        or fail with LookupError if one is not found.

        If the UUID table lookup has not been cached locally, then this method will block and cache
        the entire UUID table from firestore... which may take a while.
        This will only happen once per instance of FirestoreUuidTable

        :param uuid_to_lookup: the UUID to be matched
        :return: the associated data or LookupError if the UUID could not be found
        """
        self.cache_uuid_table()
        if uuid_to_lookup in self._uuid_to_data:
            return self._uuid_to_data[uuid_to_lookup]
        raise LookupError(f"Failed to find data for uuid {uuid_to_lookup}")

    def uuid_to_data_batch(self, uuids_to_lookup):
        """Return a mapping of UUIDs to associated data.
        Fail with LookupError if any of the UUIDs could not be found.

        If the UUID table lookup has not been cached locally, then this method will block and cache
        the entire UUID table from firestore... which may take a while.
        This will only happen once per instance of FirestoreUuidTable

        :param uuids_to_lookup: a list of UUIDs to be matched
        :return: a mapping of UUID to data
        """
        self.cache_uuid_table()
        results = {}
        for uuid_lookup in uuids_to_lookup:
            if uuid_lookup in self._uuid_to_data:
                results[uuid_lookup] = self._uuid_to_data[uuid_lookup]
            else:
                raise LookupError(f"Failed to find data for uuid {uuid_lookup}")
        return results

    def new_or_existing_uuid_for_data(self, data):
        """Return the uuid for the specified data, creating and storing the new uuid in firebase if necessary"""
        transaction = self.firebase_client.transaction()
        doc_ref = self._data_to_uuid_collection().document(data)
        return _new_or_existing_uuid_for_data_transaction(transaction, doc_ref, self._uuid_prefix)

    @staticmethod
    def generate_new_uuid(prefix):
        return prefix + str(uuid.uuid4())


@firestore.transactional
def _new_or_existing_uuid_for_data_transaction(transaction, doc_ref, prefix):
    """
    If the specified document exists, then return the existing UUID stored in that document,
    otherwise generate a new UUID, store the UUID in the document, and return the UUID.

    The "magic" @firestore.transactional annotation makes this method a firebase transaction
    See https://firebase.google.com/docs/firestore/manage-data/transactions

    :param transaction: the transaction
    :param doc_ref: the document with document id == data (may not exist yet)
    :param prefix: the prefix of the UUID if it needs to be generated
    :return: the existing or newly generated UUID associated with the specified document
    """

    # Check for an existing data --> uuid mapping
    log.debug(f"transaction: look up uuid for {type(doc_ref)}")
    snapshot = doc_ref.get(transaction=transaction)
    if snapshot.exists:
        existing_uuid = snapshot.get(_UUID_KEY_NAME)
        log.debug(f"transaction: found existing uuid: {existing_uuid}")
        return existing_uuid

    # Create and store a new uuid for the data
    new_uuid = FirestoreUuidTable.generate_new_uuid(prefix)
    transaction.set(doc_ref, {
        _UUID_KEY_NAME: new_uuid
    })
    log.audit(f"transaction: created and stored new uuid: {new_uuid} data: {doc_ref.id}")
    return new_uuid
