import sys

from lib import test_util

from test_firestore_uuid_table import FirestoreUuidTableTestCase
from test_rapidpro_adapter_cli import RapidProAdapterCliTestCase
from test_rapidpro_incoming import RapidProIncomingTestCase
from test_rapidpro_outgoing import RapidProOutgoingTestCase

if __name__ == '__main__':
    argv = []
    argv.extend(sys.argv)
    argv.append(FirestoreUuidTableTestCase.__name__)
    argv.append(RapidProIncomingTestCase.__name__)
    argv.append(RapidProOutgoingTestCase.__name__)
    argv.append(RapidProAdapterCliTestCase.__name__)
    test_util.setup_all_unittests(argv)
