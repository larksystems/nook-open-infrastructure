import argparse
import datetime
import json
import os
import sys
import threading
import time

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud import storage

from rapid_pro_tools.rapid_pro_client import RapidProClient

import rapidpro_incoming
import rapidpro_outgoing

from lib import pubsub_util
from lib.firestore_uuid_table import FirestoreUuidTable
from lib.simple_logger import Logger


log = None
process_messages = True


def setup(crypto_token_path, project_name, credentials_bucket_name, sync_token_path, rapid_pro_config_blob_name="rapidpro-config.json"):
    global log
    log = Logger(__name__)
    pubsub_util.log = Logger(pubsub_util.__name__)

    if read_last_update_time(sync_token_path) is None:
        raise AssertionError(f"Missing or empty rapidpro sync token: {sync_token_path}")

    log.info("Downloading Rapid Pro token")
    storage_client = storage.Client.from_service_account_json(crypto_token_path)
    credentials_bucket = storage_client.bucket(credentials_bucket_name)
    credentials_blob = credentials_bucket.blob(rapid_pro_config_blob_name)

    rapid_pro_config_dict = json.loads(credentials_blob.download_as_string())
    rapid_pro_domain = rapid_pro_config_dict["domain"]
    rapid_pro_token = rapid_pro_config_dict["token"]
    log.info(f"Rapid Pro domain: {rapid_pro_domain}")
    log.info(f"Rapid Pro token: {rapid_pro_token[0:6]}...")
    rapidpro_client = RapidProClient(rapid_pro_domain, rapid_pro_token)

    log.info("Setting up Firebase client")
    firebase_cred = credentials.Certificate(crypto_token_path)
    firebase_admin.initialize_app(firebase_cred)
    firebase_client = firestore.client()

    phone_number_uuid_table = new_uuid_table(crypto_token_path, firebase_client)
    rapidpro_lock = threading.Lock()
    rapidpro_incoming.init(crypto_token_path, rapidpro_client, rapidpro_lock, phone_number_uuid_table)
    rapidpro_outgoing.init(crypto_token_path, rapidpro_client, rapidpro_lock, phone_number_uuid_table)


def teardown():
    log.info("teardown outgoing")
    rapidpro_outgoing.teardown()
    log.info("teardown complete")


def new_uuid_table(crypto_token_path, firebase_client):
    phone_number_uuid_table = FirestoreUuidTable(
        firebase_client,
        "uuid-table",
        "nook-phone-uuid-",
        crypto_token_path
    )
    return phone_number_uuid_table


def read_last_update_time(sync_token_path):
    print(f"Reading last update time from {sync_token_path}")
    if os.path.exists(sync_token_path):
        with open(sync_token_path, "r") as f:
            data = f.read()
            if len(data) > 0:
                last_update_token = json.loads(data)
            else:
                print("Empty token file found")
                return None
        print(f"Last update token {last_update_token}")
        return datetime.datetime.fromisoformat(last_update_token["last_update_time"])
    else:
        print("No token file found")
        return None


def write_last_update_time(sync_token_path, before_exec):
    print(f"Writing last update time to {sync_token_path}")
    with open(sync_token_path, "w") as f:
        json.dump({ "last_update_time": before_exec.isoformat() }, f)


def idle_sleep():
    for count in range(0, 50):
        rapidpro_outgoing.check_exception()
        time.sleep(0.1)


def run_inbound_polling(sync_token_path, idle_funct=idle_sleep):
    global process_messages
    last_update_time = read_last_update_time(sync_token_path)
    process_messages = True
    while process_messages:
        before_exec = datetime.datetime.now(datetime.timezone.utc)
        rapidpro_incoming.transfer_messages(created_after_inclusive=last_update_time)
        last_update_time = before_exec
        write_last_update_time(sync_token_path, before_exec)

        # We should flush the system buffer so that current log entries can be seen in the console and subsequent file
        # but Python has this long standing potential deadlock when calling flush() in the presence of multiple threads.
        # See https://stackoverflow.com/questions/44069717/empty-python-process-hangs-on-join-sys-stderr-flush
        # and https://bugs.python.org/issue6721
        #sys.stdout.flush()

        idle_funct()
        log.debug("idle_funct() completed")


class DefaultHelpArgParser(argparse.ArgumentParser):
    def error(self, message):
        print(f"error: {message}")
        print(f"")
        self.print_help()
        sys.exit(2)


if __name__ == '__main__':
    parser = DefaultHelpArgParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Handles incomming and outgoing sms communications with RapidPro")
    required_named = parser.add_argument_group('required named arguments')
    required_named.add_argument("--crypto-token-file", required=True,
                        help="Project crypto file")
    required_named.add_argument("--project-name", required=True,
                        help="Project name")
    required_named.add_argument("--credentials-bucket-name", required=True,
                        help="Bucket containing RapidPro credentials token")
    required_named.add_argument("--last-update-token-path", required=True,
                        help="File storing a timestamp sync token used for incrementally polling messages from RapidPro")

    args = parser.parse_args(sys.argv[1:])

    setup(args.crypto_token_file, args.project_name, args.credentials_bucket_name, args.last_update_token_path)

    try:
        run_inbound_polling(args.last_update_token_path)
    except KeyboardInterrupt:
        print("")
        log.info("Keyboard interrupt")
    finally:
        teardown()
