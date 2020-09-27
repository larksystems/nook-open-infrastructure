import inspect
import json
import os

import time
import unittest

from lib.simple_logger import Logger
from lib.pubsub_util import Subscriber

crypto_token_path = None


class TestLogger(Logger):
    def __init__(self, logger_name):
        super().__init__(logger_name)

    def cloud_log(self, log_level, message):
        pass


class MockPubSubMessage:
    def __init__(self, data):
        self.data = data
        self.acked = False
        self.nacked = False

    def ack(self):
        self.acked = True

    def nack(self):
        self.nacked = True


class MockSubscriber(object):
    def __init__(self):
        self.canceled = False

    def cancel(self):
        self.canceled = True


class ProxyPubSubMessage(object):
    def __init__(self, message):
        self.message = message
        self.data = message.data
        self.acked = False
        self.nacked = False

    def ack(self):
        self.acked = True
        self.message.ack()

    def nack(self):
        self.nacked = True
        self.message.nack()


class MessageCollector(object):
    def __init__(self, process_message_funct=None):
        self.process_message_funct = process_message_funct
        self.payloads = []
        self.ack_count = 0
        self.nack_count = 0

    def process_message(self, message):
        payload = json.loads(message.data)['payload']
        self.payloads.append(payload)
        if self.process_message_funct is not None:
            proxy = ProxyPubSubMessage(message)
            try:
                self.process_message_funct(proxy)
            finally:
                if proxy.acked:
                    self.ack_count += 1
                elif proxy.nacked:
                    self.nack_count += 1
        else:
            # put process order critical operations above this line
            # because print() and ack() can trigger thread switching
            # causing message process ordering to change
            print(f"payload: {payload}")
            message.ack()

    def wait_for_messages(self, num_expected_messages, wait_time_in_seconds=5):
        print(f"Waiting up to {wait_time_in_seconds} seconds for pub/sub messages...")
        end_time = time.time() + wait_time_in_seconds
        while len(self.payloads) < num_expected_messages and time.time() < end_time:
            time.sleep(0.01)

        # wait a bit more for the message ack to be sent
        time.sleep(1)

        payloads = []
        payloads.extend(self.payloads)
        self.payloads.clear()
        return payloads


def discard_messages(crypto_token_path, topic_name, subscription_name, wait_time_in_seconds=3):
    print("discarding messages")
    collector = MessageCollector()
    subscriber = Subscriber(crypto_token_path, topic_name, subscription_name, collector.process_message)
    discarded_payloads = collector.wait_for_messages(1000, wait_time_in_seconds)
    subscriber.cancel()
    return discarded_payloads


def name_of_test_method():
    stack = inspect.stack()
    index = 1
    while index < 20:
        testname = f"{stack[index].function}"
        if testname.startswith("test_"):
            return testname
        # TODO auto detect setUpClass and return "<test-class-name> setUp"
        index += 1
    raise AssertionError("Cannot determine name of test method")


def print_test_header(test_name=None):
    if test_name is None:
        test_name = name_of_test_method()
    print("")
    print(f"---- {test_name} -----------------------------------------------------------")


def setup_live_test():
    print_test_header()
    if crypto_token_path is not None:
        return True
    else:
        print("Skipping live test... use option --live path/to/crypto/token to run this test")
        return False


def path_for_temp_test_file(file_name, delete_if_exists=True):
    """
    Return the absolute path to a temporary file with the given name

    :param file_name:        the name of the temporary file
    :param delete_if_exists: if True (the default), then the file will be deleted before returning
    :return: the absolute path to the temporary file
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    while True:
        cache_disk_path = os.path.join(dir_path, 'KK-CacheDisk')
        if os.path.exists(cache_disk_path):
            temp_test_dir = os.path.join(cache_disk_path, "Test")
            if not os.path.exists(temp_test_dir):
                os.mkdir(temp_test_dir)
            break
        dir_path = os.path.dirname(dir_path)
    file_path = os.path.join(temp_test_dir, file_name)
    if delete_if_exists and os.path.exists(file_path):
        os.remove(file_path)
    return file_path


def sort_live_tests_at_end(loader, name1, name2):
    """Return -1 if name1 < name2, 0 if name1 == name2 and 1 if name1 > name2
        except that tests named *_live and *_long are sorted at the end"""
    if name1.endswith("_long"):
        if name2.endswith("_long"):
            return compare_method_names(name1, name2)
        else:
            return 1
    elif name2.endswith("_long"):
        return -1

    if name1.endswith("_live"):
        if name2.endswith("_live"):
            return compare_method_names(name1, name2)
        else:
            return 1
    elif name2.endswith("_live"):
        return -1

    return compare_method_names(name1, name2)


def compare_method_names(name1, name2):
    """Return -1 if name1 < name2, 0 if name1 == name2 and 1 if name1 > name2"""
    if name1 < name2:
        return -1
    else:
        return 1


def setup_all_unittests(argv):
    global crypto_token_path
    unittest.TestLoader.sortTestMethodsUsing = sort_live_tests_at_end
    if len(argv) >= 2 and argv[1] == "--live":
        if len(argv) == 2:
            print(f"Expected --live path/to/crypto/token")
            exit(1)
        crypto_token_path = argv[2]
        argv = argv[2:]
    unittest.main(argv=argv)
