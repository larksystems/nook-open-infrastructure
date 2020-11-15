import json
import sys
import threading
import time
import traceback

from google.cloud import pubsub_v1

from lib.simple_logger import Logger


# This module cannot import katikati_pylib.logging because that module relies on this one.
# Clients should initialize this global for proper logging
log = Logger("pubsub_util")


class Subscriber:
    """Subscribe to the specific pub/sub channel.

    process_message_funct is called on a background thread once for each message
    where the message passed is an instance of google.cloud.pubsub_v1.subscriber.message.Message.
    It is the responsibility of `process_message_funct` to ack() or nack() each message.
    If an exception occurs in process_message_funct, the exception is logged and and nack() is called.
    """

    # Using pull and using streaming pull were considered.
    # For information about the differences between a pull and a streaming pull,
    # see https://stackoverflow.com/questions/56191518/google-pubsub-pull-vs-streaming-pull-differences
    #
    # Using pull was rejected because of the increased latency.
    # https://googleapis.dev/python/pubsub/latest/subscriber/api/client.html#google.cloud.pubsub_v1.subscriber.client.Client.pull
    #
    # Using streaming_pull was considered because of the lower latency and that it runs on a single thread
    # https://googleapis.dev/python/pubsub/latest/subscriber/api/client.html#google.cloud.pubsub_v1.subscriber.client.Client.streaming_pull
    # but could not get it to work and ultimately rejected because a comment near the end of
    # https://github.com/googleapis/google-cloud-python/issues/8555
    # "
    #   streaming_pull is a low-level method and you're really not intended to use it directly.
    #   The pubsub streaming pull protocol is pretty complicated
    #   and subscribe does a lot of machinery to properly open and maintain the stream.
    # "

    def __init__(self, crypto_token_path, topic_name, subscription_name, process_message_funct):
        publisher = Publisher(crypto_token_path, topic_name)
        self.client = pubsub_v1.SubscriberClient.from_service_account_json(publisher.crypto_token_path)

        log.debug("Create subscription")
        self.subscription_path = f"projects/{publisher.project_id}/subscriptions/{publisher.project_id}-{subscription_name}"
        try:
            self.client.create_subscription(name=self.subscription_path, topic=publisher.topic_path)
            log.debug(f"Subscription created: {self.subscription_path}")
        except:
            log.debug(f"Error on subscription creation for {self.subscription_path}: {sys.exc_info()[0]}")

        self.subscription = None
        self.subscribe(process_message_funct)

    def subscribe(self, process_message_funct):
        """Process messages via the supplied function

        See https://cloud.google.com/pubsub/docs/pull
        """

        if self.subscription is not None:
            raise AssertionError("active subscription")
        self.process_message_funct = process_message_funct
        self.subscription = self.client.subscribe(self.subscription_path, self.process_message)
        log.debug("Subscribed")

    def process_message(self, message):
        self.process_message_funct(message)

    def wait(self):
        """Blocks until cancel() is called or an exception occurs"""
        if self.subscription is None:
            raise AssertionError("no active subscription")
        self.subscription.result()

    def cancel(self):
        """Signal the subscription process to shutdown gracefully and exit"""
        if self.subscription is not None:
            self.subscription.cancel()
            self.subscription = None


class MessageSequencer:
    """A message processor for sequencing Google pub/sub messages that arrive sequentially but on separate threads.
    This processor ensures that
    1) messages are processed sequentially in the order in which process_message is called
    2) only one message is processed at a time even though the messages arrive on multiple threads
    3) if an exception occurs when when processing a message then subsequent messages are nacked and not processed
    """
    def __init__(self, process_message_funct):
        assert process_message_funct is not None
        self.process_message_funct = process_message_funct
        self.message_processing_lock = threading.Lock()
        self.message_processing_queue = []
        self.last_exception = None

    def process_message(self, message):
        """Process the message by forwarding it to the process_message_funct associated with this instance."""

        # As discussed in https://github.com/larksystems/KK-Project-2020-COVID19-SOM/pull/34
        # pub/sub delivers the messages in the correct order, but they can end up being processed out of order
        # if the current thread is interrupted and given that there are multiple pub/sub threads.
        #
        # As a stop gap measure, we append the message to the end of a queue before the lock
        # and pop the next message to process off the queue after the lock to ensure proper ordering.
        #
        # There is a very slight chance that the current thread will be interrupted by the garbage collector
        # during the call to append causing the queue to be corrupted. To eliminate this, we need to rewrite this
        # to have process_message append the message to a synchronized queue.
        # See https://docs.python.org/3/library/queue.html

        exception_on_this_thread = None

        self.message_processing_queue.append(message)
        with self.message_processing_lock:
            message = self.message_processing_queue.pop(0)

            if self.last_exception is None:
                try:
                    self.process_message_funct(message)
                except Exception as e:
                    self.last_exception = e
                    exception_on_this_thread = e
                    log.warning(f"process message exception: {e}")
                    log.warning(traceback.format_exc())

        if self.last_exception is not None:
            try:
                # Nack the message so that it can be properly processed at another time
                message.nack()
                log.debug(f"nacked {message}")
            except Exception as e1:
                log.warning(f"{e1} - failed to nack message: {message}")

            if exception_on_this_thread is not None:
                # If this was the thread that had the exception, then
                # 1) sleep to give other threads a chance to nack their messages
                # 2) re-raise the exception to terminate the subscription
                time.sleep(1)
                raise exception_on_this_thread
            else:
                # Give the failing thread a chance to re-raise the exception
                time.sleep(2)

        # We should flush the system buffer so that current log entries can be seen in the console and subsequent file
        # but Python has this long standing potential deadlock when calling flush() in the presence of multiple threads.
        # See https://stackoverflow.com/questions/44069717/empty-python-process-hangs-on-join-sys-stderr-flush
        # and https://bugs.python.org/issue6721
        #sys.stdout.flush()


class Publisher:
    """Publish to the specified pub/sub channel."""

    def __init__(self, crypto_token_path, topic_name):
        self.crypto_token_path = crypto_token_path
        with open(crypto_token_path) as f:
            self.project_id = json.load(f)["project_id"]
        self.topic_path = f"projects/{self.project_id}/topics/{self.project_id}-{topic_name}"
        self.client = pubsub_v1.PublisherClient.from_service_account_json(crypto_token_path)

        print("Create topic")
        try:
            self.client.create_topic(name=self.topic_path)
            print(f"topic created: {self.topic_path}")
        except NameError as e:
            print(f"Error on topic creation for: {self.topic_path}, {e}")
        except:
            print(f"Error on topic creation for {self.topic_path}: {sys.exc_info()[0]}")

    def publish(self, message):
        """Publish a single message (a dictionary)"""
        data = json.dumps({"payload": message}).encode("utf-8")
        return self.client.publish(self.topic_path, data=data)
