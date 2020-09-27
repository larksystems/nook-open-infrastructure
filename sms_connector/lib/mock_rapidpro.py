from datetime import datetime

from requests.exceptions import HTTPError
from temba_client.exceptions import TembaConnectionError

from lib import test_util


log = test_util.TestLogger(__name__)


class MockRapidProClient(object):
    def __init__(self):
        self.outgoing = []
        self.incoming = []
        self.retry_count = 0

    def get_raw_messages(self, created_after_inclusive=None):
        if self.retry_count > 0:
            self.retry_count -= 1
            raise TembaConnectionError('pretend exception for testing')
        result = self.incoming
        self.incoming = []
        return result

    def send_message_to_urn(self, message, urn, interrupt=False):
        if self.retry_count > 0:
            self.retry_count -= 1
            raise HTTPError('pretend exception for testing')
        log.info(f"Mock: Skipping send sms: {urn}, interrupt={interrupt} --> {message}")
        self.outgoing.append((urn, message))

    def send_message_to_urns(self, message, urns, interrupt=False):
        if self.retry_count > 0:
            self.retry_count -= 1
            raise HTTPError('pretend exception for testing')
        log.info(f"Mock: Skipping send smses: {urns}, interrupt={interrupt} --> {message}")
        self.outgoing.append((urns, message))


class MockRapidProMessage(object):
    def __init__(self, created_on, urn, direction, text):
        self.created_on = datetime.fromisoformat(created_on)
        self.urn = urn
        self.direction = direction
        self.text = text
        pass
