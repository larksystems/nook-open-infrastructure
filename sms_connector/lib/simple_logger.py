import json
import pytz
import socket
import uuid
from collections import OrderedDict
from datetime import datetime


def utc_now_as_iso_string():
    """
    :return: Current system time in UTC in ISO 8601 string format.
    :rtype: str
    """
    return pytz.utc.localize(datetime.utcnow()).isoformat(timespec="microseconds")


class Logger(object):

    def __init__(self, logger_name):
        self.logger_name = logger_name

    def log(self, log_level, message):
        print(f"{utc_now_as_iso_string()} {log_level.string_value} "
              f"{self.logger_name}: {message}")

    def trace(self, message):
        self.log(LogLevels.TRACE, message)

    def debug(self, message):
        self.log(LogLevels.DEBUG, message)

    def info(self, message):
        self.log(LogLevels.INFO, message)

    def perf_start(self, action, target=None):
        token = _PerfToken(action, target)
        self.log(LogLevels.PERF, token.to_log_entry())
        return token

    def perf_end(self, perf_token):
        self.log(LogLevels.PERF, perf_token.to_log_entry(datetime.utcnow()))

    def notify(self, message):
        self.log(LogLevels.NOTIFY, message)

    def warning(self, message):
        self.log(LogLevels.WARNING, message)

    def error(self, message):
        self.log(LogLevels.ERROR, message)

    def audit(self, message):
        self.log(LogLevels.AUDIT, message)


class _PerfToken(object):
    # TODO Consider adding an API allowing specification of max time after which
    #      a backround process will scream loudly that "this task took too long"
    def __init__(self, action, target):
        self.start_time = datetime.utcnow()
        self.job_id = str(uuid.uuid4()).split('-')[0]
        self.action = action
        self.target = target

    def to_log_entry(self, end_time=None):
        # use an ordered dictionary so that the log entries are this specific order
        # with job_id first and elapsed_ms last
        entry = OrderedDict({
            "job_id": self.job_id,
            "action": self.action
        })
        if self.target is not None:
            entry["target"] = self.target
        if end_time is None:
            entry["status"] = "start"
        else:
            entry["status"] = "end"
            elapse_time = end_time - self.start_time
            milliseconds = round(elapse_time.total_seconds() * 1e6) / 1e3
            entry["elapsed_ms"] = "{0:.3f}".format(milliseconds)
        return json.dumps(entry)

class LogLevel(object):
    def __init__(self, string_value, numeric_value):
        self.string_value = string_value
        self.numeric_value = numeric_value


class LogLevels(object):
    AUDIT = LogLevel("AUDIT", 60)
    ERROR = LogLevel("ERROR", 50)
    WARNING = LogLevel("WARNING", 40)
    NOTIFY = LogLevel("NOTIFY", 30)
    PERF = LogLevel("PERF", 25)
    INFO = LogLevel("INFO", 20)
    DEBUG = LogLevel("DEBUG", 10)
    TRACE = LogLevel("TRACE", 0)
