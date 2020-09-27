import datetime

# Python's default utc now implementation returns a non-tz aware
# time which creates a lot of confusion and interop problems around
# clock change

def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)
