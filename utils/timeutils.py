#!/usr/bin/python
import time
from datetime import datetime, timedelta
_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
_DATE_FORMAT = '%Y-%m-%d'


def date2stamp(date_str, _format=_DATE_FORMAT):
    timeArray = time.strptime(date_str, date_str)
    return time.mktime(timeArray)


def stamp2date(stamp, _format=_DATE_FORMAT):
    return time.strftime(_format, time.localtime(stamp))


def stamp2time(stamp, _format=_TIME_FORMAT):
    return time.strftime(_format, time.localtime(stamp))


def time2stamp(time_str, _format=_TIME_FORMAT):
    timeArray = time.strptime(time_str, _format)
    return time.mktime(timeArray)


def datetime2date(d, _format=_DATE_FORMAT):
    if not isinstance(d, datetime):
        d = datetime.now()
    return d.strftime(_format)


def datetime2time(d, _format=_TIME_FORMAT):
    if not isinstance(d, datetime):
        d = datetime.now()
    return d.strftime(_format)


def datetime2stamp(d):
    if not isinstance(d, datetime):
        d = datetime.now()
    return time.mktime(d.timetuple())


def time2datetime(time_str=None, _format=_TIME_FORMAT):
    if not time_str:
        return datetime.now()
    return datetime.strptime(time_str, _format)


def stamp2datetime(stamp=None):
    if not stamp:
        stamp = time.time()
    return datetime.fromtimestamp(stamp)

if __name__ == "__main__":
    print time2stamp("2016-6-24", '%Y-%m-%d')
