#!/usr/bin/python
import time
from datetime import datetime, timedelta


def date2stamp(date_str):
    timeArray = time.strptime(date_str, "%Y-%m-%d")
    return time.mktime(timeArray)


def stamp2date(stamp):
    return time.strftime("%Y-%m-%d", time.localtime(stamp))


def stamp2time(stamp):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stamp))


def time2stamp(time_str):
    timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return time.mktime(timeArray)


def datetime2date(d):
    if not isinstance(d, datetime):
        d = datetime.now()
    return d.strftime("%Y-%m-%d")


def datetime2time(d):
    if not isinstance(d, datetime):
        d = datetime.now()
    return d.strftime("%Y-%m-%d %H:%M:%S")


def datetime2stamp(d):
    if not isinstance(d, datetime):
        d = datetime.now()
    return time.mktime(d.timetuple())


def time2datetime(time_str=None):
    if not time_str:
        return datetime.now()
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")


def stamp2datetime(stamp=None):
    if not stamp:
        stamp = time.time()
    return datetime.fromtimestamp(stamp)

if __name__ == "__main__":
    print time2stamp("2016-6-24 15:23:00")
