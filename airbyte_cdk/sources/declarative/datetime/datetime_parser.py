#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import datetime
from typing import Union

import dateparser


class DatetimeParser:
    """
    Parses and formats datetime objects according to a specified format.

    This class mainly acts as a wrapper to properly handling timestamp formatting through the "%s" directive.

    %s is part of the list of format codes required by  the 1989 C standard, but it is unreliable because it always return a datetime in the system's timezone.
    Instead of using the directive directly, we can use datetime.fromtimestamp and dt.timestamp()
    """

    _UNIX_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

    def __init__(self):
        self._parser = dateparser.date.DateDataParser

    def parse(self, date: Union[str, int], format: str) -> datetime.datetime:
        # "%s" is a valid (but unreliable) directive for formatting, but not for parsing
        # It is defined as
        # The number of seconds since the Epoch, 1970-01-01 00:00:00+0000 (UTC). https://man7.org/linux/man-pages/man3/strptime.3.html
        #
        # The recommended way to parse a date from its timestamp representation is to use datetime.fromtimestamp
        # See https://stackoverflow.com/a/4974930
        if format == "%s":
            return datetime.datetime.fromtimestamp(int(date), tz=datetime.timezone.utc)
        elif format == "%s_as_float":
            return datetime.datetime.fromtimestamp(float(date), tz=datetime.timezone.utc)
        elif format == "%ms":
            return self._UNIX_EPOCH + datetime.timedelta(milliseconds=int(date))

        settings = dateparser.conf.Settings().replace(
            **{"TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True}
        )

        parsed_datetime = dateparser.date.parse_with_formats(
            str(date), date_formats=[format], settings=settings
        ).date_obj

        if not parsed_datetime:
            raise ValueError(f"Could not parse {date} as {format}")

        if self._is_naive(parsed_datetime):
            return parsed_datetime.replace(tzinfo=datetime.timezone.utc)
        return parsed_datetime

    def format(self, dt: datetime.datetime, format: str) -> str:
        # strftime("%s") is unreliable because it ignores the time zone information and assumes the time zone of the system it's running on
        # It's safer to use the timestamp() method than the %s directive
        # See https://stackoverflow.com/a/4974930
        if format == "%s":
            return str(int(dt.timestamp()))
        if format == "%s_as_float":
            return str(float(dt.timestamp()))
        if format == "%ms":
            # timstamp() returns a float representing the number of seconds since the unix epoch
            return str(int(dt.timestamp() * 1000))
        else:
            return dt.strftime(format)

    def _is_naive(self, dt: datetime.datetime) -> bool:
        return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None

    def isoformat(self, dt: datetime.datetime) -> str:
        """
        Convert a datetime object to an ISO 8601 formatted string.

        :param dt: (datetime.datetime) The datetime object to format.

        :return: str: The ISO 8601 formatted string representation of the datetime.
        """
        return dt.isoformat()
