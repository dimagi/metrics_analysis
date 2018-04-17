from __future__ import print_function

from datetime import datetime

import pytz
import yaml
from datadog import initialize


def get_month_int(month_string):
    for form in ('%b', '%B'):
        try:
            return datetime.strptime(month_string, form).month
        except:
            pass
    raise Exception('Unknown month: %s' % month_string)


def get_month(month_string):
    return datetime.strptime(month_string, '%Y-%m-%d')


def adjust_datetime_to_utc(value, from_tz):
    return from_tz.localize(value).astimezone(pytz.utc).replace(tzinfo=None)


def get_config(path):
    with open(path, 'r') as f:
        return yaml.load(f)


def init_datadog(config):
    initialize(**config['datadog'])
