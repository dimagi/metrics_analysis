from __future__ import print_function

import argparse
from collections import defaultdict
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


def get_date(month_string):
    return datetime.strptime(month_string, '%Y-%m-%d')


def adjust_datetime_to_utc(value, from_tz):
    return from_tz.localize(value).astimezone(pytz.utc).replace(tzinfo=None)


def get_config(path):
    with open(path, 'r') as f:
        return yaml.load(f)


def init_datadog(config):
    initialize(**config['datadog'])


def get_pointlist_by_host(query_result, tags=None):
    tags = tags or ['host']
    pointlist_by_host = defaultdict(dict)
    for by_host in query_result['series']:
        scope = {}
        for tag in by_host['scope'].split(','):
            split = tag.split(':')
            if len(split) > 2:  # device:100.71.188.44:/opt/shared_icds
                scope[split[0]] = split[-1]
            else:
                scope[split[0]] = split[1]
        pointlist = by_host['pointlist']
        context = pointlist_by_host
        for tag in tags[:-1]:
            key = scope[tag]
            if key not in context:
                context[key] = {}
            context = context[key]
        context[scope[tags[-1]]] = pointlist
    return pointlist_by_host


def arg_date_type(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(
            "Invalid date specified: '%s'. "
            "Expected date in the format: YYYY-MM-DD" % value
        )
