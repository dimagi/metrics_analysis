import argparse
import json
import sys
from collections import defaultdict
from datetime import timedelta

import pytz
from datadog import api

from icds_success import format_epoch
from utils import arg_date_type
from utils import get_pointlist_by_host, get_config, init_datadog


def _get_args():
    parser = argparse.ArgumentParser(description='Location all usages of metric')
    parser.add_argument('metric', help='Metric to search for')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    return parser.parse_args()


def _check_query(metric, request, location):
    query = request.get('q', request.get('query'))
    if metric in query:
        print("{}\n\tquery = '{}'\n".format(location, query))


if __name__ == "__main__":
    args = _get_args()
    config = get_config(args.config)
    init_datadog(config)
    metric = args.metric
    dashboards = api.Dashboard.get_all()
    for dashboard_info in dashboards['dashboards']:
        dashboard = api.Dashboard.get(dashboard_info['id'])
        for widget in dashboard['widgets']:
            widget = widget['definition']
            location = "Dashboard: '{}', Widget: '{}'".format(dashboard['title'], widget.get('title', ''))
            requests = widget.get('requests')
            if not requests:
                continue
            if isinstance(requests, list):
                for req in requests:
                    if 'q' in req:
                        _check_query(metric, req, location)
            elif isinstance(requests, dict):
                try:
                    _check_query(metric, requests['fill'], location)
                except KeyError:
                    pass

                try:
                    _check_query(metric, requests['size'], location)
                except KeyError:
                    pass

    monitors = api.Monitor.get_all()
    for monitor in monitors:
        location = "Monitor: {}".format(monitor['name'])
        _check_query(metric, monitor, location)