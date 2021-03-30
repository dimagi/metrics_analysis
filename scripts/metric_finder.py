import argparse
import csv
import os
import re
import sys

from datadog import api

from utils import get_config, init_datadog


def _get_args():
    parser = argparse.ArgumentParser(description='Location all usages of metric')
    parser.add_argument('metrics', nargs='+', help='Metric to search for, can supply multiple')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    parser.add_argument('--cache', help='Use this file to cache results')
    return parser.parse_args()


def _get_query(request):
    return request.get('q', request.get('query'))


def _check_query(metrics, query, location):
    for metric in metrics:
        if metric.findall(query):
            print("{}\n\tquery = '{}'\n".format(location, query))


class CacheBase:
    mode = None
    def __init__(self, path):
        self.path = path
        self.file = None

    def __enter__(self):
        if self.path:
            self.file = open(self.path, self.mode)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()


class CacheWriter(CacheBase):
    mode = "w"
    def __enter__(self):
        super().__enter__()
        if self.file:
            self.writer = csv.writer(self.file)
        return self

    def write(self, q, location):
        if self.file:
            self.writer.writerow([q, location])


class CacheReader(CacheBase):
    mode = "r"

    def __enter__(self):
        super().__enter__()
        if self.file:
            self.reader = csv.reader(self.file)
        return self

    def read(self):
        return self.reader


if __name__ == "__main__":
    args = _get_args()
    config = get_config(args.config)
    init_datadog(config)

    metrics = [re.compile(pattern) for pattern in args.metrics]


    if args.cache and os.path.isfile(args.cache):
        with CacheReader(args.cache) as cache:
            for q, location in cache.read():
                _check_query(metrics, q, location)
        sys.exit()

    with CacheWriter(args.cache) as cache:
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
                            q = _get_query(req)
                            cache.write(q, location)
                            _check_query(metrics, q, location)
                elif isinstance(requests, dict):
                    try:
                        q = _get_query(requests['fill'])
                        cache.write(q, location)
                        _check_query(metrics, q, location)
                    except KeyError:
                        pass

                    try:
                        q = _get_query(requests['size'])
                        cache.write(q, location)
                        _check_query(metrics, q, location)
                    except KeyError:
                        pass

        monitors = api.Monitor.get_all()
        for monitor in monitors:
            location = "Monitor: {}".format(monitor['name'])
            q = _get_query(monitor)
            cache.write(q, location)
            _check_query(metrics, q, location)
