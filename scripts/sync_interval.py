from __future__ import print_function

import argparse
from datetime import datetime, timedelta

from datadog import api

from utils import get_config, init_datadog


def _get_args():
    parser = argparse.ArgumentParser(description='Print breakdown of Sync Intervals')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')

    return parser.parse_args()


CATEGORIES = ('initial', 'lt_002d', 'lt_007d', 'lt_014d', 'lt_028d', 'over_028d')
ENVS = ('icds', 'enikshay', 'production')

def print_requests():
    end = datetime.utcnow()
    start = end - timedelta(days=7)

    data = {c: {} for c in CATEGORIES}
    for env in ENVS:
        query = ("100 * sum:commcare.restore.sync_interval{environment:%s} by {days_since_last}.as_count() "
                 "/ sum:commcare.restore.sync_interval{environment:%s}.as_count()" % (env, env))

        results = api.Metric.query(start=start.strftime('%s'), end=end.strftime('%s'), query=query)
        for series in results['series']:
            scope = [s for s in series['scope'].split(',') if s.startswith('days_since_last')][0]
            scope = scope.split(':')[1]
            vals = [v[1] for v in series['pointlist'] if v[1] is not None]
            avg = sum(vals) / len(vals)
            data[scope][env] = avg

    print("category,{},{},{}".format(*ENVS))
    for cat in CATEGORIES:
        vals = data[cat]
        print("{},{},{},{}".format(cat, *[vals[env] for env in ENVS]))


if __name__ == "__main__":

    args = _get_args()
    config = get_config(args.config)
    init_datadog(config)

    print_requests()
