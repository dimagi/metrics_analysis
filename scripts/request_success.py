from __future__ import absolute_import

import argparse
from datetime import datetime

from datadog import api

from const import ENV_TZ
from utils import get_month, get_config, init_datadog, adjust_datetime_to_utc

METRICS = (
    ('request_count', "sum:nginx.requests{{environment:{env}}}.as_count().rollup(sum, 86400)"),
    ('request_success', "sum:nginx.requests{{environment:{env},!status_code:503}}.as_count().rollup(sum, 86400)"),
)


def _get_args():
    parser = argparse.ArgumentParser(description='Print basic request success data')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    parser.add_argument('-e', '--env', choices=ENV_TZ.keys(), required=True, help='Environment to query.')
    parser.add_argument('-s', '--month-start', required=True, help='Month to start e.g. Feb or February')
    parser.add_argument('-f', '--month-end', required=True, help='Month to end e.g. Sep or September')

    return parser.parse_args()


def print_requests(env, start, end, timezone):
    title = "Data for '%s' using timezone: '%s'" % (args.env.upper(), timezone)
    print(title)
    print("=" * len(title))
    print(','.join(['Month'] + [m[0] for m in METRICS]))

    start_utc = adjust_datetime_to_utc(start, timezone)
    end_utc = adjust_datetime_to_utc(end, timezone)

    query = ', '.join([m[1] for m in METRICS])
    results = api.Metric.query(start=start_utc.strftime('%s'), end=end_utc.strftime('%s'), query=query.format(env=env))
    data = results['series'][0].get('pointlist', [])
    for i in range(0, len(data)):
        posix_time = data[i][0]
        datespan = datetime.fromtimestamp(posix_time / 1000).date()  # python datetime POSIX TZ issue
        print ", ".join([str(datespan)] + [
            '{}'.format(int(series['pointlist'][i][1]))
            for series in results['series']
        ])


if __name__ == "__main__":

    args = _get_args()

    month_start = get_month(args.month_start)
    month_end = get_month(args.month_end)

    config = get_config(args.config)
    init_datadog(config)

    print_requests(args.env, month_start, month_end, ENV_TZ[args.env])
