from __future__ import absolute_import

import argparse
from datetime import datetime

from datadog import api

from const import ENV_TZ
from utils import get_date, get_config, init_datadog, adjust_datetime_to_utc

METRICS = (
    ('request_count', "sum:nginx.requests{{environment:{env}}}.as_count().rollup(sum, {rollup})"),
    ('request_success', "sum:nginx.requests{{environment:{env},!status_code:503}}.as_count().rollup(sum, {rollup})"),
    ('submission_count', "sum:nginx.requests{{environment:{env},url_group:receiver}}.as_count().rollup(sum, {rollup})"),
    ('submission_count_success', "sum:nginx.requests{{environment:{env},url_group:receiver,status_code:201}}.as_count().rollup(sum, {rollup})"),
    ('restores_count', "sum:nginx.requests{{environment:{env},url_group:phone}}.rollup(sum, {rollup})"),
    ('restores_count_success', """
        sum:nginx.requests{{environment:{env},status_code:412,url_group:phone}}.rollup(sum, {rollup})
        + sum:nginx.requests{{environment:{env},status_code:200,url_group:phone}}.rollup(sum, {rollup})
        + sum:nginx.requests{{environment:{env},status_code:202,url_group:phone}}.rollup(sum, {rollup})
        + sum:nginx.requests{{environment:{env},status_code:401,url_group:phone}}.rollup(sum, {rollup})
    """),
)

INTERVALS = {
    # Even though we can set granularity, datadog has limit on max points it can return in a go
    # see https://help.datadoghq.com/hc/en-us/articles/204526615-What-is-the-rollup-function-
    'hourly': 60 * 60,
    'daily': 60 * 60 * 24,
    '15min': 15 * 60,
}

def _get_args():
    parser = argparse.ArgumentParser(description='Print basic request success data')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    parser.add_argument('-e', '--env', choices=ENV_TZ.keys(), required=True, help='Environment to query.')
    parser.add_argument('-s', '--start-date', required=True, help='Start date e.g. 2018-01-23')
    parser.add_argument('-f', '--end-date', required=True, help='End date e.g. 2018-01-23')
    parser.add_argument('-i', '--interval', default='daily', choices=list(INTERVALS))

    return parser.parse_args()


def print_requests(env, start, end, timezone, interval):
    title = "Data for '%s' using timezone: '%s'" % (args.env.upper(), timezone)
    print(title)
    print("=" * len(title))
    print(','.join(['Month'] + [m[0] for m in METRICS]))

    start_utc = adjust_datetime_to_utc(start, timezone)
    end_utc = adjust_datetime_to_utc(end, timezone)

    query = ', '.join([m[1] for m in METRICS])
    query = query.format(env=env, rollup=INTERVALS[interval])

    results = api.Metric.query(start=start_utc.strftime('%s'), end=end_utc.strftime('%s'), query=query)
    data = results['series'][0].get('pointlist', [])
    for i in range(0, len(data)):
        posix_time = data[i][0]
        datespan = datetime.utcfromtimestamp(posix_time / 1000).date()  # python datetime POSIX TZ issue
        print ", ".join([str(datespan)] + [
            '{}'.format(int(series['pointlist'][i][1]) if series['pointlist'][i][1] is not None else '---')
            for series in results['series']
        ])


if __name__ == "__main__":

    args = _get_args()

    month_start = get_date(args.start_date)
    month_end = get_date(args.end_date)

    config = get_config(args.config)
    init_datadog(config)

    print_requests(args.env, month_start, month_end, ENV_TZ[args.env], args.interval)
