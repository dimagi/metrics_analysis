# This script outputs daily data for different request groups. This allows us to examine the daily
# request profile for usage characterization.
from __future__ import absolute_import

import argparse
from datetime import datetime, timedelta
from itertools import izip_longest

import pytz
from datadog import api

from const import ENV_TZ
from utils import get_date, get_config, init_datadog, adjust_datetime_to_utc

METRICS = {
    'all_requests': "sum:nginx.requests{{environment:{env}}}.as_count().rollup(sum, {rollup})",
    'form_submissions': "sum:nginx.requests{{environment:{env},url_group:receiver}}.as_count().rollup(sum, {rollup})",
    'restores': "sum:nginx.requests{{environment:{env},url_group:phone/restore}}.as_count().rollup(sum, {rollup})",
    'heartbeat': "sum:nginx.requests{{environment:{env},url_group:phone/heartbeat}}.as_count().rollup(sum, {rollup})",
    'apps': "sum:nginx.requests{{environment:{env},url_group:apps}}.as_count().rollup(sum, {rollup})",
    'django_requests': """
        sum:nginx.requests{{environment:{env}}}.as_count().rollup(sum, {rollup})
        - sum:nginx.requests{{environment:{env},url_group:mm/audio}}.as_count().rollup(sum, {rollup})
        - sum:nginx.requests{{environment:{env},url_group:mm/video}}.as_count().rollup(sum, {rollup})
        - sum:nginx.requests{{environment:{env},url_group:mm/image}}.as_count().rollup(sum, {rollup})
    """

}

INTERVALS = {
    # Even though we can set granularity, datadog has limit on max points it can return in a go
    # see https://help.datadoghq.com/hc/en-us/articles/204526615-What-is-the-rollup-function-
    'hourly': 60 * 60,
    '15min': 15 * 60,
}

def _get_args():
    parser = argparse.ArgumentParser(description='Print basic request success data')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    parser.add_argument('--metric', choices=sorted(METRICS.keys()))
    parser.add_argument('-e', '--env', choices=ENV_TZ.keys(), required=True, help='Environment to query.')
    parser.add_argument('-d', '--duration', required=True, help='How many days to export')
    parser.add_argument('-i', '--interval', default='hourly', choices=list(INTERVALS))

    return parser.parse_args()


def print_requests(env, metric, start, timezone, interval):
    title = "Data for '%s' using timezone: '%s'" % (args.env.upper(), timezone)
    print(title)
    print("=" * len(title))

    start_utc = adjust_datetime_to_utc(start, timezone)

    query = METRICS[metric]
    query = query.format(env=env, rollup=INTERVALS[interval])
    print(query)

    data = []
    start_day = start_utc
    while start_day < adjust_datetime_to_utc(datetime.today() - timedelta(days=1) , timezone):
        end_day = start_day + timedelta(days=1)
        if end_day.weekday() == 6:
            start_day += timedelta(days=1)
            continue

        results = api.Metric.query(start=start_day.strftime('%s'), end=end_day.strftime('%s'), query=query)
        series = results['series']
        if not series:
            start_day += timedelta(days=1)
            continue

        data.append([
            (point[0], int(point[1]) if point[1] is not None else 0)
            for point in series[0]['pointlist']
        ])

        start_day += timedelta(days=1)

    heads = ['#'] + [
        '{}'.format(from_utc_to_tz(datetime.fromtimestamp(day_points[0][0] / 1000), timezone).strftime('%Y-%m-%d')) for day_points in data
    ]
    print(','.join(heads))
    for row_count, row in enumerate(izip_longest(*data)):
        print(','.join([str(row_count)] + [
            str(pair[1]) if pair else ''
            for pair in row
        ]))

    for day in data:
        vals = [r[1] for r in day]
        val_max = max(vals)
        val_sum = sum(vals)
        print(from_utc_to_tz(datetime.fromtimestamp(day[0][0] / 1000), timezone).strftime('%Y-%m-%d'), float(val_max)/float(val_sum))


def from_utc_to_tz(date, tz):
    return pytz.utc.localize(date).astimezone(tz).replace(tzinfo=None)


if __name__ == "__main__":

    args = _get_args()

    start = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=int(args.duration))

    config = get_config(args.config)
    init_datadog(config)

    print_requests(args.env, args.metric, start, ENV_TZ[args.env], args.interval)
