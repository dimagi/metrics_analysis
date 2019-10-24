# This script outpus the ICDS success metrics:
#     - Peak Performance (15 Minute Max) Form Processing Volume
#     - Peak Performance (15 Minute Max) Phone Sync Volume

from __future__ import absolute_import

import argparse
from datetime import datetime, timedelta
from itertools import izip_longest

import pytz
from datadog import api
from dateutil.relativedelta import relativedelta

from const import ENV_TZ
from utils import get_date, get_config, init_datadog, adjust_datetime_to_utc

INTERVAL_SEC = 15 * 60

MAX_INTERVAL_METRICS = {
    'Form Processing Volume': "sum:nginx.requests{{environment:{env},url_group:receiver,status_code:201}}.as_count().rollup(sum, {rollup})",
    'Phone Sync Volume': "sum:nginx.requests{{environment:{env},url_group:phone/restore,status_code:200}}.as_count().rollup(sum, {rollup})",
}


def _get_max(results):
    pointslist = results['series'][0]['pointlist']
    return sorted(pointslist, key=lambda p: p[1])[-1]


def _get_avg(results):
    pointslist = results['series'][0]['pointlist']
    return None, sum([p[1] for p in pointslist]) / len(pointslist)


def _get_top(results):
    [value] = results['series'][0]['attributes']['top']['value']
    return None, int(value)


METRICS = {
    'Total forms submissions': (
        "top(cumsum(sum:commcare.xform_submissions.count{{environment:{env},!submission_type:device-log}}.as_count()), 1, 'last', 'desc')",
        _get_top
    ),
    'Max Form submissions per day': (
        "sum:nginx.requests{{environment:{env},url_group:receiver,status_code:201}}.as_count().rollup(sum, 86400)",
        _get_max
    ),
    'Max Restores submissions per day': (
        "sum:nginx.requests{{environment:{env},url_group:phone/restore,status_code:200}}.as_count().rollup(sum, 86400)",
        _get_max
    ),
    'Avg Success %': (
        "("
        "  sum:nginx.requests{{environment:{env},status_code:200}}.as_count().rollup(sum, 86400)"
        "+ sum:nginx.requests{{environment:{env},status_code:201}}.as_count().rollup(sum, 86400)"
        "+ sum:nginx.requests{{environment:{env},status_code:202}}.as_count().rollup(sum, 86400)"
        "+ sum:nginx.requests{{environment:{env},status_code:301}}.as_count().rollup(sum, 86400)"
        "+ sum:nginx.requests{{environment:{env},status_code:302}}.as_count().rollup(sum, 86400)"
        "+ sum:nginx.requests{{environment:{env},status_code:412}}.as_count().rollup(sum, 86400)"
        ")*100/sum:nginx.requests{{environment:{env}}}.as_count().rollup(sum, 86400)",
        _get_avg
    ),
}

def _get_args():
    parser = argparse.ArgumentParser(description='Print basic request success data')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    parser.add_argument('--start-date', help='Start Date. Defaults to first day of last month')
    parser.add_argument('--end-date', help='End Date. Defaults to last day of last month')
    parser.add_argument('--show-data', action='store_true', help='Show all data.')

    return parser.parse_args()


def print_requests(start_date, end_date, show_data=False):
    timezone = ENV_TZ['icds']
    start_utc = adjust_datetime_to_utc(start_date, timezone)

    end_utc = adjust_datetime_to_utc(end_date, timezone)
    print('Reporting for period: {} to {}'.format(start_utc, end_utc))

    for metric, (query, extractor) in METRICS.items():
        query = query.format(env='icds')
        results = api.Metric.query(start=start_utc.strftime('%s'), end=end_utc.strftime('%s'), query=query)
        date, value = extractor(results)
        date_output = ' on {}'.format(format_epoch(date, timezone)) if date else ''
        print('\n{}: {:.0f}{}'.format(metric, value, date_output))

    for metric in MAX_INTERVAL_METRICS:
        if show_data:
            print('\n{}'.format(metric))
            print('{}\n'.format('=' * len(metric)))

        query = MAX_INTERVAL_METRICS[metric]
        query = query.format(env='icds', rollup=INTERVAL_SEC)

        daily_data = []
        start_day = start_utc
        while start_day < end_utc:
            end_day = start_day + timedelta(days=1)

            results = api.Metric.query(start=start_day.strftime('%s'), end=end_day.strftime('%s'), query=query)
            series = results['series']
            if not series:
                start_day += timedelta(days=1)
                continue

            daily_data.append([
                (point[0], int(point[1]) if point[1] is not None else 0)
                for point in series[0]['pointlist']
            ])

            interval_start = daily_data[-1][0][0]
            interval_end = daily_data[-1][1][0]
            interval = interval_end - interval_start
            if interval != INTERVAL_SEC * 1000:
                print('[WARNING] data interval different from requested: {}'.format(interval / 1000))

            start_day += timedelta(days=1)

        if show_data:
            heads = ['#'] + [
                '{}'.format(format_epoch(day_points[0][0], timezone)) for day_points in daily_data
            ]
            print(','.join(heads))
            for row_count, row in enumerate(izip_longest(*daily_data)):
                print(','.join([str(row_count)] + [
                    str(pair[1]) if pair else ''
                    for pair in row
                ]))

        daily_max = []
        for day in daily_data:
            max_item = sorted(day, key=lambda x: x[1])[-1]
            daily_max.append(max_item)

        if show_data:
            print('\nDaily Max:')
            for day, val_max in daily_max:
                print('   {}: {}'.format(
                    format_epoch(day, timezone, '%Y-%m-%d %H:%M'),
                    val_max
                ))

        max_item = sorted(daily_max, key=lambda x: x[1])[-1]

        date = format_epoch(max_item[0], timezone, '%Y-%m-%d %H:%M')
        print('\nPeak Performance (15 Minute Max) {}: {} on {}'.format(metric, max_item[1], date))


def format_epoch(day, timezone, format='%Y-%m-%d'):
    return from_utc_to_tz(datetime.fromtimestamp(day / 1000), timezone).strftime(format)


def from_utc_to_tz(date, tz):
    return pytz.utc.localize(date).astimezone(tz).replace(tzinfo=None)


if __name__ == "__main__":

    args = _get_args()

    now = datetime.utcnow()

    if args.start_date:
        start = get_date(args.start_date)
    else:
        start = datetime(now.year, now.month, 1) - relativedelta(months=1)

    if args.end_date:
        end = get_date(args.end_date)
    else:
        end = datetime(now.year, now.month, 1) - relativedelta(seconds=1)


    config = get_config(args.config)
    init_datadog(config)
    print_requests(start, end, args.show_data)
