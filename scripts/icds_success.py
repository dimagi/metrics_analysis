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

METRICS = {
    'Form Processing Volume': "sum:nginx.requests{{environment:{env},url_group:receiver,status_code:201}}.as_count().rollup(sum, {rollup})",
    'Phone Sync Volume': "sum:nginx.requests{{environment:{env},url_group:phone/restore,status_code:200}}.as_count().rollup(sum, {rollup})",
}

def _get_args():
    parser = argparse.ArgumentParser(description='Print basic request success data')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    parser.add_argument('--start-date', help='Start Date. Defaults to first day of last month')
    parser.add_argument('--end-date', help='End Date. Defaults to last day of last month')
    parser.add_argument('--show_data', action='store_true', help='Show all data.')

    return parser.parse_args()


def print_requests(start_date, end_date, show_data=False):
    timezone = ENV_TZ['icds']
    start_utc = adjust_datetime_to_utc(start_date, timezone)

    print('Reporting for period: {} to {}'.format(start_utc, adjust_datetime_to_utc(end_date, timezone)))
    for metric in METRICS:
        if show_data:
            print('\n{}'.format(metric))
            print('{}\n'.format('=' * len(metric)))

        query = METRICS[metric]
        query = query.format(env='icds', rollup=INTERVAL_SEC)

        daily_data = []
        start_day = start_utc
        while start_day < adjust_datetime_to_utc(end_date , timezone):
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
                '{}'.format(from_utc_to_tz(datetime.fromtimestamp(day_points[0][0] / 1000), timezone).strftime('%Y-%m-%d')) for day_points in daily_data
            ]
            print(','.join(heads))
            for row_count, row in enumerate(izip_longest(*daily_data)):
                print(','.join([str(row_count)] + [
                    str(pair[1]) if pair else ''
                    for pair in row
                ]))

        daily_max = []
        for day in daily_data:
            daily_max.append((day[0][0], max([r[1] for r in day])))

        if show_data:
            print('\nDaily Max:')
            for day, val_max in daily_max:
                print('   {}: {}'.format(
                    from_utc_to_tz(datetime.fromtimestamp(day / 1000), timezone).strftime('%Y-%m-%d'),
                    val_max
                ))

        print('\nPeak Performance (15 Minute Max) {}: {}'.format(metric, max([dm[1] for dm in daily_max])))


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
