from __future__ import print_function

import argparse
from datetime import datetime, timedelta

from datadog import api
from dateutil.relativedelta import relativedelta

from const import ENV_TZ
from utils import get_month_int, get_config, init_datadog, adjust_datetime_to_utc


def _get_args():
    parser = argparse.ArgumentParser(description='Print total requests per month for the given environment.')
    parser.add_argument('-e', '--env', choices=ENV_TZ.keys(), required=True, help='Environment to query.')
    parser.add_argument('-s', '--month-start', required=True, help='Month to start e.g. Feb or February')
    parser.add_argument('-f', '--month-end', required=True, help='Month to end e.g. Sep or September')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')

    return parser.parse_args()


def print_requests(env, month_start, month_end, timezone):
    title = "Data for '%s' using timezone: '%s'" % (args.env.upper(), timezone)
    print(title)
    print("=" * len(title))
    print("Month,Total Requests (approximate)")

    for month in range(month_start, month_end + 1):
        start = datetime.min.replace(year=2017, month=month, day=1)
        start_utc = adjust_datetime_to_utc(
            start, timezone
        )
        end_utc = start + relativedelta(months=1) - timedelta(seconds=1)
        query = "integral(avg:nginx.requests{environment:%s})" % args.env
        results = api.Metric.query(start=start_utc.strftime('%s'), end=end_utc.strftime('%s'), query=query)
        data = results['series'][0].get('pointlist', [])

        print("%s,%s" % (start.strftime('%b'), int(data[-1][1])))


if __name__ == "__main__":

    args = _get_args()

    month_start = get_month_int(args.month_start)
    month_end = get_month_int(args.month_end)

    config = get_config(args.config)
    init_datadog(config)

    print_requests(args.env, month_start, month_end, ENV_TZ[args.env])
