import argparse
from collections import defaultdict
from datetime import timedelta

import pytz
from datadog import api

from icds_success import format_epoch
from utils import arg_date_type
from utils import get_pointlist_by_host, get_config, init_datadog


def export_metric(query, start_date, end_date):
    hosts = set()
    by_date = defaultdict(dict)
    while start_date < end_date:
        end_day = start_date + timedelta(days=1)

        print(f"Collecting data for day {start_date.date()}")
        mem_stats = get_pointlist_by_host(
            api.Metric.query(start=start_date.strftime('%s'), end=end_day.strftime('%s'), query=query))
        hosts |= set(mem_stats)
        tz = pytz.timezone('Asia/Kolkata')
        for host, pointlist in mem_stats.items():
            for ts, value in pointlist:
                date = format_epoch(ts, tz, '%Y-%m-%d %H:%M')
                by_date[date][host] = str(value / 1024 ** 3)

        start_date += timedelta(days=1)

    hosts = sorted(list(hosts))
    print(','.join(['date'] + hosts))
    for date, host_data in by_date.items():
        row = [date] + [host_data[host] for host in hosts]
        print(','.join(row))


def _get_args():
    parser = argparse.ArgumentParser(description='Print CSV data from by host query')
    parser.add_argument(
        'query',
        help='Datadog query string. e.g. "max:system.mem.used{environment:icds}by{host}.rollup(max, 3600)',
        required=True
    )
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    parser.add_argument('--start-date', type=arg_date_type, help='Start Date', required=True)
    parser.add_argument('--end-date', type=arg_date_type, help='End Date', required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = _get_args()
    config = get_config(args.config)
    init_datadog(config)
    export_metric(args.query, args.start_date, args.end_date)