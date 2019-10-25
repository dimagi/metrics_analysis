from __future__ import print_function

import argparse
from datetime import datetime, timedelta

from datadog import api
from dateutil.relativedelta import relativedelta

from const import ENV_TZ
from utils import get_date, get_config, init_datadog, adjust_datetime_to_utc

ENV_DISKS = {
    'icds': ['/opt/data', '/opt/data1', '/opt_new'],
}


def _get_env(series):
    envs = [
        scope.split(':')[1]
        for scope in series['scope'].split(',')
        if scope.startswith('environment')
    ]
    return envs[0] if envs else None


def _get_args():
    parser = argparse.ArgumentParser(description='Print total requests per month for the given environment.')
    parser.add_argument('-e', '--env', nargs='+', choices=ENV_TZ.keys(), required=True, help='Environments to query.')
    parser.add_argument('-s', '--month-start', required=True, help='Month to start e.g. Feb or February')
    parser.add_argument('-f', '--month-end', required=True, help='Month to end e.g. Sep or September')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')

    return parser.parse_args()


def print_requests(envs, month_start, month_end):
    query = []
    for env in envs:
        env_query = []
        devices = ENV_DISKS.get(env, ['/opt/data'])
        for device in devices:
            env_query.append("sum:system.disk.used{{environment:{},device:{}}}.rollup(avg, 2592000)".format(env, device))
        query.append(' + '.join(env_query))

    results = api.Metric.query(start=month_start.strftime('%s'), end=month_end.strftime('%s'), query=','.join(query))
    data = results['series'][0].get('pointlist', [])
    senvs = [
        _get_env(series)
        for series in results['series']
    ]
    headers = ['Month'] + ['Avg data on "{}" (TB)'.format(env) for env in senvs]
    print(','.join(headers))
    for i in range(0, len(data)):
        posix_time = data[i][0]
        datespan = datetime.utcfromtimestamp(posix_time / 1000).date()  # python datetime POSIX TZ issue
        print(", ".join([str(datespan)] + [
            '{}'.format(int(series['pointlist'][i][1]) / 1000000000000 if series['pointlist'][i][1] is not None else '---')
            for series in results['series']
        ]))


if __name__ == "__main__":

    args = _get_args()

    month_start = get_date(args.month_start)
    month_end = get_date(args.month_end)

    config = get_config(args.config)
    init_datadog(config)

    print_requests(args.env, month_start, month_end)
