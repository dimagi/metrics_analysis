from __future__ import print_function
from __future__ import division
import argparse
import json
from collections import defaultdict, namedtuple

from memoized import memoized

from utils import get_config, init_datadog

DATADOG_ENVS = [
    'icds',
    'pna',
    'production',
    'softlayer',
    'staging',
    'swiss',
]

def _get_args():
    parser = argparse.ArgumentParser(description='Print machine sizes for cluster.')
    parser.add_argument('env_name', choices=DATADOG_ENVS, help='Environment to query.')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')

    return parser.parse_args()


def kb_to_gb(string):
    assert string.endswith('kB')
    in_mb = int(string[:-2]) / 1024
    in_gb = in_mb / 1000
    return '{:.0f}'.format(in_gb)


HostStats = namedtuple('HostStats', 'name, memory, swap, cpu_logical_processors, disk, root_disk')


@memoized
def get_host_stats(env_name):
    from datadog import api
    import requests
    s = requests.session()

    s.params = {
        'api_key': api._api_key,
        'application_key': api._application_key,
        'tags': 'environment:{}'.format(env_name),
        'with_meta': True,
    }
    infra_link = 'https://app.datadoghq.com/reports/v2/overview'
    infra_content = s.request(
        method='GET', url=infra_link, params=s.params
    ).json()
    host_stats_list = []
    for host in infra_content['rows']:
        specs = json.loads(host['meta']['gohai'])
        memory = kb_to_gb(specs['memory']['total'])
        swap = kb_to_gb(specs['memory']['swap_total'])
        cpu_logical_processors = int(specs['cpu']['cpu_logical_processors'])
        disk = kb_to_gb('{}kB'.format(sum(int(drive['kb_size'])
                                          for drive in specs['filesystem']
                                          if drive['name'].startswith('/opt/data'))))
        root_disk = kb_to_gb('{}kB'.format( sum(int(drive['kb_size']) for drive in specs['filesystem'] 
                    if drive['mounted_on'] == '/' )))
        host_stats_list.append(HostStats(
            name=host['host_name'],
            memory=memory,
            swap=swap,
            cpu_logical_processors=cpu_logical_processors,
            disk=disk,
            root_disk=root_disk
        ))
    host_stats_list.sort(key=lambda host_stats: host_stats.name)
    return host_stats_list


def get_host_usage_stats(env_name):
    from datadog import api
    import time

    now = int(time.time())
    usage_stats_by_host = defaultdict(dict)
    host_stats_by_host = {host_stats.name: host_stats for host_stats in get_host_stats(env_name)}

    def get_pointlist_by_host(query_result):
        pointlist_by_host = defaultdict(dict)
        for by_host in query_result['series']:
            scope = dict(tag.split(':') for tag in by_host['scope'].split(','))
            pointlist = by_host['pointlist']
            pointlist_by_host[scope['host']] = pointlist
        return {host: pointlist for host, pointlist in pointlist_by_host.items()
                # make sure the host is still around
                if host in host_stats_by_host}

    def add_highest_cpu_in_last_week():
        """
        CPU expressed as proportion of total used. E.g. 0.25 means 25% used
        """
        query = 'system.cpu.idle{environment:%s}by{host}' % env_name
        cpu_stats = get_pointlist_by_host(api.Metric.query(start=now - 604800, end=now, query=query))
        for host, pointlist in cpu_stats.items():
            cpu_proportion = (1 - min(value for _, value in pointlist if value is not None) / 100)
            cpu_total = host_stats_by_host[host].cpu_logical_processors
            usage_stats_by_host[host]['cpu_logical_processors'] = cpu_total * cpu_proportion

    def add_highest_mem_in_last_week():
        query = 'system.mem.used{environment:%s}by{host}' % env_name
        mem_stats = get_pointlist_by_host(api.Metric.query(start=now - 604800, end=now, query=query))
        for host, pointlist in mem_stats.items():
            usage_stats_by_host[host]['memory'] = max(value for _, value in pointlist) / 1024 ** 3

    def add_highest_swap_in_last_week():
        query = 'system.swap.used{environment:%s}by{host}' % env_name
        swap_stats = get_pointlist_by_host(api.Metric.query(start=now - 604800, end=now, query=query))
        for host, pointlist in swap_stats.items():
            usage_stats_by_host[host]['swap'] = max(value for _, value in pointlist) / 1024 ** 3

    def add_highest_disk_in_last_week():
        for host in host_stats_by_host:
            usage_stats_by_host[host]['disk'] = 0
        for opt_data in ['/opt/data', '/opt/data/ecrypt']:
            query = 'sum:system.disk.used{environment:%s,device:%s}by{host}' % (env_name, opt_data)
            disk_stats = get_pointlist_by_host(api.Metric.query(start=now - 604800, end=now, query=query))
            for host, pointlist in disk_stats.items():
                new_value = max(value for _, value in pointlist) / 1024 ** 3
                usage_stats_by_host[host]['disk'] = max(usage_stats_by_host[host]['disk'], new_value)

    def add_highest_root_disk_in_last_week():
        for host in host_stats_by_host:
            usage_stats_by_host[host]['root_disk'] = 0
        for root_drive in ['/dev/mapper/vg_root-lv_root']:
            query = 'sum:system.disk.used{environment:%s,device:%s}by{host}' % (env_name, root_drive)
            disk_stats = get_pointlist_by_host(api.Metric.query(start=now - 604800, end=now, query=query))
            for host, pointlist in disk_stats.items():
                new_value = max(value for _, value in pointlist) / 1024 ** 3
                usage_stats_by_host[host]['root_disk'] = max(usage_stats_by_host[host]['root_disk'], new_value)    

    add_highest_cpu_in_last_week()
    add_highest_mem_in_last_week()
    add_highest_swap_in_last_week()
    add_highest_disk_in_last_week()
    add_highest_root_disk_in_last_week()
    return sorted((HostStats(name=host, **stats) for host, stats in usage_stats_by_host.items()),
                  key=lambda host_stats: host_stats.name)


def print_hosts(env_name):
    print('{}\t{}\t{}\t{}\t{}\t{}'.format('Name', 'Memory (GB)', 'Swap (GB)', 'Logical Processors', 'Disk (GB)', 'ROOT DISK(GB)'))
    for host_stats in get_host_stats(env_name):
        print('{name}\t{memory}\t{swap}\t{cpu_logical_processors}\t{disk}\t{root_disk}'.format(**host_stats._asdict()))


def print_host_usage(env_name):
    print('{}\t{}\t{}\t{}\t{}\t{}'.format('Name', 'Memory (GB)', 'Swap (GB)', 'Logical Processors', 'Disk (GB)','ROOT DISK(GB)'))
    for host_stats in get_host_usage_stats(env_name):
        print('{name}\t{memory:.1f}\t{swap:.1f}\t{cpu_logical_processors:.1f}\t{disk:.1f}\t{root_disk:.1f}'.format(**host_stats._asdict()))


if __name__ == "__main__":
    args = _get_args()
    config = get_config(args.config)
    init_datadog(config)
    print_hosts(args.env_name)
    print_host_usage(args.env_name)
