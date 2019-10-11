from __future__ import print_function
from __future__ import division
import argparse
import json
from collections import defaultdict, namedtuple, OrderedDict

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
    parser.add_argument('-c', '--config', default='config.yml', help='Path to config file.')
    parser.add_argument('-d', '--days-past', type=int, default=7, help='How many days in the past to query.')

    return parser.parse_args()


def kb_to_gb(string):
    assert string.endswith('kB')
    in_mb = int(string[:-2]) / 1024
    in_gb = in_mb / 1000
    return '{:.0f}'.format(in_gb)


HostStats = namedtuple('HostStats', 'name, memory, swap, cpu_logical_processors, disk, all_disks, memory_max_usage, cpu_max_usage')

disk_ignores = [
    'none', None, 'udev', '/dev/mapper/vg_data-lv_data', '/opt/tmp',
    'tmpfs', '/dev/vda1', '/boot'
]


class Disk(object):
    def __init__(self, name, mount_point, kb_size):
        self.name = name
        self.mount_point = mount_point
        self.kb_size = kb_size

    @property
    def gb_size(self):
        return kb_to_gb('{}kB'.format(self.kb_size))

    def __repr__(self):
        return 'Disk({self.name}, {self.mount_point}, {self.kb_size})'.format(self=self)

    def __str__(self):
        return self.gb_size


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
    all_disks = set()
    for host in infra_content['rows']:
        specs = json.loads(host['meta']['gohai'])
        memory = kb_to_gb(specs['memory']['total'])
        swap = kb_to_gb(specs['memory']['swap_total'])
        cpu_logical_processors = int(specs['cpu']['cpu_logical_processors'])
        disk = kb_to_gb('{}kB'.format(sum(int(drive['kb_size'])
                                          for drive in specs['filesystem']
                                          if drive['name'].startswith('/opt/data'))))
        disks = {
            drive['mounted_on']: Disk(drive['name'], drive['mounted_on'], int(drive['kb_size']))
            for drive in specs['filesystem']
            if drive['name'] not in disk_ignores and drive['mounted_on'] not in disk_ignores
        }
        all_disks.update(set(disks))
        host_stats_list.append(HostStats(
            name=host['host_name'],
            memory=memory,
            swap=swap,
            cpu_logical_processors=cpu_logical_processors,
            disk=disk,
            all_disks=disks,
            cpu_max_usage=None,
            memory_max_usage=None
        ))
    host_stats_list.sort(key=lambda host_stats: host_stats.name)
    return host_stats_list, all_disks


def get_host_usage_stats(env_name, days_past):
    from datadog import api
    import time

    now = int(time.time())
    usage_stats_by_host = defaultdict(dict)
    host_stats, all_disks = get_host_stats(env_name)
    host_stats_by_host = {stats.name: stats for stats in host_stats}

    period = 24 * 3600 * days_past
    start = now - period

    def get_pointlist(query_result, tags=None):
        tags = tags or ['host']
        pointlist_by_host = defaultdict(dict)
        for by_host in query_result['series']:
            scope = {}
            for tag in by_host['scope'].split(','):
                split = tag.split(':')
                if len(split) > 2:  # device:100.71.188.44:/opt/shared_icds
                    scope[split[0]] = split[-1]
                else:
                    scope[split[0]] = split[1]
            pointlist = by_host['pointlist']
            context = pointlist_by_host
            for tag in tags[:-1]:
                key = scope[tag]
                if key not in context:
                    context[key] = {}
                context = context[key]
            context[scope[tags[-1]]] = pointlist
        return pointlist_by_host

    def add_highest_cpu_in_last_week():
        """
        CPU expressed as proportion of total used. E.g. 0.25 means 25% used
        """
        query = 'min:system.cpu.idle{environment:%s}by{host}.rollup(min, 86400)' % env_name
        cpu_stats = get_pointlist(api.Metric.query(start=start, end=now, query=query))
        for host, pointlist in cpu_stats.items():
            cpu_proportion = (1 - min(value for _, value in pointlist if value is not None) / 100)
            cpu_total = host_stats_by_host[host].cpu_logical_processors
            usage_stats_by_host[host]['cpu_logical_processors'] = cpu_total * cpu_proportion
            usage_stats_by_host[host]['cpu_max_usage'] = cpu_proportion * 100

    def add_highest_mem_in_last_week():
        query = 'max:system.mem.used{environment:%s}by{host}.rollup(max, 86400)' % env_name
        mem_stats = get_pointlist(api.Metric.query(start=start, end=now, query=query))
        for host, pointlist in mem_stats.items():
            memory_total = host_stats_by_host[host].memory
            max_usage = max(value for _, value in pointlist) / 1024 ** 3
            usage_stats_by_host[host]['memory'] = max_usage
            usage_stats_by_host[host]['memory_max_usage'] = 100 * max_usage / int(memory_total)

    def add_highest_swap_in_last_week():
        query = 'max:system.swap.used{environment:%s}by{host}.rollup(max, 86400)' % env_name
        swap_stats = get_pointlist(api.Metric.query(start=start, end=now, query=query))
        for host, pointlist in swap_stats.items():
            usage_stats_by_host[host]['swap'] = max(value for _, value in pointlist) / 1024 ** 3

    def add_highest_disk_in_last_week():
        for host in host_stats_by_host:
            usage_stats_by_host[host]['disk'] = 0
            usage_stats_by_host[host]['all_disks'] = defaultdict(int)

        query = 'max:system.disk.in_use{environment:%s}by{host,device}.rollup(max, 86400)' % (env_name)
        disk_stats = get_pointlist(api.Metric.query(start=start, end=now, query=query), tags=['host', 'device'])
        for host, by_device in disk_stats.items():
            for device, pointlist in by_device.items():
                new_value = max(value for _, value in pointlist) * 100
                if device in ('/opt/data', '/opt/data/ecrypt', '/opt_new', '/opt/data1'):
                    usage_stats_by_host[host]['disk'] = max(usage_stats_by_host[host]['disk'], new_value)
                usage_stats_by_host[host]['all_disks'][device] = max(usage_stats_by_host[host]['all_disks'][device], new_value)

    add_highest_cpu_in_last_week()
    add_highest_mem_in_last_week()
    add_highest_swap_in_last_week()
    add_highest_disk_in_last_week()
    return sorted((HostStats(name=host, **stats) for host, stats in usage_stats_by_host.items()),
                  key=lambda host_stats: host_stats.name)


def print_hosts(env_name):
    stats, all_disks = get_host_stats(env_name)
    all_disks = sorted(all_disks)
    print('{},{},{},{},{}'.format('Name', 'Memory (GB)', 'Swap (GB)', 'Logical Processors', ','.join(all_disks)))
    template = '{name},{memory},{swap},{cpu_logical_processors},{%s}' % '},{'.join(all_disks)
    for host_stats in stats:
        asdict = host_stats._asdict()
        disks = asdict.pop('all_disks')
        disks = OrderedDict(sorted(disks.items()))
        for d in all_disks:
            disks.setdefault(d, '')
        asdict.update(disks)
        print(template.format(**asdict))


def print_host_usage(env_name, days_past):
    stats, all_disks = get_host_stats(env_name)
    fixed_headers = ','.join([
        'Name', 'Memory (GB)', 'Swap (GB)', 'Logical Processors',
        'Max Memory Usage (%)', 'Max CPU Usage (%)', 'Max Disk Usage (%)'
    ])
    print('{},{}'.format(fixed_headers, ','.join(all_disks)))
    template = '{name},{memory:.1f},{swap:.1f},{cpu_logical_processors:.1f},{memory_max_usage:.1f},{cpu_max_usage:.1f},{disk_max:.1f},{%s:.1f}' % ':.1f},{'.join(all_disks)
    for host_stats in get_host_usage_stats(env_name, days_past):
        asdict = host_stats._asdict()
        disks = asdict.pop('all_disks')
        disks = OrderedDict(sorted(disks.items()))
        for d in all_disks:
            disks.setdefault(d, 0)
        asdict.update(disks)
        asdict['disk_max'] = max(disks.values())
        print(template.format(**asdict))


if __name__ == "__main__":
    args = _get_args()
    config = get_config(args.config)
    init_datadog(config)
    print_hosts(args.env_name)
    print_host_usage(args.env_name, args.days_past)
