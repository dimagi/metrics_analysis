from __future__ import print_function
from __future__ import division
import argparse
import json
from collections import defaultdict, namedtuple, OrderedDict
from datetime import datetime
from memoized import memoized
import time
import sys

from scripts.const import DATADOG_ENVS
from utils import get_config, init_datadog, get_pointlist_by_host


def _get_args():
    parser = argparse.ArgumentParser(description='Print machine sizes for cluster.')
    parser.add_argument('--env-name', choices=DATADOG_ENVS, help='Environment to query.', required=True)
    parser.add_argument('-c', '--config', default='config.yml', help='Path to config file.', required=True)
    parser.add_argument('-d', '--days-past', type=int, help='How many days in the past to query.')
    parser.add_argument('--fixed-date', type=lambda d: datetime.strptime(d, '%Y-%m-%d') , help='Particular Date for which to query <YYYY-MM-DD>')

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


def get_host_usage_stats(env_name, days_past, fixed_date):
    from datadog import api
 
    def datetime_timestamp(days_past, fixed_date):
        if days_past:
            end_time = int(time.time())
            period = 24 * 3600 * days_past
            start_time = end_time - period
        if fixed_date:
            start_time= time.mktime(fixed_date.timetuple())
            end_time = start_time + 24 * 60 * 60 - 1 
        return start_time,end_time    
    
    start_time,end_time = datetime_timestamp(days_past, fixed_date)

    usage_stats_by_host = defaultdict(dict)
    host_stats, all_disks = get_host_stats(env_name)
    host_stats_by_host = {stats.name: stats for stats in host_stats}
    
    
      
    def add_highest_cpu_in_last_week():
        """
        CPU expressed as proportion of total used. E.g. 0.25 means 25% used
        """
        query = 'min:system.cpu.idle{environment:%s}by{host}.rollup(min, 86400)' % env_name
        cpu_stats = get_pointlist_by_host(api.Metric.query(start=start_time, end=end_time, query=query))
        for host, pointlist in cpu_stats.items():
            try:
                cpu_proportion = (1 - min(value for _, value in pointlist if value is not None) / 100)
                cpu_total = host_stats_by_host[host].cpu_logical_processors
                usage_stats_by_host[host]['cpu_logical_processors'] = cpu_total * cpu_proportion
                usage_stats_by_host[host]['cpu_max_usage'] = cpu_proportion * 100
            except KeyError:
                usage_stats_by_host[host]['cpu_logical_processors'] = 'NA'
                usage_stats_by_host[host]['cpu_max_usage'] = 'NA'

    def add_highest_mem_in_last_week():
        query = 'max:system.mem.used{environment:%s}by{host}.rollup(max, 86400)' % env_name
        mem_stats = get_pointlist_by_host(api.Metric.query(start=start_time, end=end_time, query=query))
        for host, pointlist in mem_stats.items():
            try:
                memory_total = host_stats_by_host[host].memory
                max_usage = max(value for _, value in pointlist) / 1024 ** 3
                usage_stats_by_host[host]['memory'] = max_usage
                usage_stats_by_host[host]['memory_max_usage'] = 100 * max_usage / int(memory_total)
            except KeyError:
                usage_stats_by_host[host]['memory'] = 'NA'
                usage_stats_by_host[host]['memory_max_usage'] = 'NA'

    def add_highest_swap_in_last_week():
        query = 'max:system.swap.used{environment:%s}by{host}.rollup(max, 86400)' % env_name
        swap_stats = get_pointlist_by_host(api.Metric.query(start=start_time, end=end_time, query=query))
        for host, pointlist in swap_stats.items():
            try:
                usage_stats_by_host[host]['swap'] = max(value for _, value in pointlist) / 1024 ** 3
            except KeyError:
                usage_stats_by_host[host]['swap'] = 'NA'

    def add_highest_disk_in_last_week():
        for host in host_stats_by_host:
                usage_stats_by_host[host]['disk'] = 0
                usage_stats_by_host[host]['all_disks'] = defaultdict(int)

        query = 'max:system.disk.in_use{environment:%s}by{host,device}.rollup(max, 86400)' % (env_name)
        disk_stats = get_pointlist_by_host(api.Metric.query(start=start_time, end=end_time, query=query), tags=['host', 'device'])
        for host, by_device in disk_stats.items():
            for device, pointlist in by_device.items():
                try:
                    new_value = max(value for _, value in pointlist) * 100
                    if device in ('/opt/data', '/opt/data/ecrusage_stats_by_hostusage_stats_by_hostypt', '/opt_new', '/opt/data1'):
                        usage_stats_by_host[host]['disk'] = max(usage_stats_by_host[host]['disk'], new_value)  

                    if not (usage_stats_by_host[host]['disk'] == 'NA'):
                        usage_stats_by_host[host]['all_disks'][device] = max(usage_stats_by_host[host]['all_disks'][device], new_value)
                except KeyError:
                    usage_stats_by_host[host]['disk'] = 'NA'
                    usage_stats_by_host[host]['all_disks'] = 'NA'

    add_highest_cpu_in_last_week()
    add_highest_mem_in_last_week()
    add_highest_swap_in_last_week()
    add_highest_disk_in_last_week()
    
    # DELETE HOSTS FROM DICT
    for key,value in usage_stats_by_host.items():
        if 'NA' in value: # WHICH DATA ARE AVAILABLE IN PAST BUT REMOVED NOW
            del usage_stats_by_host[key]
        if len(usage_stats_by_host[key]) < 7: # WHICH DATA ARE PRESENT NOW BUT NOT AVAILABLE IN PAST
            del usage_stats_by_host[key]     
               
    return sorted((HostStats(name=host, **stats)  for host, stats in usage_stats_by_host.items()),
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


def print_host_usage(env_name, days_past, date_fixed):
    stats, all_disks = get_host_stats(env_name)
    fixed_headers = ','.join([
        'Name', 'Memory (GB)', 'Swap (GB)', 'Logical Processors',
        'Max Memory Usage (%)', 'Max CPU Usage (%)', 'Max Disk Usage (%)'
    ])
    print('{},{}'.format(fixed_headers, ','.join(all_disks)))
    template = '{name},{memory:.1f},{swap:.1f},{cpu_logical_processors:.1f},{memory_max_usage:.1f},{cpu_max_usage:.1f},{disk_max:.1f},{%s:.1f}' % ':.1f},{'.join(all_disks)
    for host_stats in get_host_usage_stats(env_name, days_past, date_fixed):
        asdict = host_stats._asdict()
        disks = asdict.pop('all_disks')
        if disks != 'NA':
            disks = OrderedDict(sorted(disks.items()))
            for d in all_disks:
                disks.setdefault(d, 0)
            asdict.update(disks)
            asdict['disk_max'] = max(disks.values())
            print(template.format(**asdict))


if __name__ == "__main__":
    args = _get_args()
    if args.days_past is None and args.fixed_date is None :
       print("ERROR : Please specify either days_past or fixed_date args") 
       sys.exit(1)
    if args.days_past and args.fixed_date:
        print("ERROR : Please specify either days_past or fixed_date args")
        sys.exit(1)
    config = get_config(args.config)
    init_datadog(config)
    print_hosts(args.env_name)
    print_host_usage(args.env_name, args.days_past, args.fixed_date)
