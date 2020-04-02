import argparse
import difflib
import itertools
import json
import re
from datetime import datetime

from datadog import api

from utils import get_config, init_datadog
from clint.textui import colored


class IN(object):
    def __init__(self, val, tokenize=False):
        self.tokenize = tokenize
        self.val = val

    def __call__(self, query):
        if not self.tokenize:
            return self.val in query
        tokens = re.split('[:{}]', query)
        return any(token == self.val for token in tokens)

    def __repr__(self):
        return "IN('{self.val}')".format(self=self)


class OR(object):
    def __init__(self, *children):
        self.children = children

    def __call__(self, query):
        return any(child(query) for child in self.children)

    def __repr__(self):
        return "OR({self.children})".format(self=self)


class AND(object):
    def __init__(self, *children):
        self.children = children

    def __call__(self, query):
        return all(child(query) for child in self.children)

    def __repr__(self):
        return "AND({self.children})".format(self=self)


class NOT(object):
    def __init__(self, child):
        self.child = child

    def __call__(self, query):
        return not self.child(query)

    def __repr__(self):
        return "NOT({self.child})".format(self=self)


class Rename(object):
    def __init__(self, from_val, to_val, *checkers):
        self.checker = AND(IN(from_val, tokenize=True), *checkers)
        self.from_val = from_val
        self.to_val = to_val
        self.seen = False

    def __call__(self, query):
        if self.checker(query):
            return query.replace(self.from_val, self.to_val)

    def mark_seen(self):
        self.seen = True

    def __iter__(self):
        yield self

    def __repr__(self):
        return "Change(from='{self.from_val}', to='{self.to_val}', checks={self.checker})".format(self=self)


def histogram_change(orig_name, new_name, tag_name='duration'):
    return [
        Rename(orig_name, new_name, IN('by {{{}}}'.format(tag_name))),
        Rename(orig_name, new_name, IN('{}:'.format(tag_name)), NOT(IN('by {{{}}}'.format(tag_name)))),
    ]


CHANGES = [
    histogram_change('commcare.xform_submissions.count', 'commcare.xform_submissions.lag.days', 'lag'),
    histogram_change('commcare.xform_submissions.count', 'commcare.xform_submissions.duration.seconds'),

    Rename('commcare.corrupt-multimedia-submission.error.count', 'commcare.corrupt_multimedia_submissions'),

    histogram_change('commcare.case_importer.cases', 'commcare.case_importer.duration_per_case', 'active_duration_per_case'),
    Rename('active_duration_per_case', 'duration'),

    histogram_change('commcare.celery.task.time_to_run', 'commcare.celery.task.time_to_run.seconds'),
    Rename('commcare.celery.task.time_to_run', 'commcare.celery.task.time_to_run.seconds'),
    histogram_change('commcare.restores.count', 'commcare.restores.duration.seconds')
]

for segment in ('waiting', 'fixtures', 'fixture', 'cases'):
    CHANGES.append(histogram_change('commcare.restores.{}'.format(segment), 'commcare.restores.{}.duration.seconds'.format(segment)))


def _get_args():
    parser = argparse.ArgumentParser(description='Print CSV data from by host query')
    parser.add_argument('--config', default='config.yml', help='Path to config file.')
    parser.add_argument('--update', action='store_true', help='Perform the update')
    parser.add_argument('--dashboard', help='Only process this dashboard')
    return parser.parse_args()


def inline_diff(a, b):
    matcher = difflib.SequenceMatcher(None, a, b)

    def process_tag(tag, i1, i2, j1, j2):
        if tag == 'replace':
            return colored.red(matcher.a[i1:i2]), colored.green(matcher.b[j1:j2])
        if tag == 'delete':
            return colored.red(matcher.a[i1:i2]), ''
        if tag == 'equal':
            return matcher.a[i1:i2], matcher.a[i1:i2]
        if tag == 'insert':
            return '', colored.green(matcher.b[j1:j2])
        assert False, "Unknown tag %r"%tag

    parts = [process_tag(*t) for t in matcher.get_opcodes()]
    l1, l2 = list(zip(*parts))
    return ''.join(str(p) for p in l1), ''.join(str(p) for p in l2)


def _check_query(request, context):
    attr = 'q'
    query = request[attr]
    for change in itertools.chain.from_iterable(CHANGES):
        new = change(query)
        if new:
            change.mark_seen()
            diff_old, diff_new = inline_diff(query, new)
            print('    "{}"\n\t{}\n\t{}\n'.format(context, diff_old, diff_new))
            query = new
    if request[attr] != query:
        request[attr] = query
        return True

    return False


if __name__ == "__main__":
    args = _get_args()
    config = get_config(args.config)
    init_datadog(config)
    dashboards = api.Dashboard.get_all()
    for dashboard_info in dashboards['dashboards']:
        if args.dashboard and dashboard_info['title'] != args.dashboard:
            continue
        print('--------------------------------------------------------')
        print(dashboard_info['title'])
        changed = False
        dashboard = api.Dashboard.get(dashboard_info['id'])
        dashboard_orig = json.loads(json.dumps(dashboard))
        for widget in dashboard['widgets']:
            widget = widget['definition']
            requests = widget.get('requests')
            if not requests:
                continue
            widget_title = widget.get('title', '')
            if isinstance(requests, list):
                for req in requests:
                    if 'q' in req:
                        changed |= _check_query(req, widget_title)
            elif isinstance(requests, dict):
                try:
                    changed |= _check_query(requests['fill'], widget_title)
                except KeyError:
                    pass

                try:
                    changed |= _check_query(requests['size'], widget_title)
                except KeyError:
                    pass
        if changed and args.update:
            with open("{}-{}-{}.json".format(dashboard_info['id'], dashboard_info['title'], datetime.utcnow().isoformat()), 'w') as f:
                json.dump(dashboard_orig, f, indent=4)
            del dashboard['author_name']
            resp = api.Dashboard.update(**dashboard)
            if 'errors' in resp:
                raise Exception(resp['errors'])

    monitors = api.Monitor.get_all()
    for monitor in monitors:
        for change in itertools.chain.from_iterable(CHANGES):
            query = monitor['query']
            if change.from_val in query:
                print('Found in monitor: {}\n\t{}\n'.format(monitor['name'], query))
            # api.Monitor.update(monitor['id'], query=)

    for change in itertools.chain.from_iterable(CHANGES):
        if not change.seen:
            print("Change not found: {}".format(change))