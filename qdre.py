import sys
import os
import re
import argparse
import subprocess
from bs4 import BeautifulSoup
from pandas import to_datetime


def check_pattern(job, pattern):
    name = job.jb_name.string
    if pattern.search(name):
        return True
    else:
        return False


def check_state(job, state):
    if state in job.state.string:
        return True
    else:
        return False


def check_queue(job, queue):
    job_queue = job.queue_name.string
    if job_queue is not None:
        job_queue = job_queue.split('@')[0]
        if queue in job_queue:
            return True
        else:
            return False
    else:
        return False


def check_time_before(job, timepoint):
    t = 'jat_start_time' if 'r' in job.state.string else 'jb_submission_time'
    job_time = to_datetime(getattr(job, t).string)
    if job_time < timepoint:
        return True
    else:
        return False


def check_time_after(job, timepoint):
    t = 'jat_start_time' if 'r' in job.state.string else 'jb_submission_time'
    job_time = to_datetime(getattr(job, t).string)
    if job_time > timepoint:
        return True
    else:
        return False


def list_job_info():
    job_info, _ = subprocess.Popen(
        ['qstat', '-u', os.environ['USER'], '-xml'],
        stdout=subprocess.PIPE
    ).communicate()

    xml = BeautifulSoup(job_info, 'lxml')
    return xml.find_all('job_list')


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-p', '--pattern', type=str,
        default=None,
        help='jobs with names matching this pattern will be killed')

    parser.add_argument(
        '-s', '--state', type=str,
        default=None,
        help='jobs with this state will be killed')

    parser.add_argument(
        '-q', '--queue', type=str,
        default=None,
        help='jobs in this queue will be killed')

    parser.add_argument(
        '-b', '--before', type=str,
        default=None,
        help='only jobs submitted / started before this time will be killed. '
             'can be anything that pandas.to_datetime understands')

    parser.add_argument(
        '-a', '--after', type=str,
        default=None,
        help='only jobs submitted / started after this time will be killed')

    parser.add_argument(
        '-n', '--dry-run', action='store_true',
        default=False,
        help='do dry run, do not kill.')

    options = parser.parse_args()

    if options.pattern is not None:
        options.pattern = re.compile(options.pattern)

    if options.before is not None:
        options.before = to_datetime(options.before)

    if options.after is not None:
        options.after = to_datetime(options.after)

    return options


def qdre():
    options = parse_args()
    to_kill = set()
    for job in list_job_info():
        job_id = job.jb_job_number.string
        criteria = []
        if options.pattern is not None:
            criteria.append(check_pattern(job, options.pattern))
        if options.state is not None:
            criteria.append(check_state(job, options.state))
        if options.queue is not None:
            criteria.append(check_queue(job, options.queue))
        if options.before is not None:
            criteria.append(check_time_before(job, options.before))
        if options.after is not None:
            criteria.append(check_time_after(job, options.after))

        if criteria and all(criteria):
            to_kill.add(job_id)

    if to_kill:
        print('matching jobs: ' + ', '.join(to_kill))
        if not options.dry_run:
            p = subprocess.Popen(
                ['qdel', ','.join(to_kill)], stdout=subprocess.PIPE)
            stdout, stderr = p.communicate()
            if stdout:
                sys.stdout.write(stdout.decode())
            if stderr:
                sys.stdout.write(stderr.decode())
    else:
        if options.pattern is not None:
            print('no jobs matching pattern "{}"'.format(
                options.pattern.pattern))
        else:
            print('no jobs matching parameters')

if __name__ == '__main__':
    qdre()
