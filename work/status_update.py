import re
import json
from collections import defaultdict

import datetime
from datetime import datetime, timedelta, timezone
import dateutil.parser
import pytz
from pytz import timezone

import pandas as pd

import plotly.figure_factory as ff

from IPython.core.display import display, HTML

from pydent import AqSession, __version__


def get_session(instance):
    with open('secrets.json') as f:
        secrets = json.load(f)

    credentials = secrets[instance]
    session = AqSession(
        credentials["login"],
        credentials["password"],
        credentials["aquarium_url"]
    )

    msg = "Connected to Aquarium at {} using pydent version {}"
    print(msg.format(session.url, str(__version__)))

    me = session.User.where({'login': credentials['login']})[0]
    print('Logged in as {}\n'.format(me.name))
    
    return session

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
        
def find_in_batches(model, ids, batch_size):
    n_total = len(ids)
    results = []
    nested_ids = chunks(ids, batch_size)

    for these_ids in nested_ids:
        these_results = model.where({"id": these_ids})
        if these_results:
            results += these_results
            n_found = len(results)
            pct_found = round((100 * n_found / n_total))
            print("Found {}% ({}) of {} records".format(pct_found, n_found, n_total))
            
    return results

def get_delta(times):
    start = dateutil.parser.parse(times[0])
    end = dateutil.parser.parse(times[1])
    return (end - start).seconds/60

def get_step_times(state):
    times = [s["time"] for s in state if s["operation"] == "next"]
    step_times = []
    
    i = 0
    while i < (len(times) - 1):
        step_times.append(get_delta([times[i], times[i+1]]))
        i += 1
    
    return step_times

class StatusExplorer():

    def __init__(self, session):
        self.session = session
        
        self.samples = []
        
        self.field_values = []
        self.operations = []
        
        self.plan_associations = []
        self.plans = []
        
        self.job_associations = []
        self.jobs = []
        
    def set_samples(self, samples):
        self.samples = samples
        
    def set_field_values(self, field_values):
        self.field_values = field_values
        
    def set_operations(self, operations):
        self.operations = operations
        
    def set_plan_associations(self, plan_associations):
        self.plan_associations = plan_associations
        
    def set_plans(self, plans):
        self.plans = plans
        
    def set_job_associations(self, job_associations):
        self.job_associations = job_associations
        
    def set_jobs(self, jobs):
        self.jobs = jobs
        
    def operations_for(self, sample=None):
        if sample:
            these_fvs = [fv for fv in self.field_values if fv.child_sample_id == sample.id]
            these_op_ids = [fv.parent_id for fv in these_fvs]
            return [op for op in self.operations if op.id in these_op_ids]
        
    def plans_for(self, operations=None):
        if operations:
            these_op_ids = [op.id for op in operations]
            these_plan_ids = [pa.plan_id for pa in self.plan_associations if pa.operation_id in these_op_ids]
            return [p for p in self.plans if p.id in these_plan_ids]
        
    def jobs_for(self, operations=None):
        if operations:
            these_op_ids = [op.id for op in operations]
            these_job_ids = [ja.job_id for ja in self.job_associations if ja.operation_id in these_op_ids]
            return [j for j in self.jobs if j.id in these_job_ids]

    def get_stats(self, job):
        js = {}
        js["id"] = job.id

        these_job_associations = [j for j in self.job_associations if j.job_id == job.id]
        js["n_ops"] = len(these_job_associations)
        ot_id = job.state[0]["arguments"]["operation_type_id"]
        ot = self.session.OperationType.find(ot_id)
        js["ot_name"] = ot.name

        js["start_time"] = job.state[2].get('time')
        js["stop_time"] = job.state[-2].get('time')

        if js["start_time"] and js["stop_time"]:
            js["duration"] = get_delta((js["start_time"], js["stop_time"]))

        js["step_times"] = get_step_times(job.state)
        js["length"] = len([s for s in job.state if s["operation"] == "display"])
        js["job_completeness"] = job.is_complete
        js["state_completeness"] = job.state[-1]['operation']
        js["complete"] = job.is_complete and job.state[-1]['operation'] == "complete"
        return js