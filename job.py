import json, socket, argparse, sys, auth
import requests
import os
from jinja2 import Environment, PackageLoader
from datetime import datetime

current_time = datetime.now().strftime("%H:%M:%S")

# Optional: lift to env files. 

parser = argparse.ArgumentParser(description='DSC job management for Databricks')
parser.add_argument('--params', type=str, help='your Databricks and Azure parameter file', default='params.json')
args = parser.parse_args()

configuration = json.load(open(args.params))

auth_token = auth.get_auth_token(configuration)
databricks_uri = configuration['databricks_uri'] + "/api/2.0/%s"

# Settings for jinja2
tplenv = Environment(loader=PackageLoader('job','templates'))
tplenv.filters['jsonify'] = json.dumps

# Settings for request/urllib3
head = {'Authorization': 'Bearer ' + auth_token["access_token"], 'Content-Type': 'application/json'}

# Get something from Databricks, parse to JSON if asked
def get_db(action, returnJson=False):
    url = databricks_uri % action
    log("REST - GET - Calling %s" % action)
    response = requests.get(url, headers=head)
    return response.json() if json else response

# Post something from Databricks, parse to JSON if asked
def post_db(action, jsonpl, returnJson=False):
    url = databricks_uri % action
    log("REST - POST - Calling %s" % action)
    response = requests.post(url, headers=head, data=jsonpl)
    return response

# Delete a job, this is a guaranteed operation by the Databricks API on successful ack.
def delete_job(id):
    log("Deleting %s" % id)

    tpl = tplenv.get_template('delete.jinja2').render(id=id)
    result = post_db("jobs/delete", tpl)
    if(result.ok):
        log("Deleted job %s" % id)
    else:
        log("Error deleting job: %s" % result.content)

# Helper to print timestamps
def log(s):
    print("[%s] %s" % (current_time, s))

def main():

    log("Running execution against %s" % configuration['databricks_uri'])

    current_jobs = get_db("jobs/list", returnJson=True)
    current_jobnames = []

    if(len(current_jobs) > 0): 
        log("Total of %s jobs found" % len(current_jobs['jobs']))
        current_jobnames = [(j['settings']['name'],j['job_id']) for j in current_jobs['jobs'] if j['creator_user_name'] == configuration["client_id"]]
    else:
        log("No jobs")

    # Set up definition based on input from Molly
    target_jobs = [json.load(open(jobcfg)) for jobcfg in os.scandir('jobs') if(jobcfg.is_file() and jobcfg.path.endswith('.json'))]
    target_jobnames = [j['name'] for j in target_jobs]

    # All jobs that need to be deleted
    jobs_to_delete = filter(lambda x: x[0] in target_jobnames, current_jobnames)

    # Delete active jobs for the name in job1
    # TODO: The above definition need to come from a folder in DBFS, then loop over them and pull. 
    [delete_job(item[1]) for item in jobs_to_delete]
    
    # Create a new job with the name above
    template = tplenv.get_template('standard.jinja2')
    
    for x in target_jobs:
        task = template.render(job=x)
        result = post_db("jobs/create", task).json()
        log("Created a new job %s" % result['job_id'])

# Module hook
if __name__ == '__main__':
    main()