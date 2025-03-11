
import boto3

# Initialize AWS Glue client for us-east-2
glue_client = boto3.client('glue', region_name='us-east-2')

# Global Configurations
CRAWLER_NAME = "customer360_crawler"
WORKFLOW_NAME = "Customer360_Analytics_Workflow"
GLUE_ROLE_ARN = "arn:aws:iam::509399637194:role/GlueDevRole"
RDS_CONNECTION_NAME = "my-rds-mysql-connection"
GLUE_DATABASE_NAME = "customer_analytics"
S3_TARGET_PATH = ["s3://mybucket31101999/analytics/"]
table_prefix = "customer_"
PROJECT_LIB_PATH="s3://mybucket31101999/code/customer_analytics/customer_analytics-0.1.0-py3-none-any.whl"


# Job Scripts in S3
GLUE_JOBS = {
    "purchase_behavior": "s3://mybucket31101999/code/customer_analytics/purchase_behavior.py",
    "churn_prediction": "s3://mybucket31101999/code/customer_analytics/churn_prediction.py",
    "omni_channel_engagement": "s3://mybucket31101999/code/customer_analytics/omni_channel_engagement.py",
    "fraud_detection": "s3://mybucket31101999/code/customer_analytics/fraud_detection.py",
    "pricing_trends": "s3://mybucket31101999/code/customer_analytics/pricing_trends.py",
}


import boto3

def create_glue_workflow(workflow_name):
    """Create an AWS Glue Workflow."""
    response = glue_client.create_workflow(
        Name=workflow_name,
        Description="Workflow for Customer 360 analytics pipeline"
    )
    print(f"✅ Workflow Created: {response['Name']}")
    return response


def create_glue_job(job_name, script_path):
    """Create an AWS Glue Job."""
    response = glue_client.create_job(
        Name=job_name,
        Role=GLUE_ROLE_ARN,
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_path,
            "PythonVersion": "3"
        },
        DefaultArguments={
           "--enable-glue-datacatalog": "true", 
           "--job-bookmark-option": "job-bookmark-enable",
            "--TempDir": f"s3://mybucket31101999/code/temp/{job_name}/",
            "--S3_TARGET_PATH": S3_TARGET_PATH[0],
            "--extra-py-files": PROJECT_LIB_PATH ,
            "--job-language": "python"
        },
        Connections={"Connections": [RDS_CONNECTION_NAME]},  # Attach RDS MySQL Connection
        NumberOfWorkers=2,  # Set Worker Count
        WorkerType="G.1X"  # Standard Glue Worker Type
    )
    print(f"✅ Job Created: {job_name}")
    return response


def create_glue_trigger(trigger_name, workflow_name, job_name=None, prev_job_name=None, schedule_expression=None, crawler_name=None):
    """Create a Glue Trigger to sequence jobs."""
    trigger_params = {
        "Name": trigger_name,
        "WorkflowName": workflow_name,
        "Actions": [{"JobName": job_name}]
    }
    if crawler_name:
        # If a crawler is provided, trigger the crawler instead of a job
        trigger_params["Actions"] = [{"CrawlerName": crawler_name}]
    else:
        trigger_params["Actions"] = [{"JobName": job_name}]    

    # First Job: ON_DEMAND Trigger (manual start)
    if schedule_expression:
        # Scheduled Trigger
        trigger_params["Type"] = "SCHEDULED"
        trigger_params["Schedule"] = schedule_expression
    elif not prev_job_name:
        # First Job: ON_DEMAND Trigger (manual start)
        trigger_params["Type"] = "ON_DEMAND"
    else:
        trigger_params["Type"] = "CONDITIONAL"
        trigger_params["Predicate"] = {
            "Conditions": [{"LogicalOperator": "EQUALS", "JobName": prev_job_name, "State": "SUCCEEDED"}]
        }

    response = glue_client.create_trigger(**trigger_params)
    print(f"✅ Trigger Created: {trigger_name}")
        # Activate the trigger after creation
    if prev_job_name is not None:
        glue_client.start_trigger(Name=trigger_name)
        print(f"🚀 Trigger Activated: {trigger_name}")
    return response


def start_workflow(workflow_name):
    """Start the Glue Workflow execution."""
    run_response = glue_client.start_workflow_run(Name=workflow_name)
    print(f"🚀 Workflow Run Started: {run_response['RunId']}")
    return run_response


def delete_glue_workflow(workflow_name):
    """Deletes an AWS Glue Workflow and all associated triggers."""
    glue_client = boto3.client("glue", region_name="us-east-2")

    try:
        # Delete workflow
        response = glue_client.delete_workflow(Name=workflow_name)
        print(f"✅ Workflow '{workflow_name}' deleted successfully.")
        return response
    except glue_client.exceptions.EntityNotFoundException:
        print(f"⚠️ Workflow '{workflow_name}' not found.")
    except Exception as e:
        print(f"❌ Error deleting workflow: {e}")

import boto3
from urllib.parse import urlparse

def delete_glue_job(job_name, script_s3_path=None):
    """Deletes an AWS Glue Job and optionally deletes the script from S3."""
    glue_client = boto3.client("glue", region_name="us-east-2")
    s3_client = boto3.client("s3")

    try:
        # Delete Glue Job
        glue_client.delete_job(JobName=job_name)
        print(f"✅ Glue job '{job_name}' deleted successfully.")
        
    except glue_client.exceptions.EntityNotFoundException:
        print(f"⚠️ Glue job '{job_name}' not found.")
    except Exception as e:
        print(f"❌ Error deleting Glue job: {e}")

import boto3

def delete_glue_trigger(trigger_name, workflow_name, job_name, prev_job_name=None):
    """Deletes an AWS Glue Trigger linked to a workflow and job."""
    glue_client = boto3.client("glue", region_name="us-east-2")

    try:
        # Delete Glue Trigger
        glue_client.delete_trigger(Name=trigger_name)
        print(f"✅ Trigger '{trigger_name}' deleted successfully.")

    except glue_client.exceptions.EntityNotFoundException:
        print(f"⚠️ Trigger '{trigger_name}' not found.")
    except Exception as e:
        print(f"❌ Error deleting trigger: {e}")

def create_glue_starting_trigger(workflow_name, trigger_name, first_job):
    """Creates a starting trigger for an AWS Glue Workflow."""
    glue_client = boto3.client("glue", region_name="us-east-2")

    try:
        response = glue_client.create_trigger(
            Name=trigger_name,
            Type="ON_DEMAND",  # Can be "SCHEDULED" if needed
            WorkflowName=workflow_name,
            Actions=[{"JobName": first_job}],
        )
        print(f"✅ Starting Trigger '{trigger_name}' created for workflow '{workflow_name}'.")
        return response
    except Exception as e:
        print(f"❌ Error creating trigger: {e}")

def delete_glue_workflow(workflow_name):
    """Deletes an AWS Glue Workflow and all associated triggers."""
    glue_client = boto3.client("glue", region_name="us-east-2")

    try:
        # Delete workflow
        response = glue_client.delete_workflow(Name=workflow_name)
        print(f"✅ Workflow '{workflow_name}' deleted successfully.")
        return response
    except glue_client.exceptions.EntityNotFoundException:
        print(f"⚠️ Workflow '{workflow_name}' not found.")
    except Exception as e:
        print(f"❌ Error deleting workflow: {e}")


import boto3
import time

def create_or_update_glue_crawler(crawler_name, role_arn, database_name, s3_target_paths, table_prefix=""):
    """
    Creates or updates an AWS Glue Crawler to scan S3 and create tables in Glue Data Catalog.

    :param crawler_name: Name of the Glue Crawler
    :param role_arn: IAM role for the Glue Crawler
    :param database_name: Glue Data Catalog database name
    :param s3_target_paths: List of S3 paths to crawl
    :param table_prefix: Prefix for tables created in the catalog
    """
    glue_client = boto3.client("glue", region_name="us-east-2")

    targets = {"S3Targets": [{"Path": path} for path in s3_target_paths]}

    crawler_def = {
        "Name": crawler_name,
        "Role": role_arn,
        "DatabaseName": database_name,
        "Targets": targets,
        "TablePrefix": table_prefix,
        "SchemaChangePolicy": {
            "UpdateBehavior": "LOG",
            "DeleteBehavior": "LOG"
        },
        "RecrawlPolicy": {"RecrawlBehavior": "CRAWL_EVERYTHING"}
        #"RecrawlPolicy": { "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY" }

    }

    try:
        # Check if the crawler exists
        glue_client.get_crawler(Name=crawler_name)
        response = glue_client.update_crawler(**crawler_def)
        print(f"🔄 Updated Glue Crawler: {crawler_name}")
    except glue_client.exceptions.EntityNotFoundException:
        # If the crawler does not exist, create a new one
        response = glue_client.create_crawler(**crawler_def)
        print(f"✅ Created Glue Crawler: {crawler_name}")

    return response

def start_crawler(crawler_name):
    """
    Starts the AWS Glue Crawler to scan S3 data.
    """
    glue_client = boto3.client("glue", region_name="us-east-2")
    
    print(f"🚀 Starting Glue Crawler: {crawler_name}")
    glue_client.start_crawler(Name=crawler_name)
    
    # Wait for the crawler to finish
    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        status = response["Crawler"]["State"]
        if status == "READY":
            print(f"✅ Crawler '{crawler_name}' finished successfully!")
            break
        print(f"⏳ Crawler running... Waiting for completion.")
        time.sleep(30)  # Wait for 30 seconds before checking again



def create_workflow():
    create_glue_workflow(WORKFLOW_NAME)
    create_or_update_glue_crawler(CRAWLER_NAME, GLUE_ROLE_ARN, GLUE_DATABASE_NAME, S3_TARGET_PATH, table_prefix)
    
    # Step 2: Create Glue Jobs
    for job, script in GLUE_JOBS.items():
        #delete_glue_job(job, script)
        create_glue_job(job, script)
        

    # Step 3: Create Triggers to Sequence Jobs
    prev_job = None
    for job in GLUE_JOBS.keys():
        #delete_glue_trigger(f"trigger_{job}", WORKFLOW_NAME, job, prev_job)
        create_glue_trigger(f"trigger_{job}", WORKFLOW_NAME, job, prev_job)
        prev_job = job

    create_glue_trigger(trigger_name=f"trigger_{CRAWLER_NAME}",workflow_name=WORKFLOW_NAME,prev_job_name=list(GLUE_JOBS.keys())[-1],
                        crawler_name=CRAWLER_NAME)
    #start_workflow(WORKFLOW_NAME)

def delete_workflow():
    for job, script in GLUE_JOBS.items():
        delete_glue_job(job, script)  

    for job in GLUE_JOBS.keys():
        delete_glue_trigger(f"trigger_{job}", WORKFLOW_NAME, job)
    delete_glue_trigger(f"trigger_{CRAWLER_NAME}", WORKFLOW_NAME, job)
    delete_glue_workflow(WORKFLOW_NAME)

def main():
       create_workflow()
       #delete_workflow()





 
    
if __name__ == "__main__":
    main()
