import json
import os
import shutil
from google.cloud import bigquery
from google.cloud import storage
from jinja2 import Environment, FileSystemLoader, select_autoescape

from datetime import datetime, timedelta
from pytz import timezone
from tzlocal import get_localzone

from constants import *

# Construct a BigQuery client object.
client = bigquery.Client()

# Current time in UTC
now_utc = datetime.now(timezone('Asia/Kolkata'))
prediction_day = now_utc.strftime('%A')
env = Environment(loader=FileSystemLoader('/home/jupyter/Blog_recommendation/queries'))
# Bucket object
bucket = storage.Client().get_bucket(bucket_name)

def get_latest_dates():
    basic_history_date_query = "SELECT max(date) from `{}.{}.{}`".format(
        project_id,
        dataset_id,
        basic_history_table
    )

    basic_history_date_job = client.query(
        basic_history_date_query
    )
    from_date = basic_history_date_job.to_dataframe().values.tolist()[0][0]
    from_date = (datetime.strptime(
        from_date, "%Y%m%d"
    ).date() + timedelta(days=1)).strftime("%Y%m%d")

    to_date = now_utc.date() - timedelta(days=1)
    to_date = to_date.strftime("%Y%m%d")
    return from_date, to_date

def update_basic_history(from_date, to_date):
    basic_history_template = env.get_template("basic_history.jinja2")
    basic_history_query = basic_history_template.render(
        from_date=from_date,
        to_date=to_date
    )

    # Update the table
    basic_table_id = "{}.{}.{}".format(project_id, dataset_id, basic_history_table)
    job_config = bigquery.QueryJobConfig(destination=basic_table_id)

    # Create partitioned table
    day_partition = bigquery.TimePartitioning()
    job_config.time_partitioning = day_partition

    # Table Disposition
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

    # Start the query, passing in the extra configuration.
    query_job = client.query(basic_history_query, job_config=job_config)
    query_job.result()  # Wait for the job to complete.


def top_visited_blogs():
    top_visited_template = env.get_template("top_visited_blogs.jinja2")
    top_visited_query = top_visited_template.render(
        project_id=project_id,
        dataset_id=dataset_id,
        basic_history_table=basic_history_table
    )

    query_job = client.query(top_visited_query)
    top_blogs = query_job.to_dataframe()

    # Create dictionary for top visited blogs
    top_visited_blogs = {}
    top_visited_blogs['recommended_blogs'] = list(top_blogs['pagepath'].to_dict().values())

    # Upload the json to the gcs
    blob = bucket.blob("top_visited_blogs.json")
    blob.upload_from_string(data=json.dumps(top_visited_blogs),content_type='application/json')


def create_ratings():
    final_rating_template = env.get_template("final_rating.jinja2")
    final_rating_query = final_rating_template.render(
        project_id=project_id,
        dataset_id=dataset_id,
        basic_history_table=basic_history_table
    )

    # Update the table
    rating_table_id = "{}.{}.{}".format(project_id, dataset_id, final_rating_table)
    job_config = bigquery.QueryJobConfig(destination=rating_table_id)


    # Table Disposition
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

    # Start the query, passing in the extra configuration.
    query_job = client.query(final_rating_query, job_config=job_config)
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(rating_table_id))

