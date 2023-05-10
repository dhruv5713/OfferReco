import os
import json
import shutil
import turicreate as tc
from tqdm import tqdm
import pandas as pd
import pathlib

from google.cloud import bigquery
from google.cloud import storage

from constants import *
from utils import (get_latest_dates, update_basic_history,
                   top_visited_blogs, create_ratings)

import pytz
from datetime import datetime
from pytz import timezone
from tzlocal import get_localzone
format = "%Y-%m-%d %H:%M:00"
# Current time in UTC
now_utc = datetime.now(timezone('Asia/Kolkata'))
prediction_time = now_utc.strftime(format)

fmt = "%Y-%m-%d %H:%M:%S"

now_utc = datetime.now(pytz.timezone('UTC'))

print("start --------------------",now_utc.astimezone(pytz.timezone('Asia/Kolkata')).strftime(fmt))



# Get Latest Dates
from_date, to_date = get_latest_dates()
print("Basic history will be updated from: {} to: {}".format(from_date, to_date))

# Update Basic History
update_basic_history(from_date, to_date)
print("Basic Table updated from: {} to: {}".format(from_date, to_date))

# Update Top Visited blogs
top_visited_blogs()
print("Top visited blogs updated")

# Create Rating
create_ratings()
print("Ratings Created")

# Get the latest ratings
rating_data = """SELECT * except(clientId), CONCAT("'", clientId) as clientId FROM `{}.{}.{}`""".format(
    project_id,
    dataset_id,
    final_rating_table
)

# Construct a BigQuery client object.
client = bigquery.Client()

# Query to get the ratings data
query_job = client.query(rating_data)
main_rating_df = query_job.to_dataframe()

# Remove the pagepaths 
rating_df = main_rating_df[['clientId', 'blog', 'rating']]
rating_df = rating_df.groupby(['clientId', 'blog']).agg({'rating': 'mean'}).reset_index()
rating_df["clientId"] = rating_df['clientId'].apply(lambda x: x.lstrip("'")) 

print("Creating Model")
# Create TuriDF
actions = tc.SFrame(rating_df)
model = tc.recommender.item_similarity_recommender.create(
    actions,
    'clientId',
    'blog',
    target="rating"
)

print("Model Created")
results = model.recommend()
results = results.to_dataframe()

main_rating_df["clientId"] = main_rating_df['clientId'].apply(lambda x: x.lstrip("'"))

blog_tags = main_rating_df[['blog', 'pagepath']]
blog_tags = blog_tags.drop_duplicates().reset_index(drop=True)

print("Mapping blog tags with their pagepaths")
mapped_blogs = pd.merge(
    left=results,
    right=blog_tags,
    on=['blog'],
    how='left'
)

recommended_df = mapped_blogs.groupby("clientId")['pagepath'].apply(list).reset_index()


print("Saving the recommendations")
PARENT_FOLDER = "recommended_blogs"
shutil.rmtree(PARENT_FOLDER, ignore_errors=True)
os.makedirs(PARENT_FOLDER, exist_ok=True)

for row in tqdm(recommended_df.iterrows()):
    clientid = row[1].clientId
    path = os.path.join(PARENT_FOLDER, clientid + ".json")
    reco_blog = {}
    reco_blog['recommended'] = row[1].pagepath
    with open(path, 'w') as f:
        f.write(json.dumps(reco_blog))

print("JSON files created")

print("Uploading the jsons to the GCS bucket: {}".format(bucket_name))
local_path = os.path.join(
    "/home/jupyter/Blog_recommendation/production",
    PARENT_FOLDER
)
pathlib.Path(local_path).mkdir(parents=True, exist_ok=True)

gcs_path = "gs://{}/clientids".format(bucket_name)
command = "gsutil -m cp -r '{}' '{}'".format(
    local_path,
    gcs_path
)

print("Uploading the recommendations to the GCS")
print("uploading start time --------------------",now_utc.astimezone(pytz.timezone('Asia/Kolkata')).strftime(fmt))
os.system('{} >/dev/null 2>&1'.format(command))
print("uploading end time --------------------",now_utc.astimezone(pytz.timezone('Asia/Kolkata')).strftime(fmt))

print("DONE")