# environment variable setup for private key file
import os
import pandas as pd   #pip install pandas  ##to install

from google.cloud import pubsub_v1    #pip install google-cloud-pubsub  ##to install
import time
import json;
import io;17
import glob

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="oceanic-student-451923-m7-5c6c7b5afc6f.json"

# TODO : fill project id 
project_id = 'oceanic-student-451923-m7'
topic_id = "mnist_image"


publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

df=pd.read_csv('mnist.csv')

for row in df.iterrows():
    value=row[1].to_dict()
    future = publisher.publish(topic_path, json.dumps(value).encode('utf-8'));
    print("Image with key "+str(value["ID"])+" is sent")
    time.sleep(0.1);
    
