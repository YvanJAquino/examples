import os
import time
from datetime import datetime, timezone, timedelta
import json
import logging
from uuid import uuid4
from requests import Session, Request
from requests.exceptions import HTTPError
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import logging as gcplogs

# Cloud Logging configuration
logs = gcplogs.Client() 
logs.setup_logging()

# Must be provided as environment variables.
PROJECT_ID = os.environ.get("PROJECT_ID")
BUCKET = os.environ.get("BUCKET")
DATASET, TABLE = os.environ.get("DATASET"), os.environ.get("TABLE")
AUTH_TOKEN = os.environ.get("GFW_AUTHTOKEN")


class JSONLogger:

    """
    JSONLogger is a helper class that wraps the default loggers messages 
    with json.dumps(msg, default=str).      
    """

    def __init__(self):
        self.logger = logging

    def log(self, level, msg, *args, default=str, **kwargs):
        # Convert the message to JSON first.
        msg = json.dumps(msg, default=default)
        return self.logger.log(level, msg, *args, **kwargs)
    
    def debug(self, msg, *args, **kwargs):
        level = 10
        return self.log(level, msg, *args, default=str, **kwargs)

    def info(self, msg, *args, default=str, **kwargs):
        level = 20
        return self.log(level, msg, *args, default=str, **kwargs)

    def warning(self, msg, *args, default=str, **kwargs):
        level = 30
        return self.log(level, msg, *args, default=str, **kwargs)

    def error(self, msg, *args, default=str, **kwargs):
        level = 40
        return self.log(level, msg, *args, default=str, **kwargs)

    def critical(self, msg, *args, default=str, **kwargs):
        level = 50
        return self.log(level, msg, *args, default=str, **kwargs)


class DadJoke:

    def __init__(self, tz=timezone(-timedelta(hours=5))):
        self.url = "https://icanhazdadjoke.com"
        

        # Use standard logging library (GCP Logging integrates directly)
        self.logger = JSONLogger()
        # Default UTC -5:00 (EST)
        self.tz = tz
        
        # You can set your AUTH_TOKEN into headers
        self.headers = dict(accept="application/json")
        self.session = Session()
        self.session.headers.update(**self.headers)
        self.responses = []
        self.gcs = storage.Client()
        self.bq = bigquery.Client()

    @property
    def timestamp(self):
        return datetime.now(tz=self.tz).isoformat()

    def get(self):
        req = Request("GET", self.url)
        req = self.session.prepare_request(req)
        res = self.session.send(req)
        if res.status_code != 200:
            log = {
                'apiCallStatus': res.status_code,
                'attemptedAt': self.timestamp,
            }
            self.logger.critical(log)
            raise HTTPError(f"CRITICAL: {res.status_code} - please review and retry.")
        data = res.json()
        data.update({"created": time.time_ns()})
        # Appends to "data" object, can handle multiple calls.  
        self.responses.append(data)
        return data

    def store_object(self, bucket_name=None):
        name = f"{uuid4()}.json"
        bucket_name = BUCKET if not bucket_name else bucket_name
        bucket = self.gcs.get_bucket(bucket_name)
        blob = bucket.blob(name)
        blob.upload_from_string(json.dumps(self.responses, indent=4))
        return blob

    def store_table(self, table_id=None):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.job.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
            schema_update_options=bigquery.job.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        )
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}" if not table_id else table_id
        job = self.bq.load_table_from_json(self.responses, table_id, job_config=job_config, location="us")
        return job.result()

    def __call__(self, request=None):
        begin = time.time()
        # You can completely ignore the request if you'd like. 
        self.get()
        blob = self.store_object()
        bq_res = self.store_table()
        
        end = time.time()
        log = {
            "executionTime": end - begin,
            "attemptedAt": self.timestamp,
            "status": "COMPLETE",
            "gcsObject": {
                "gcsUri":f"gs://{BUCKET}/{blob.name}",
                "gcsSelfLink":blob.self_link,
            },
            "bigQueryResult": json.dumps(vars(bq_res), default=str)  
        }
        self.logger.info(log)

# This is the entrypoint for the Cloud Function.
def entrypoint(request=None):
    joker = DadJoke()
    joker(request)

# # Uncomment the call to entrypoint to test locally.
# entrypoint()
