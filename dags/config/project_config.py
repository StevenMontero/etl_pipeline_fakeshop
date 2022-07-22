#!/usr/bin/env python
from datetime import datetime
from random import randint

# General config
# Edit this file

limit = 50
ramdom_offset = randint(0, 200)
date_today = datetime.now().strftime("%d_%m_%Y")

local_file_dir = "./dags/tempfiles"

end_point_api = {
    "FAKE_SHOP_API": "https://api.escuelajs.co/api/v1/",
    "RANDOM_USER_API": "https://randomuser.me/api/",
}

get_conf_api = [
    {
        "name": f"ramdon_user_data_{date_today}",
        "url": end_point_api["RANDOM_USER_API"],
        "params": {
            "results": limit,
        },
    },
    {
        "name": f"ramdon_product_data_{date_today}",
        "url": f'{end_point_api["FAKE_SHOP_API"]}products',
        "params": {"offset": ramdom_offset, "limit": limit},
    },
]

general_config = {
    "BUCKET": "fake_shop_bucket",
    "PROJECT_ID": "fake-shop-data-pipeline", # Type here your project id
}

# dataproc config
dataproc_config = {
    "REGION": "us-central1",
    "ZONE_URI": "us-central1-f",
    "CLUSTER_NAME": "fake-shop-cluster",
    "INIT_ACTIONS": "gs://dataproc-initialization-actions/python/pip-install.sh",
    "IMAGE": "2.0-debian10",
}
dataproc_create_cluster_config = {
    "projectId": general_config["PROJECT_ID"],
    "region": dataproc_config["REGION"],
    "clusterName": dataproc_config["CLUSTER_NAME"],
    "config": {
        
        "config_bucket": general_config["BUCKET"],
        "gce_cluster_config": {
            "zone_uri": dataproc_config["ZONE_URI"],
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-4",
        },
        "software_config": {
            "image_version": dataproc_config["IMAGE"],
            "properties": {"dataproc:dataproc.allow.zero.workers": "true"},
            "optional_components": [],
        },
        "endpoint_config": {"enable_http_port_access": True},
    },
}

# pySPARK config
pyspark_config = {
    "PYTHON_FILE": "gs://"
    + general_config["BUCKET"]
    + "/data_pipeline_fakeshop_main.py",
    "SPARK_BQ_JAR": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
}
dataproc_pyspark_job_config = {
    "projectId": general_config["PROJECT_ID"],
    "job": {
        "placement": {"cluster_name": dataproc_config["CLUSTER_NAME"]},
        "reference": {
            "job_id": "stack-pyspark-job" + datetime.now().strftime("%m%d%Y%H%M%S"),
            "project_id": general_config["PROJECT_ID"],
        },
        "pyspark_job": {
            "main_python_file_uri": pyspark_config["PYTHON_FILE"],
            "jar_file_uris": [pyspark_config["SPARK_BQ_JAR"]],
            "args": [f'gs://{general_config["BUCKET"]}/api-data/{value["name"]}.json' for value in get_conf_api]
        },
    },
}
