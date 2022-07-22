import config as project_config
import utils

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

default_args = {
    "start_date": days_ago(1),
}


@task
def get_list_data(kwargs):
    import requests

    response = requests.get(kwargs["url"], params=kwargs["params"]).json()

    if type(response) is dict and 'results' in response:
        return response['results']

    return response

@task
def json_to_file(response_data):
    conf_files = [
        (project_config.local_file_dir,f"{v}.json")
        for configmap in project_config.get_conf_api
        for k, v in configmap.items()
        if k == "name"
    ]
    return list(
        map(utils.write_file, conf_files, response_data)
    )


@task
def delete_temp_files():
    utils.remove_temp_files(project_config.local_file_dir)


with DAG(dag_id="fake_shop_datapipeline", default_args=default_args) as dag:

    gcp_storage = LocalFilesystemToGCSOperator(
        task_id="upload_file_gcp",
        gcp_conn_id="gcp_storage",
        src=f"{project_config.local_file_dir}/*.json",
        dst=f"api-data/",
        bucket=project_config.general_config["BUCKET"],
        mime_type="application/json",
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        gcp_conn_id="gcp_storage",
        project_id=project_config.general_config["PROJECT_ID"],
        cluster_config=project_config.dataproc_create_cluster_config["config"],
        region=project_config.dataproc_create_cluster_config["region"],
        cluster_name=project_config.dataproc_create_cluster_config["clusterName"],
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        gcp_conn_id="gcp_storage",
        job=project_config.dataproc_pyspark_job_config["job"],
        region=project_config.dataproc_create_cluster_config["region"],
        project_id=project_config.general_config["PROJECT_ID"],
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        gcp_conn_id="gcp_storage",
        project_id=project_config.general_config["PROJECT_ID"],
        cluster_name=project_config.dataproc_create_cluster_config["clusterName"],
        region=project_config.dataproc_create_cluster_config["region"],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        json_to_file(get_list_data.expand(kwargs=project_config.get_conf_api))
        >> gcp_storage
    )
    (
        gcp_storage
        >> delete_temp_files()
        >> create_cluster
        >> pyspark_task
        >> delete_cluster
    )
