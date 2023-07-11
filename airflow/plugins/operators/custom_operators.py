from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.decorators import apply_defaults

class S3toBigQueryOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        s3_bucket,
        s3_key,
        gcs_bucket,
        gcs_key,
        dataset_id,
        table_id,
        bigquery_conn_id='bigquery_default',
        gcp_conn_id='google_cloud_default',
        aws_conn_id='aws_default',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.bigquery_conn_id = bigquery_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

        self.log.info("Downloading file from S3")
        data = s3_hook.read_key(self.s3_key, self.s3_bucket)

        self.log.info("Uploading file to GCS")
        gcs_hook.upload(self.gcs_bucket, self.gcs_key, data)

        self.log.info("Loading data to BigQuery")
        gcs_to_bq = GCSToBigQueryOperator(
            task_id='gcs_to_bq',
            bucket=self.gcs_bucket,
            source_objects=[self.gcs_key],
            destination_project_dataset_table=f'{self.dataset_id}.{self.table_id}',
            autodetect=True,
            source_format='NEWLINE_DELIMITED_JSON',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            bigquery_conn_id=self.bigquery_conn_id,
            google_cloud_storage_conn_id=self.gcp_conn_id,
        )
        gcs_to_bq.execute(context)

class DogNamedMike():
    print('bark bark')