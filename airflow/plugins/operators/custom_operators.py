import logging

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
logging.getLogger().setLevel(logging.INFO)


class S3ToGCSAndBigQueryOperator(BaseOperator):
    template_fields = ('s3_key', 'gcs_bucket', 'gcs_key', 'bigquery_table')

    def __init__(
        self,
        s3_bucket,
        s3_key,
        gcs_bucket,
        gcs_key,
        bigquery_table,
        bigquery_schema_fields=None,
        s3_conn_id='aws_default',
        gcs_conn_id='google_cloud_default',
        bigquery_conn_id='bigquery_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key
        self.bigquery_table = bigquery_table
        self.bigquery_schema_fields = bigquery_schema_fields
        self.s3_conn_id = s3_conn_id
        self.gcs_conn_id = gcs_conn_id
        self.bigquery_conn_id = bigquery_conn_id

    def execute(self, context):
        logging.info("Transferring file from S3 to GCS...")
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        gcs_hook = GCSHook(gcp_conn_id=self.gcs_conn_id)

        s3_object = f's3://{self.s3_bucket}/{self.s3_key}'
        gcs_object = f'gs://{self.gcs_bucket}/{self.gcs_key}'

        s3_to_gcs_op = S3ToGCSOperator(
            task_id="s3_to_gcs",
            bucket=self.s3_bucket,
            prefix=self.s3_key,
            aws_conn_id=self.s3_conn_id,
            gcp_conn_id=self.gcs_conn_id,
            dest_gcs=f'gs://{self.gcs_bucket}/',
            #gcs_prefix=gcs_object,
            replace=True,
            gzip=False,
            )

        s3_to_gcs_op.execute(context)

        logging.info("File transferred from S3 to GCS successfully.")

        logging.info("Loading data from GCS to BigQuery...")
        bigquery_operator = BigQueryInsertJobOperator(
            task_id='gcs_to_bigquery',
            configuration={
                'load': {
                    'sourceFormat': 'NEWLINE_DELIMITED_JSON',
                    'destinationTable': self.bigquery_table,
                    'sourceUris': [gcs_object],
                    'schema': {'fields': self.bigquery_schema_fields},
                    'writeDisposition': 'WRITE_APPEND',
                    'ignoreUnknownValues': True
                }
            },
            gcp_conn_id=self.bigquery_conn_id
        )
        bigquery_operator.execute(context)
        logging.info("Data loaded from GCS to BigQuery successfully.")
