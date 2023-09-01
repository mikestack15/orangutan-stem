import logging

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
logging.getLogger().setLevel(logging.INFO)


class S3ToGCSAndBigQueryOperator(BaseOperator):
    """
    Author: Michael Stack
    Last Updated: 8/28/2023

    This custom airflow operator transfers json data objects from S3 to Google Cloud Storage (GCS) and then loads the data 
    into the destination BigQuery table. Be sure to set up the prerequisite connections needed for this operator 
    to run successfully, which includes:
    aws_default
    google_cloud_default
    bigquery_default

    This operator inherits from the Airflow `BaseOperator` and uses the `S3ToGCSOperator` and `GCSToBigQueryOperator`.
    Airflow has excellent documentation on the `S3ToGCSOperator` and `GCSToBigQueryOperator` if you want to add additional
    attributes to 'S3ToGCSAndBigQueryOperator' operator for your own use-cases.

    Attributes:
        template_fields (tuple[str]): Contains the templated fields that will be resolved by Airflow.
        s3_bucket (str): Name of the source S3 bucket.
        s3_key (str): Key of the source file in the S3 bucket.
        gcs_bucket (str): Name of the destination GCS bucket.
        gcs_key (str): Key for the destination file in the GCS bucket.
        gcs_source_obj (str): Path of the file in GCS to be loaded into BigQuery.
        bigquery_table (str): Name of the BigQuery table (in 'dataset.table' format) where data will be loaded.
        gcs_bq_source_format (str): options are ['CSV', 'NEWLINE_DELIMITED_JSON', 'AVRO', 'GOOGLE_SHEETS', 'DATASTORE_BACKUP', 'PARQUET']
        bq_write_disposition (str): options are ['WRITE_APPEND', 'WRITE_TRUNCATE']
        bigquery_schema_fields (List[Dict], optional): List of schema fields for the BigQuery table.
        s3_conn_id (str, optional): ID of the Airflow connection used for S3.
        gcs_conn_id (str, optional): ID of the Airflow connection used for GCS.
        bigquery_conn_id (str, optional): ID of the Airflow connection used for BigQuery.
    """

    template_fields = ('s3_key', 'gcs_bucket', 'gcs_key', 'bigquery_table')

    def __init__(
        self,
        s3_bucket,
        s3_key,
        gcs_bucket,
        gcs_key,
        gcs_source_obj,
        bigquery_table,
        gcs_bq_source_format,
        bq_write_disposition,
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
        self.gcs_source_obj = gcs_source_obj
        self.bigquery_table = bigquery_table
        self.gcs_bq_source_format = gcs_bq_source_format
        self.bq_write_disposition = bq_write_disposition
        self.bigquery_schema_fields = bigquery_schema_fields
        self.s3_conn_id = s3_conn_id
        self.gcs_conn_id = gcs_conn_id
        self.bigquery_conn_id = bigquery_conn_id

    def execute(self, context):
        logging.info("Transferring file from S3 to GCS...")
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        gcs_hook = GCSHook(gcp_conn_id=self.gcs_conn_id)

        s3_object = f's3://{self.s3_bucket}/{self.s3_key}'

        s3_to_gcs_op = S3ToGCSOperator(
            task_id="s3_to_gcs",
            bucket=self.s3_bucket,
            prefix=self.s3_key,
            aws_conn_id=self.s3_conn_id,
            gcp_conn_id=self.gcs_conn_id,
            dest_gcs=f'gs://{self.gcs_bucket}/',
            replace=True,
            gzip=False,
            )

        s3_to_gcs_op.execute(context)

        logging.info("File transferred from S3 to GCS successfully.")

        logging.info("Loading data from GCS to BigQuery...")
        bigquery_operator = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery',
            bucket=self.gcs_bucket,  # name of the GCS bucket where the source object is loaded
            source_objects=[self.gcs_source_obj],  # GCS path for the file you want to load into bigquery
            destination_project_dataset_table=self.bigquery_table,  # 'project.dataset.table' for the BQ table
            schema_fields=self.bigquery_schema_fields, # List[Dict] of fields 
            allow_quoted_newlines=True, # see https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/transfers/gcs_to_bigquery.html#GCSToBigQueryOperator
            source_format=self.gcs_bq_source_format, # options are ['CSV', 'NEWLINE_DELIMITED_JSON', 'AVRO', 'GOOGLE_SHEETS', 'DATASTORE_BACKUP', 'PARQUET']
            write_disposition=self.bq_write_disposition,  # specify what happens if the table already exists ['WRITE_APPEND', 'WRITE_TRUNCATE']
            #skip_leading_rows=1,  # useful for csv files with a header row
        )
        bigquery_operator.execute(context)
        logging.info("Data loaded from GCS to BigQuery successfully.")
