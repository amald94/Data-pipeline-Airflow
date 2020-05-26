from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_events = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS JSON '{}'
            TIMEFORMAT as 'epochmillisecs'
        """
    copy_songs = """ 
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS JSON '{}'   
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name = "",
                 s3_bucket="",
                 s3_key = "",
                 file_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.execution_date = kwargs.get('execution_date')  

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        if self.table_name == 'staging_songs':
            copy_sql = self.copy_songs.format(self.table_name, s3_path, 
                                                credentials.access_key, 
                                                credentials.secret_key, 'auto')
        else:
            copy_sql = self.copy_events.format(self.table_name, s3_path, 
                                                credentials.access_key, 
                                                credentials.secret_key, 'auto')


        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift_hook.run(copy_sql)






