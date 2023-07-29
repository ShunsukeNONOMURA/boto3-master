from boto3_driver import *
from boto3_app_service import *

boto3_app_service = Boto3AppService()
boto3_app_service.pprint_ec2_list()
boto3_app_service.pprint_s3_bucket_list()
boto3_app_service.pprint_s3_object_list(
    bucket_name='bucket_name',
    prefix='prefix/'
)
boto3_app_service.pprint_s3_link(
    'bucket_name',
    'key_name',
    'file_name',
)
boto3_app_service.create_cost_report(
    path_output_root='./out',
    path_profiles_yaml='./profiles.yml',
)
from datetime import datetime
from zoneinfo import ZoneInfo
boto3_app_service.create_log_events(
    log_group_name='/logs/name',
    start_datetime=datetime(2023,7,5,8),
    end_datetime=datetime(2023,7,5,10),
    filter_pattern="pattern",
)
boto3_app_service.print_assume_role_pyathena_select(
    s3_staging_dir='s3://tmp',
    work_group='workgroup',
    query='select * from db.table limit 10;'
)
boto3_app_service.print_pyathena_select(
    s3_staging_dir='s3://tmp',
    work_group='workgroup',
    query='select * from db.table limit 10;'
)
exit(0)

