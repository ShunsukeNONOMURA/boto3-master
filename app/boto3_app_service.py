from boto3_driver import *

import os
import shutil
from zoneinfo import ZoneInfo

from pprint import pprint

class Boto3AppService:
    def __init__(self):
        pass

    def pprint_ec2_list(self, path_profile_yaml='./profile.yml'):
        """
        boto3_app_service = Boto3AppService()
        boto3_app_service.pprint_ec2_list()
        """
        pprint(Boto3Driver.create_driver_from_profile_yaml(path_profile_yaml).get_ec2_list())
    
    def pprint_s3_bucket_list(self, path_profile_yaml='./profile.yml'):
        """
        boto3_app_service = Boto3AppService()
        boto3_app_service.pprint_s3_bucket_list()
        """
        pprint(Boto3Driver.create_driver_from_profile_yaml(path_profile_yaml).get_s3_bucket_list())
    
    def pprint_s3_object_list(self, bucket_name, prefix=None, path_profile_yaml='./profile.yml'):
        """
        boto3_app_service = Boto3AppService()
        boto3_app_service.pprint_s3_object_list(
            bucket_name='bucket',
            prefix='prefix/prefix'
        )
        """
        response = Boto3Driver.create_driver_from_profile_yaml(path_profile_yaml).get_s3_object_list(bucket_name, prefix)
        pprint(response)
        pprint(len(response))

    def pprint_s3_link(self, bucket_name, key, file_name=None, path_profile_yaml='./profile.yml'):
        """
        boto3_app_service = Boto3AppService()
        boto3_app_service.pprint_s3_link(
            'bucket_name',
            'prefix/hogehoge.ext',
            # 'file_name.ext'
        )
        """
        pprint(Boto3Driver.create_driver_from_profile_yaml(path_profile_yaml).create_s3_url(bucket_name, key, file_name))

    def export_ssm_parameters_to_csv(self, path_profile_yaml='./profile.yml'):
        params = Boto3Driver.create_driver_from_profile_yaml(path_profile_yaml).get_ssm_parameters()
        params.export_df_csv(f'./out/ssm.csv')
        # pprint(params.df)

    def create_log_events(
            self, 
            log_group_name,
            start_datetime=None,
            end_datetime=None,
            filter_pattern=None,
            path_profile_yaml='./profile.yml',
            path_out='./out/log.json'
        ):
        boto3_driver = Boto3Driver.create_driver_from_profile_yaml(path_profile_yaml)

        log_events_all = boto3_driver.filter_log_events(
            log_group_name = log_group_name,
            start_datetime = start_datetime,
            end_datetime = end_datetime,
            filter_pattern=filter_pattern,
        )

        ## 変換
        for l in log_events_all:
            st = l['message'].replace('\"', '').replace('\'', '\"')
            try:
                stj = json.loads(st)
            except:
                stj = {}
            l['message_d'] = stj

        ## 書き込み
        path_out_log_json = path_out
        with open(path_out_log_json, 'w') as f:
            json.dump(log_events_all, f, sort_keys=True, indent=4, ensure_ascii=False)

    def create_cost_report(self, path_output_root='./out', path_profiles_yaml='./profiles.yml'):
        """
        boto3_app_service = Boto3AppService()
        boto3_app_service.create_cost_report(
            path_output_root='./out',
            path_profiles_yaml='./profiles.yml',
        )
        """
        # 実行時刻記録
        datetime_now_jst_strftime = datetime.now(ZoneInfo('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S %Z')
        
        # データ取得 #####################################
        with open(path_profiles_yaml) as file:
            profiles = yaml.safe_load(file)['profiles']
            profile_names = [profile['profile_name'] for profile in profiles]
            monthly_costs = [
                Boto3Driver(
                    aws_access_key_id = profile['aws_access_key_id'],
                    aws_secret_access_key = profile['aws_secret_access_key'], 
                    region_name = profile['region_name']
                ).get_monthly_cost(
                    metric = 'UnblendedCost',
                    metric_forecast = 'UNBLENDED_COST',
                )
                for profile in profiles
            ]

        # レポート作成 #####################################
        latest_month_cost_all = sum([i.latest_month_cost() for i in monthly_costs])
        latest_month = monthly_costs[0].latest_month()
        date_end_month = monthly_costs[0].date_end.isoformat()[:-3]
        name_report_base = f'aws-cost-report-{date_end_month}'
        path_report_resource_root = f'{path_output_root}/{name_report_base}'
        path_report = path_report_resource_root+'.md'

        try:
            shutil.rmtree(path_output_root)
        except OSError as err:
            print(err)
            pass
        os.makedirs(path_report_resource_root)

        text_md = f'# AWSコストレポート（{date_end_month}）\n'
        text_md += f'- レポート生成時刻 : {datetime_now_jst_strftime}\n'
        text_md += f"- {latest_month}の全アカウント合計コスト[USD] : ${round(latest_month_cost_all, 2)}\n"
        for profile_name, monthly_cost in zip(profile_names, monthly_costs):
            name_profile = profile_name
            name_bar_png=f'{name_profile}.png'
            monthly_cost.export_df_csv(f'{path_report_resource_root}/{name_profile}.csv')
            monthly_cost.export_df_bar_png(f'{path_report_resource_root}/{name_bar_png}')
            text_md += f'## {name_profile}\n'
            text_md += monthly_cost.cost_md(
                section='###',
                path_df_bar_png=f'./{name_report_base}/{name_bar_png}'
            )
        with open(path_report, mode='w') as f:
            f.write(text_md)

    def print_assume_role_pyathena_select(self, s3_staging_dir, work_group, query, path_profile_yaml='./profile.yml'):
        boto3_driver = Boto3Driver.create_driver_from_profile_yaml(path_profile_yaml).create_driver_from_profile_yaml_assume_role(path_profile_yaml)
        cursor = boto3_driver.athena_cursor(
            s3_staging_dir=s3_staging_dir,
            work_group=work_group,
        )
        cursor.execute(query)
        print(cursor.fetchall())
    
    def print_pyathena_select(self, s3_staging_dir, work_group, query, path_profile_yaml='./profile.yml'):
        boto3_driver = Boto3Driver.create_driver_from_profile_yaml(path_profile_yaml)
        cursor = boto3_driver.athena_cursor(
            s3_staging_dir=s3_staging_dir,
            work_group=work_group,
        )
        cursor.execute(query)
        print(cursor.fetchall())

    def sandbox(self, path_profile_yaml='./profile.yml'):
        return