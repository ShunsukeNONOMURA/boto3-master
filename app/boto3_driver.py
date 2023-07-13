import yaml
import json
import pandas as pd
import matplotlib.pyplot  as plt
import boto3
from botocore.client import Config
from dateutil import relativedelta
from datetime import date, datetime, timedelta

from pprint import pprint

class MonthlyCost():
    def __init__(
            self,
            creation_datetime, # 作成日
            metric, # 集計コスト算出方法
            metric_forecast, # 予想コスト算出方法
            date_start, # 集計開始日
            date_end, # 集計終了日（指定日の1日前まで）
            df, # 集計結果全体
            forcast, # コスト予想     
            ):
        self.creation_datetime = creation_datetime
        self.metric = metric
        self.metric_forecast = metric_forecast
        self.date_start = date_start
        self.date_end = date_end
        self.df = df
        self.forcast = forcast

    def df_simple(self):
        df = self.df
        top_number = 10# aws_cost_top_service_number # n = 10
        top_services = df.groupby('Service')['Cost'].sum().nlargest(top_number).index
        df_top = df[df['Service'].isin(top_services)]
        df_other = df[~df['Service'].isin(top_services)]
        df_other = df_other.groupby('Month')['Cost'].sum().reset_index()
        df_other['Service'] = 'Others'
        df_simple = pd.concat([df_top, df_other])
        return df_simple.sort_values(by=['Month', 'Cost'], ascending=[False, False])
    
    def df_simple_last_month(self):
        df = self.df_simple()
        return df[df['Month']==self.latest_month()].loc[:,['Service','Cost']]
    
    def secondary_latest_month_cost(self):
        df = self.df.copy()
        df['Month'] = pd.to_datetime(df['Month'])
        latest_month = df['Month'].max()
        one_month_before = latest_month - pd.DateOffset(months=1)
        return df[df['Month'] == one_month_before]['Cost'].sum()

    def latest_month(self):
        return self.df['Month'].max()
    
    def latest_month_cost(self):
        return self.df[self.df['Month']==self.latest_month()]["Cost"].sum()
    
    def latest_month_cost_gap(self):
        return self.latest_month_cost() - self.secondary_latest_month_cost()
    
    def latest_month_cost_ratio(self):
        return self.latest_month_cost() / self.secondary_latest_month_cost() - 1
    
    def total_cost(self):
        return self.df["Cost"].sum()
    
    def average_cost(self):
        return self.total_cost() / self.df['Month'].nunique()

    def service_count(self):
        return self.df.groupby('Service')['Cost'].sum().shape[0]
    
    def export_df_csv(self, path):
        self.df.to_csv(path, index = False)

    def export_df_bar_png(self, path):
        """
        アルファベット順になる
        21種類以上の場合は色がループするので適当に調節すること
        boto3_driver.export_df_bar_png('./out/bar.png', result_cost['df'])
        """
        df_graph = self.df_simple()
        figsize=(10, 8)
        df_pivot = df_graph.pivot(index='Month', columns='Service', values='Cost')
        ax = df_pivot.plot.bar(
            stacked=True, 
            figsize=figsize,
            color=plt.get_cmap("tab20").colors # 20以降で色ループ
        )
        ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1), ncol=3)
        plt.ylabel('Cost ($)')
        plt.show()
        plt.savefig(path)

    
    def cost_md(
            self, 
            section,
            path_df_bar_png,
        ):
        md_text = ''
        latest_month = self.latest_month()
        table_dict = {
            f"集計方法" : f"{self.metric}",
            f"{latest_month}のコスト[USD]" : f"${round(self.latest_month_cost(), 2)}",
            # f"{date_end} last のコスト[USD]" : f"${round(self.secondary_latest_month_cost(), 2)}",
            f"{latest_month}のコスト変動[USD]" : f"${round(self.latest_month_cost_gap(), 2)}",
            f"{latest_month}のコスト変動率[％]" : f"{round(self.latest_month_cost_ratio(), 2) * 100}％",
            f"{latest_month}のコスト予測[USD]" : f"${round(self.forcast, 2) if self.forcast is not None else ''}",
            f"集計期間の合計コスト[USD]" : f"${round(self.total_cost(), 2)}",
            f"集計期間の1カ月当たりの平均コスト[USD]" : f"${round(self.average_cost(), 2)}",
            f"集計期間のサービスカウント" : f"{self.service_count()}",
        }
        df = pd.DataFrame.from_dict(table_dict, orient='index', columns=['内容'])
        df.index.name = '項目'
        md_text+=f"{section} 基本情報\n"
        md_text+=df.to_markdown()+'\n'

        # 月別コスト
        md_text+=f"{section} 月別コスト\n![]({path_df_bar_png})\n"

        md_text+=f"{section} {latest_month}コスト内訳\n"
        md_text+=self.df_simple_last_month().round(2).to_markdown(index=False)

        md_text+='\n'
        return md_text
    
    def export_cost_md(
            self, 
            path, 
            section, 
            path_df_bar_png,
        ):
        """
        # サンプル
        monthly_cost.export_df_bar_png('./out/bar.png')
        text_md = '## title\n'
        text_md += monthly_cost.cost_md(
            section='###',
            path_df_bar_png='./bar.png'
        )
        with open('./out/cost.md', mode='w') as f:
            f.write(text_md)
        """
        with open(path, mode='w') as f:
            f.write(self.cost_md(section, path_df_bar_png))

class SSMParameters():
    def __init__(self, data):
        self.df = pd.DataFrame(data)
    def export_df_csv(self, path):
        self.df.to_csv(path, index = False)

class Boto3Driver():
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

    @staticmethod
    def create_driver_from_profile_yaml(path_profile_yaml):
        with open(path_profile_yaml) as file:
            profile = yaml.safe_load(file)
        return Boto3Driver(
            aws_access_key_id = profile['aws_access_key_id'],
            aws_secret_access_key = profile['aws_secret_access_key'], 
            region_name = profile['region_name']
        )

    def session(self):
        return boto3.Session(
            aws_access_key_id = self.aws_access_key_id,
            aws_secret_access_key = self.aws_secret_access_key,
            region_name = self.region_name,
        )
    
    def get_s3_bucket_list(self):
        """
        s3のバケットを一覧
        boto3_driver.get_ec2_list()
        """
        client = self.session().client('s3')
        response = client.list_buckets()
        return response

    def get_s3_object_list(self, bucket_name, prefix=None):
        client = self.session().client('s3')
        contents = []
        args={
            'Bucket': bucket_name
        }
        if prefix is not None:
            args['Prefix'] = prefix
        while True:
            response = client.list_objects_v2(**args)
            if response['KeyCount'] == 0:
                break
            contents += response['Contents']
            if 'NextContinuationToken' not in response:
                break
            args['ContinuationToken'] = response['NextContinuationToken']
        return contents

    def create_s3_url(self, bucket_name, key, file_name = None, expires_in=3600):
        client = self.session().client('s3', config=Config(signature_version='s3v4'))
        params = {
            'Bucket': bucket_name, 
            'Key': key,
        }
        if file_name is not None:
            params['ResponseContentDisposition'] = f'attachment; filename="{file_name}"'
        return client.generate_presigned_url(
            'get_object', 
            Params=params, 
            ExpiresIn=expires_in,
            HttpMethod = 'GET',
        )

    def get_ec2_list(self):
        """
        ec2インスタンスを一覧
        boto3_driver.get_s3_bucket_list()
        """
        client = self.session().client('ec2')
        response = client.describe_instances()
        return response
    
    def get_ssm_parameters(self):
        """
        利用可能なもの
        https://docs.aws.amazon.com/cli/latest/reference/ssm/describe-parameters.html
        https://docs.aws.amazon.com/cli/latest/reference/ssm/get-parameters.html
        https://docs.aws.amazon.com/cli/latest/reference/ssm/get-parameter.html
        https://docs.aws.amazon.com/cli/latest/reference/ssm/get-parameters-by-path.html
        """
        client = self.session().client('ssm')
        args = {}
        next_token = None
        parameters_all=[]
        while True:
            if next_token is not None:
                args["NextToken"] = next_token
            response = client.describe_parameters(**args)
            
            next_token = response['NextToken'] if 'NextToken' in response else None
            parameters = response['Parameters']

            parameters_g = client.get_parameters(
                Names=[p['Name'] for p in parameters],
                WithDecryption=True
            )['Parameters']

            for p, p_g in zip(parameters, parameters_g):
                if p['Name'] == p_g['Name']:
                    p['Value'] = p_g['Value']
                    p['ARN'] = p_g['ARN']
                else:
                    print('Name Unmatch')

            # pprint(parameters[0])
            # pprint(parameters_g[0])

            parameters_all += parameters

            # 最新のログイベントを取得するまでループする
            print(f'件数: {len(parameters)}')
            # print(next_token)

            if len(parameters) == 0 or next_token is None:
                break

        return SSMParameters(parameters_all)
    
    def filter_log_events(
            self, 
            log_group_name,
            limit=10000,
            filter_pattern=None,
            start_datetime=None, # datetime
            end_datetime=None, # datetime
            ): 
        args = {
            "logGroupName": log_group_name,
            "limit":limit, # デフォルトが10000らしい
        }
        
        if filter_pattern is not None:
            args["filterPattern"] = filter_pattern
        if start_datetime is not None:
            args["startTime"] = int(start_datetime.timestamp() * 1000)
        if end_datetime is not None:
            args["endTime"] = int(end_datetime.timestamp() * 1000)
        
        client = self.session().client('logs')
        log_events_all = []
        log_events = []
        next_token = None

        while True:
            if next_token is not None:
                args["nextToken"] = next_token
            response = client.filter_log_events(**args)
            log_events = response['events']
            
            next_token = response['nextToken'] if 'nextToken' in response else None
            if len(log_events) == 0:
                break
            log_events_all += log_events

            # 最新のログイベントを取得するまでループする
            print(f'ログイベントの件数: {len(log_events)}')
            print(f"ログイベントの最初の時刻: {self.__change_milli_to_datetime(log_events[0]['timestamp'], is_jst=True)}")

        # 読みやすくするようの変換
        for l in log_events_all:
            l['datetime_jst'] = self.__change_milli_to_datetime(l['timestamp'], is_jst=True)
        return log_events_all
    
    def __change_milli_to_datetime(self, milli_seconds, is_jst=False):
        if is_jst:
            utc_datetime = datetime.fromtimestamp(milli_seconds / 1000)
            return_datetime = utc_datetime + timedelta(hours=9)
        else:
            return_datetime = datetime.fromtimestamp(milli_seconds / 1000)
        return return_datetime.strftime('%Y-%m-%d %H:%M:%S')

    def get_monthly_cost(
            self,
            metric,
            metric_forecast,
            aws_cost_top_service_number = 10,
            date_base=None, # date
        ):
        """
        base_date
            ・フォーカスする月日。
                ・defaultだと今日を基準日とする
            ・デフォルトの場合の振る舞い
                ・月初：先月末までのコストを計算（end_date=今日）
                ・それ以外：今月末までのコストを計算（end_date=来月初日）
                ・コスト予測はend_dateが来月初日の場合のみ機能
        """
        creation_datetime = datetime.now() # 現在時刻
        date_today = date.today() # 今日

        # コスト予測用
        date_this_month_first = date.today().replace(day=1) # 今月1日
        date_next_month_first = (date_this_month_first + relativedelta.relativedelta(months=1)).replace(day=1) # 来月1日
        
        # コスト集計用
        date_start = (date_this_month_first - relativedelta.relativedelta(years=1)).replace(day=1) # 今月1年前1日
        if date_base is None:
            date_base = date_today
        date_end =((date_base - relativedelta.relativedelta(days=1)) + relativedelta.relativedelta(months=1)).replace(day=1) # 今日の1日前の来月1日
        
        result = {
            'creation_datetime': creation_datetime,
            'metric': metric,
            'metric_forecast': metric_forecast,
            'date_start': date_start,
            'date_end': date_end,
            'df': None,
            'forcast': None,
        }
        client = self.session().client('ce')

        # 予想コスト(最新月と計測月が等しい場合は計算する)
        if date_end == date_next_month_first:
            response = client.get_cost_forecast(
                TimePeriod={
                    'Start': date_today.isoformat(),
                    'End': date_next_month_first.isoformat(),
                },
                Granularity='MONTHLY',
                Metric=metric_forecast
            )
            result['forcast'] = float(response['Total']['Amount'])

        # 集計コスト
        response = client.get_cost_and_usage(
            TimePeriod={
                'Start': date_start.isoformat(),
                'End': date_end.isoformat(),
            },
            Granularity='MONTHLY',
            Metrics=[metric],
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                },
            ]
        )
        data = []
        for r_i in response['ResultsByTime']:
            month = r_i['TimePeriod']['Start'][:-3]
            for group in r_i['Groups']:
                service = group['Keys'][0]
                cost = float(group['Metrics'][metric]['Amount'])
                data.append({'Month': month, 'Service': service, 'Cost': cost})
        df = pd.DataFrame(data)
        result['df'] =  df.sort_values(by=['Month', 'Cost'], ascending=[False, False])
        return MonthlyCost(**result)
