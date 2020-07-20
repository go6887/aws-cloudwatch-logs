import boto3
from datetime import datetime, date, timedelta
import argparse

def get_log_streams(client, group_name, from_date=date.today(), to_date=date.today()):
    """関数の説明タイトル

        指定したロググループから指定した期間分のログストリームを取得

        Args:
            client: boto3.Session(profile_name='xxx') などaws cliのprofile
            group_name: CloudWatch LogsのLog groupsの名前
            from_date: 取得したいログの初日 デフォルトは当日
            to_date: 取得したいログの最終日 デフォルトは当日

        Returns:
           戻り値の型: cloudwatchのログ
    """
    next_token = None
    day_diff = (to_date - from_date).days + 1
    result = []
    
    for i in range(day_diff) :
        target_date = from_date + timedelta(i)
        while 1:
            if next_token is not None and next_token != '':
                response = client.describe_log_streams(
                            logGroupName=group_name,
                            logStreamNamePrefix = target_date.strftime("%Y/%m/%d"),
                            descending=True,
                            nextToken=next_token
                            )
            else:
                response = client.describe_log_streams(
                            logGroupName=group_name,
                            logStreamNamePrefix = target_date.strftime("%Y/%m/%d"),
                            descending=True,
                            )
            result += get_log_events(client, response, group_name)  
              
            if 'nextToken' not in response:
                break
            else:
                next_token = response['nextToken']
                
    return result


def get_log_events(client, response, group_name):
    """関数の説明タイトル

        指定したストリームのLog eventsを取得

        Args:
            client: boto3.Session(profile_name='xxx') などaws cliのprofile
            group_name:client.describe_log_streamsのresponse
            group_name: CloudWatch LogsのLog groupsの名前
    """
    result = []
    for stream in response['logStreams']:
        stream_name = stream['logStreamName']        

        # ログを取得
        logs = client.get_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            startFromHead=True
        )
        
        result += logs['events']
        while True:
            prev_token = logs['nextForwardToken']
            logs = client.get_log_events(
                logGroupName=group_name,
                logStreamName=stream_name,
                nextToken=prev_token)
            result += logs['events']
            
            if logs['nextForwardToken'] == prev_token:
                break
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get CloudWatch Logs')

    # hyper-parameter
    parser.add_argument('--from_date', default=date.today().strftime("%Y-%m-%d"), type=str, help='aggregates from, with format YYYY-MM-DD')
    parser.add_argument('--to_date', default=date.today().strftime("%Y-%m-%d"), type=str, help='aggregates to, with format YYYY-MM-DD')
    parser.add_argument('--group_name', type=str, help='Log groups Name')
    parser.add_argument('--profile', default='default', type=str, help='aws profile name to access')
    
    args = parser.parse_args()

    try:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")

    # AWSセッション
    session = boto3.Session(profile_name=args.profile)

    # ロググループ名
    group_name = args.group_name

    # ログストリーム一覧を取得
    client = session.client('logs')

    result = get_log_streams(client, group_name, from_date, to_date)

    print(result)