"""
本工具是dump DynamoDB上关键列的数据，并存入本地文件
"""

import boto3
import os, time, csv
from operator import itemgetter

def get_ddb():
    result = []
    try:
        response = table.scan()
        if "Items" in response:
            result.extend(response['Items'])
        if "LastEvaluatedKey" in response:
            LastEvaluatedKey = response['LastEvaluatedKey']
            while LastEvaluatedKey:
                response = table.scan(
                    ExclusiveStartKey=LastEvaluatedKey
                )
                if "Items" in response:
                    result.extend(response['Items'])
                if "LastEvaluatedKey" in response:
                    LastEvaluatedKey = response['LastEvaluatedKey']
                else:
                    break
    except Exception as e:
        print(e)
    return result


def get_running(data):
    result = []
    for i in data:
        if 'lastTimeProgress' in i:
            if i['lastTimeProgress'] != 100:
                result.append(i)
    print('Total:', len(result), 'running')
    return result


def size_to_str(size):
    def loop(integer, remainder, level):
        if integer >= 1024:
            remainder = integer % 1024
            integer //= 1024
            level += 1
            return loop(integer, remainder, level)
        else:
            return integer, round(remainder / 1024, 1), level

    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    integer, remainder, level = loop(int(size), 0, 0)
    if level+1 > len(units):
        level = -1
    return f'{integer+remainder} {units[level]}'


def display(data, limit=10, mute=False):
    count = 0
    result = []
    for i in data:
        p = "    "
        if 'Size' in i:
            size_str = size_to_str(i['Size'])
            p += f"Size: \033[0;34;1m{size_str}\033[0m "
        else:
            i['Size'] = 0
        if 'firstTime_f' in i:
            p += f"firstTime: \033[0;34;1m{i['firstTime_f']}\033[0m "
        else:
            i['firstTime_f'] = 'NA'
        if 'lastTimeProgress' in i:
            p += f"lastTimeProgress: \033[0;34;1m{i['lastTimeProgress']}%\033[0m "
        else:
            i['lastTimeProgress'] = 'NA'
        if 'totalSpentTime' in i:
            p += f"totalSpentTime: \033[0;34;1m{i['totalSpentTime']}\033[0m "
        else:
            i['totalSpentTime'] = 'NA'
        if 'tryTimes' in i:
            p += f"tryTimes: \033[0;34;1m{i['tryTimes']}\033[0m "
        else:
            i['tryTimes'] = 'NA'
        if 'thisRoundStart_f' in i:
            p += f"thisRoundStart: \033[0;34;1m{i['thisRoundStart_f']}\033[0m "
        else:
            i['thisRoundStart_f'] = 'NA'
        if 'endTime_f' in i:
            p += f"endTime_f: \033[0;34;1m{i['endTime_f']}\033[0m "
        else:
            i['endTime_f'] = 'NA'
        if 'instanceID' in i:
            p += f"instanceID: {str(i['instanceID'])} "
        else:
            i['instanceID'] = 'NA'
        if 'jobStatus' in i:
            p += f"jobStatus: {str(i['jobStatus'])}"
        else:
            i['jobStatus'] = 'NA'
        result.append(i)
        if mute:
            continue
        print({i['Key']})
        print(p)
        count += 1
        if count > limit:
            break
    return result


# Main
if __name__ == '__main__':
    table_queue_name = "covid19-s3-migrate-serverless-covid19s3migrateddb4500B92E-A8EFI0ZPV1UE"
    src_session = boto3.session.Session(profile_name='iad')
    dynamodb = src_session.resource('dynamodb')
    table = dynamodb.Table(table_queue_name)

    file_path = os.path.split(os.path.abspath(__file__))[0]
    ddb_result_path = file_path + '/s3_migration_ddb_result'
    os.system('mkdir ' + ddb_result_path)
    this_file_name = os.path.splitext(os.path.basename(__file__))[0]
    t = time.localtime()
    start_time = f'{t.tm_year}-{t.tm_mon}-{t.tm_mday}-{t.tm_hour}-{t.tm_min}-{t.tm_sec}'
    ddb_result_filename = f'{ddb_result_path}/{this_file_name}-{start_time}.csv'

    '''
    MAIN
    '''
    # 整个表scan下来
    ddb_result = get_ddb()
    print("DDB records: ", len(ddb_result))

    # 查询正在进行中的Job
    query_running = get_running(ddb_result)
    display(query_running, 10)

    # 输出启动时间最晚的记录
    print("\nLast firstTime data:")
    query_descend = sorted(
        ddb_result, key=itemgetter('firstTime'), reverse=True)
    display(query_descend, 10)

    # 补缺数据，否则文件输出csv会在缺失数据的地方中断
    convert_data = display(ddb_result, mute=True)

    # 写入本地文件
    with open(ddb_result_filename, "w+") as f:
        csvwriter = csv.writer(f)
        csvwriter.writerow(['Key', 'Size', 'firstTime_f', 'lastTimeProgress', 'totalSpentTime', 'tryTimes',
                            'thisRoundStart_f', 'endTime_f', 'instanceID', 'jobStatus'])  # 写表头

        for i in ddb_result:
            csvwriter.writerow([i['Key'], i['Size'], i['firstTime_f'], i['lastTimeProgress'], i['totalSpentTime'],
                i['tryTimes'], i['thisRoundStart_f'], i['endTime_f'], str(i['instanceID']), str(i['jobStatus'])])
    print(f'\n Full data write to {ddb_result_filename}')
