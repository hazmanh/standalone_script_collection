import os
import boto3,json,time, datetime
import requests
import json
import logging
from datetime import date
import time
from collections import OrderedDict

def get_vpc_flowlogs():
    log_groups = '[]'
    log_streams = None #use '[stream1,stream2,stream3]' to specify streams
    mount_dir = ''
    start_time = ''
    end_time = ''
    time_pattern = '%d.%m.%Y %H:%M:%S'

    start_epoch = int(time.mktime(time.strptime(start_time, time_pattern)))
    end_epoch = int(time.mktime(time.strptime(end_time, time_pattern)))
    log_group_split = split_input_list(log_groups)

    try:
        for group_name in log_group_split:
            print('Log group: ' + group_name)
            all_streams = []
            output_log_file_name = os.path.join(mount_dir +'VPC_FLOWLOGS_' + str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + ".log")
            out_to = open(output_log_file_name, 'w')

            client = boto3.client('logs')
            stream_batch = client.describe_log_streams(logGroupName=group_name)
            all_streams += stream_batch['logStreams']
            while 'nextToken' in stream_batch:
                stream_batch = client.describe_log_streams(logGroupName=group_name,nextToken=stream_batch['nextToken'])
                all_streams += stream_batch['logStreams']
            
            print("Number of streams in the log group: " + str(len(all_streams)))
            stream_names = []
            
            if(log_streams is not None):
                all_streams = split_input_list(log_streams)

                for stream in all_streams:
                    stream_names.append(stream)
            else:
                for stream in all_streams:
                    stream_names.append(stream['logStreamName'])
            
            print("Number of streams to download from: " + str(len(stream_names)))

            for stream in stream_names:
                print("stream name - " + stream + " || start time - " + str(start_time) + " || end time - " + str(end_time))
                logs_batch = client.get_log_events(endTime= int(end_time), logGroupName=group_name, logStreamName=stream,  startFromHead =True, startTime = int(start_epoch))
                write_to_file(logs_batch, out_to, group_name, stream)

                #only prints out the streams with logs available
                if(len(logs_batch['events']) > 0):
                    print(str(stream) +  " has logs to be downloaded ")
                    
                total_logs = total_logs + len(logs_batch['events'])

                while 'nextForwardToken' in logs_batch:
                    current_token = logs_batch['nextForwardToken']						
                    logs_batch = client.get_log_events(logGroupName=group_name, logStreamName=stream, nextToken=current_token, startTime = int(start_epoch), endTime= int(end_epoch))
                    write_to_file(logs_batch, out_to, group_name, stream)
                    total_logs = total_logs + len(logs_batch['events'])
                    if current_token == logs_batch['nextForwardToken']:							
                        break
                print("Total logs downloaded in stream - " + stream + " = " + str(total_logs) )
                total_count += total_logs
                total_logs = 0
            out_to.close()
            print(" Log group - " + str(group_name)  + " || Output file -  " + str(output_log_file_name))

    except Exception as error:
       print('Exception raised: ' + str(error))

    finally:
        print("Total logs downloaded in this run - " + str(total_count))

def split_input_list(input_value):
    """
    used to split log groups and log streams
    """
    input_list = input_value.strip('[]').split(',')
    return input_list

def write_to_file(logs_batch, out_to, group_name, stream):
    data_to_write = OrderedDict()
    for event in logs_batch['events']:	
        data_to_write = event
        data_to_write['group'] = group_name
        data_to_write['stream'] = stream
        out_to.write(json.dumps(data_to_write) + '\n')


get_vpc_flowlogs()