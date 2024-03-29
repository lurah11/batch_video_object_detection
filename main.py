import boto3
from detect import VideoDetect
from dotenv import load_dotenv
import os
import glob

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--labels', type=str, nargs='+', help='List of categories separated by whitespaces')
args = parser.parse_args()
labels = args.labels 

if not labels : 
    raise Exception("Please provide at least one label, i don't want to detect all those available labels in aws!")


load_dotenv('.env')
original_video_dir = "original_video"
result_video_dir = "video_result"

roleArn = os.getenv('REKOG_ROLE_ARN')
bucket = os.getenv('REKOG_BUCKET_NAME')


video_list = glob.glob('*.mp4',root_dir=original_video_dir)
session = boto3.Session(profile_name='default')
client = session.client('rekognition')
rek = boto3.client('rekognition')
sqs = boto3.client('sqs')
sns = boto3.client('sns')
s3 = boto3.client('s3')

for video in video_list : 
    print("Uploading file to S3.. please wait...")
    with open(f"{original_video_dir}/{video}", "rb") as f:
        s3.upload_fileobj(f,bucket,video)
    print(f"File {video} is Successfully uploaded")
    print("Object Detection Task is started, grab a cup of coffee while waiting")
    analyzer = VideoDetect(roleArn, bucket, video, client, rek, sqs, sns, labels, original_video_dir, result_video_dir)
    analyzer.CreateTopicandQueue()

    analyzer.StartLabelDetection()
    if analyzer.GetSQSMessageSuccess() == True:
        analyzer.GetLabelDetectionResults()

    analyzer.DeleteTopicandQueue()
    print("Supporting resources have been deleted")
    print(f"The task has finished for video {video}, check it inside video_result directory")

