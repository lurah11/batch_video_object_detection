## Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# PDX-License-Identifier: MIT-0 (For details, see https://github.com/awsdocs/amazon-rekognition-developer-guide/blob/master/LICENSE-SAMPLECODE.)


import json
import sys
import time
import cv2
import datetime
import csv
import os

class VideoDetect:

    jobId = ''

    roleArn = ''
    bucket = ''
    video = ''
    startJobId = ''

    sqsQueueUrl = ''
    snsTopicArn = ''
    processType = ''

    def __init__(self, role, bucket, video, client, rek, sqs, sns, labels, original_video_dir, result_video_dir):
        self.roleArn = role
        self.bucket = bucket
        self.video = video
        self.client = client
        self.rek = rek
        self.sqs = sqs
        self.sns = sns
        self.labels = labels
        self.original_video_dir = original_video_dir
        self.result_video_dir = result_video_dir

    def GetSQSMessageSuccess(self):

        jobFound = False
        succeeded = False

        dotLine = 0
        while jobFound == False:
            sqsResponse = self.sqs.receive_message(QueueUrl=self.sqsQueueUrl, MessageAttributeNames=['ALL'],
                                                   MaxNumberOfMessages=10)

            if sqsResponse:

                if 'Messages' not in sqsResponse:
                    if dotLine < 40:
                        print('.', end='')
                        dotLine = dotLine + 1
                    else:
                        print()
                        dotLine = 0
                    sys.stdout.flush()
                    time.sleep(5)
                    continue

                for message in sqsResponse['Messages']:
                    notification = json.loads(message['Body'])
                    rekMessage = json.loads(notification['Message'])
                    print(rekMessage['JobId'])
                    print(rekMessage['Status'])
                    if rekMessage['JobId'] == self.startJobId:
                        print('Matching Job Found:' + rekMessage['JobId'])
                        jobFound = True
                        if (rekMessage['Status'] == 'SUCCEEDED'):
                            succeeded = True

                        self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                                                ReceiptHandle=message['ReceiptHandle'])
                    else:
                        print("Job didn't match:" +
                              str(rekMessage['JobId']) + ' : ' + self.startJobId)
                    # Delete the unknown message. Consider sending to dead letter queue
                    self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                                            ReceiptHandle=message['ReceiptHandle'])

        return succeeded

    def StartLabelDetection(self):
        response = self.rek.start_label_detection(Video={'S3Object': {'Bucket': self.bucket, 'Name': self.video}},
                                                  NotificationChannel={'RoleArn': self.roleArn,
                                                                       'SNSTopicArn': self.snsTopicArn},
                                                  MinConfidence=90,
                                                  Features=['GENERAL_LABELS'],
                                                  Settings={
                                                  'GeneralLabels': {
                                                  'LabelInclusionFilters': self.labels
                                                  }}
                                                   )

        self.startJobId = response['JobId']
        print('Start Job Id: ' + self.startJobId)

    def GetLabelDetectionResults(self):
        maxResults = 10
        paginationToken = ''
        finished = False
        resultList=[]

        while finished == False:
            response = self.rek.get_label_detection(JobId=self.startJobId,
                                                    MaxResults=maxResults,
                                                    NextToken=paginationToken,
                                                    SortBy='TIMESTAMP',
                                                    AggregateBy="TIMESTAMPS")

            try: 
                for labelDetection in response['Labels']:
                    result = {}
                    label = labelDetection['Label']['Name']
                    timestamp = labelDetection['Timestamp']
                    instances = labelDetection['Label']["Instances"]
                    
                    if instances == []: 
                        continue 
                    result["label"] = label
                    result["timestamp"] = timestamp
                    result["boundingBox"] = {}
                    for instance in instances:
                        result["confidence"] = instance["Confidence"]
                        result["boundingBox"]["width"] = instance["BoundingBox"]["Width"]
                        result["boundingBox"]["height"] = instance["BoundingBox"]["Height"]
                        result["boundingBox"]["left"] = instance["BoundingBox"]["Left"]
                        result["boundingBox"]["top"] = instance["BoundingBox"]["Top"]    

                    resultList.append(result)           
                
            except Exception as e : 
                resultList = []
                print(e)
            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                finished = True
        print(resultList)
        self.saveToVideo(resultList)
        self.write_detection_result_to_disk(resultList)

    def write_detection_result_to_disk(self, responses): 
        file_path = f"{self.result_video_dir}/object_detection.summary.csv"
        file_exists = os.path.exists(file_path)

        with open(file_path, mode='a', newline='') as file:
            field_names = ['video_name'] + self.labels
            writer = csv.DictWriter(file, fieldnames=field_names)
            
            # If file does not exist, write header
            if not file_exists:
                writer.writeheader()
            
            # Prepare data for the current row
            row_data = {
                'video_name': self.video
            }
            for label in self.labels:
                detected = False
                for response in responses :
                    if label == response['label']: 
                        detected = True
                        break
                row_data[label] = "detected" if detected else "not detected"
                
            
            # Write the current row to the CSV file
            writer.writerow(row_data)
    
    def saveToVideo(self,responses): 
        print("saving the video to video_result folder")
        start_time = datetime.datetime.now()        

        video_capture = cv2.VideoCapture(f"{self.original_video_dir}/{self.video}")

        # Get video properties
        frame_width = int(video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_height = int(video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = int(video_capture.get(cv2.CAP_PROP_FPS))

        # Define the codec and create VideoWriter object
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        output_video = cv2.VideoWriter(f'{self.result_video_dir}/{self.video}', fourcc, fps, (frame_width, frame_height))
        print("try to writing frame to video.... the speed will depends on your video duration")
        # Iterate through frames
        while True:
            # Read a frame
            ret, frame = video_capture.read()
            if not ret:
                break

            # Get current frame timestamp in milliseconds
            current_time_ms = int(video_capture.get(cv2.CAP_PROP_POS_MSEC))
            bbox_duration = 300
            
            # Iterate through responses and draw bounding boxes
            for response in responses:
                timestamp_ms = response["timestamp"]
                if current_time_ms >= timestamp_ms and current_time_ms <= timestamp_ms + bbox_duration:
                    label = response["label"]
                    confidence = response.get("confidence")
                    bounding_box = response.get("boundingBox")  # Check if bounding box exists
                    if bounding_box:
                        # Convert bounding box coordinates from relative to absolute values
                        left = int(bounding_box["left"] * frame_width)
                        top = int(bounding_box["top"] * frame_height)
                        right = int((bounding_box["left"] + bounding_box["width"]) * frame_width)
                        bottom = int((bounding_box["top"] + bounding_box["height"]) * frame_height)

                        # Draw bounding box
                        cv2.rectangle(frame, (left, top), (right, bottom), (0, 255, 0), 2)

                        # Put label and confidence
                        label_text = f"{label} (Confidence: {confidence:.2f})"
                        cv2.putText(frame, label_text, (left, top - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

            # Write the frame to the output video
            output_video.write(frame)
            


            # Break the loop if 'q' is pressed
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

            # Update current_time_ms for the next iteration
            current_time_ms = int(video_capture.get(cv2.CAP_PROP_POS_MSEC))

            # Remove the responses that have been processed
            responses = [response for response in responses if response["timestamp"] + bbox_duration >= current_time_ms]

        # Release video capture and writer
        video_capture.release()
        output_video.release()
        cv2.destroyAllWindows()   
        end_time = datetime.datetime.now()  
        time_delta = end_time-start_time
        print(f"Video {str(self.video)} processing duration is {time_delta.seconds} seconds")


    def CreateTopicandQueue(self):

        millis = str(int(round(time.time() * 1000)))

        # Create SNS topic

        snsTopicName = "AmazonRekognitionExample" + millis

        topicResponse = self.sns.create_topic(Name=snsTopicName)
        self.snsTopicArn = topicResponse['TopicArn']

        # create SQS queue
        sqsQueueName = "AmazonRekognitionQueue" + millis
        self.sqs.create_queue(QueueName=sqsQueueName)
        self.sqsQueueUrl = self.sqs.get_queue_url(QueueName=sqsQueueName)['QueueUrl']

        attribs = self.sqs.get_queue_attributes(QueueUrl=self.sqsQueueUrl,
                                                AttributeNames=['QueueArn'])['Attributes']

        sqsQueueArn = attribs['QueueArn']

        # Subscribe SQS queue to SNS topic
        self.sns.subscribe(
            TopicArn=self.snsTopicArn,
            Protocol='sqs',
            Endpoint=sqsQueueArn)

        # Authorize SNS to write SQS queue
        policy = """{{
  "Version":"2012-10-17",
  "Statement":[
    {{
      "Sid":"MyPolicy",
      "Effect":"Allow",
      "Principal" : {{"AWS" : "*"}},
      "Action":"SQS:SendMessage",
      "Resource": "{}",
      "Condition":{{
        "ArnEquals":{{
          "aws:SourceArn": "{}"
        }}
      }}
    }}
  ]
}}""".format(sqsQueueArn, self.snsTopicArn)

        response = self.sqs.set_queue_attributes(
            QueueUrl=self.sqsQueueUrl,
            Attributes={
                'Policy': policy
            })

    def DeleteTopicandQueue(self):
        self.sqs.delete_queue(QueueUrl=self.sqsQueueUrl)
        self.sns.delete_topic(TopicArn=self.snsTopicArn)

