Simple Script to do batch object detection task in videos. Before begin to use this script, please configure aws access key and secret key in .aws folder located in your home directory. 
Put the original video inside original_video folder. The program will give 2 results : 1. Summary of detection in .csv file and 2. Modified videos with detected objects. Both results are stored inside the video_result folder. 

How to use the script : 
python main.py --labels (list of labels available in AWS Rekognition bounding box labels , separated by whitespace)

Example : 
python main.py --labels Gun Male "Basketball (Ball)" Car 


