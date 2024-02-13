import cv2
from ultralytics import YOLO

"""
Author: Michael Stack
Last Modified: 2/12/2024

YOLOv8 Object Tracker for Computer Vision videos. This python module will allow you to place your own video inside the 
'videos/' directory and replace the variable video_name in the Example Usage section with whatever video you want! This is 
using a pretrained model ('yolov8n.pt') where 'n' stands for nano, and .pt stands for 'pre-trained'.

 Ultralytics (the organization) that created and maintains yolov8 has a variety of pre-trained models, datasets, and object 
 classes to utilize. There is also good documentation on how to create your own dataset and train custom models here:
https://docs.ultralytics.com/modes/train/

For new feature requests (i.e. checking video types and converting them to .mp4 by default), please submit a git issue to the 
orangutan-stem GitHub Repo
"""

class ObjectTracker:
    def __init__(self, video_path_prefix, video_media_type='.mp4'):
        self.video_path = f'{video_path_prefix}{video_media_type}'
        self.model = YOLO('yolov8n.pt')
        self.cap = cv2.VideoCapture(self.video_path)
        
        if not self.cap.isOpened():
            print("Error: Unable to open video file.")
            exit()

    def track_objects(self):
        while True:
            ret, frame = self.cap.read()

            if not ret:
                print("End of video.")
                break

            try:
                results = self.model.track(frame, persist=True)

                # Plot results
                frame_ = results[0].plot()

                # Visualize
                cv2.imshow('frame', frame_)
                if cv2.waitKey(25) & 0xFF == ord('q'):
                    break
            except Exception as e:
                print("An error occurred:", e)
                break

        # Release video capture and close windows
        self.cap.release()
        cv2.destroyAllWindows()


video_name = 'wally'

# Example usage:
video_path_prefix = f'./videos/{video_name}'
tracker = ObjectTracker(video_path_prefix)
tracker.track_objects()


