from datetime import datetime
import time
from picamera2 import Picamera2, Preview
import os


def take_still_frame_image():
	
	"""
	take still frame image from raspberry pi and save to local directory
	"""
	picam = Picamera2()
	os.chdir('chicago-grow-bed-images/')
	config = picam.create_preview_configuration()
	picam.configure(config)


	curr_datetime = datetime.now().strftime('%Y-%m-%d %H-%M-%S')

	picam.start_preview(Preview.QTGL)

	picam.start()
	time.sleep(2)
	picam.capture_file(f"chicago-grow-bed-{curr_datetime}.jpg")

	picam.close()
	
if __name__ == "__main__":
	take_still_frame_image()
