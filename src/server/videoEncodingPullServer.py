import zmq
import threading
import queue
import os
import ffmpeg

from repository.PostgreSQLVideo import PostgreSQLVideo

class EncodingServer:
    def __init__(self, host, port, video_path):
        self.HOST = host
        self.PORT = port

        self.VIDEO_PATH = video_path

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)
        self.socket.connect(f"tcp://{self.HOST}:{self.PORT}")

        self.requestQueue = queue.Queue()

        processThread = threading.Thread(target=self.processEncoding)
        processThread.daemon = True
        processThread.start()


    def run(self):
        while True:
            videoID = self.socket.recv().decode('utf-8')
            print(f"[LOG] Encoding Request is comming about video {videoID}")
            self.requestQueue.put(videoID)

    def processEncoding(self):
        while True:
            videoId = self.requestQueue.get()

            INPUT_VIDEO_PATH = f"{self.VIDEO_PATH}/{videoId}/original/"
            OUTPUT_VIDEO_PATH = f"{self.VIDEO_PATH}/{videoId}/encoded/"
            print(INPUT_VIDEO_PATH)
            if not os.path.exists(INPUT_VIDEO_PATH):
                print(f"[ERROR] Video {videoId} does not exist.")
                continue

            if not os.path.exists(OUTPUT_VIDEO_PATH):
                os.makedirs(OUTPUT_VIDEO_PATH)

            ( 
                ffmpeg.input(INPUT_VIDEO_PATH + f"{videoId}.mp4")
                    .output(OUTPUT_VIDEO_PATH + f"{videoId}.m3u8",
                            codec='copy',
                            hls_segment_filename=OUTPUT_VIDEO_PATH + f'{videoId}_%03d.ts',
                            start_number=0,
                            hls_time=10,
                            hls_list_size=0,
                            format="hls"
                            )
                    .run()
            )

            PostgreSQLVideo.updateVideoStatus(videoId, "Published")

            self.requestQueue.task_done()