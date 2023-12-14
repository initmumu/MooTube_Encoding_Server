import zmq
import threading
import queue
import os
import ffmpeg
import json
import time

from repository.PostgreSQLVideo import PostgreSQLVideo

class EncodingServer:
    def __init__(self, host, port, video_path):
        self.HOST = host
        self.PORT = port

        self.VIDEO_PATH = video_path

        self.formats = [
            {"resolution": "640x360", "video_bitrate": "500k", "audio_bitrate": "96k"},
            {"resolution": "1280x720", "video_bitrate": "1500k", "audio_bitrate": "128k"},
            {"resolution": "1920x1080", "video_bitrate": "3000k", "audio_bitrate": "192k"},
        ]

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)
        self.socket.connect(f"tcp://{self.HOST}:{self.PORT}")

        self.requestQueue = queue.Queue()

        self.processThread = threading.Thread(target=self.processEncoding)
        self.processThread.daemon = True
        self.processThread.start()
        print(f"[INFO] Encoding Thread is ready")

    def getWorker(self):
        return self.processThread
        
    def monitorThread(self, thread, name = "FFMPEG Encoding"):
        while True:
            if not thread.is_alive():
                print(f"{name} Thread died, restarting")
                newThread = threading.Thread(target=self.processEncoding)
                thread = newThread
                thread.start()
            time.sleep(10)

    def run(self):
        monitor = threading.Thread(target=self.monitorThread, args=(self.getWorker(),))
        monitor.daemon = True
        monitor.start()
        print(f"[INFO] Monitor Thread is ready")
        print(f"[INFO] Server is ready")

        while True:
            videoInfo = json.loads(self.socket.recv().decode('utf-8'))
            print(f"[LOG] Encoding Request is comming about video {videoInfo}")
            self.requestQueue.put(videoInfo)

    def encodeToHLS(self, videoInfo, resolution, videoBitrate, audioBitrate):
        resolutionList = resolution.split('x')

        
        curWork = resolutionList.pop()
        width = resolutionList.pop()

        INPUT_VIDEO_PATH = f"{self.VIDEO_PATH}/{videoInfo['video_id']}/original/"
        OUTPUT_VIDEO_PATH = f"{self.VIDEO_PATH}/{videoInfo['video_id']}/encoded/{curWork}p/"

        if not os.path.exists(INPUT_VIDEO_PATH):
            print(f"[ERROR] Video {videoInfo['video_id']} does not exist.")
            return

        if not os.path.exists(OUTPUT_VIDEO_PATH):
            os.makedirs(OUTPUT_VIDEO_PATH)

        # 영상 인코딩
        ( 
            ffmpeg.input(INPUT_VIDEO_PATH + f"{videoInfo['video_id']}.{videoInfo['fileExt']}")
            # .filter('scale', int(width), int(curWork))
                .output(OUTPUT_VIDEO_PATH + f"{curWork}p.m3u8",
                        format="hls",
                        start_number=0,
                        hls_time=10,
                        hls_list_size=0,
                        vf=f"scale={width}:-1",
                        acodec="aac",
                        # video_bitrate=videoBitrate,
                        # audio_bitrate=audioBitrate,
                        hls_segment_filename=OUTPUT_VIDEO_PATH + f"{videoInfo['video_id']}_%03d.ts",
                        )
                .run()
        )

    def extractThumbnail(self, videoInfo):
        INPUT_VIDEO_PATH = f"{self.VIDEO_PATH}/{videoInfo['video_id']}/original/"
        OUTPUT_VIDEO_PATH = f"{self.VIDEO_PATH}/{videoInfo['video_id']}/encoded/"

        # 썸네일 추출
        ( 
            ffmpeg.input(INPUT_VIDEO_PATH + f"{videoInfo['video_id']}.{videoInfo['fileExt']}", t =3)
                .filter('fps', fps=10, round='up')
                .output(OUTPUT_VIDEO_PATH + f"{videoInfo['video_id']}.gif",
                        loop=0
                        )
                .run()
        )

    def writeMasterPlaylist(self, videoInfo):
        OUTPUT_VIDEO_PATH = f"{self.VIDEO_PATH}/{videoInfo['video_id']}/encoded/"
        with open(f"{OUTPUT_VIDEO_PATH}/{videoInfo['video_id']}.m3u8", "w") as mp:
            mp.write("#EXTM3U\n")
            for f in self.formats:
                height = f["resolution"].split('x').pop()
                videoBitrate = f['video_bitrate']
                outputDir = f"{height}"

                mp.write(f"#EXT-X-STREAM-INF:BANDWIDTH={int(videoBitrate[:-1]) * 1000}\n")
                mp.write(f"{outputDir}p/{height}p.m3u8\n")

    def processEncoding(self):
        while True:
            videoInfo = self.requestQueue.get()
            self.requestQueue.task_done()

            for f in self.formats:
                resolution = f["resolution"]
                videoBitrate = f["video_bitrate"]
                audioBitrate = f["audio_bitrate"]
                self.encodeToHLS(videoInfo, resolution, videoBitrate, audioBitrate)

            self.extractThumbnail(videoInfo)
            self.writeMasterPlaylist(videoInfo)

            PostgreSQLVideo.updateVideoStatus(videoInfo['video_id'], "Published")
