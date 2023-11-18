from server.videoEncodingPullServer import EncodingServer
from dotenv import load_dotenv
import os

load_dotenv()

pushServerHost = os.getenv("PUSHSERVER_HOST")
pushServerPort = os.getenv("PUSHSERVER_PORT")
videoPath = os.getenv("VIDEO_PATH")

server = EncodingServer(pushServerHost, pushServerPort, videoPath)
server.run()