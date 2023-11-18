import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
import os

load_dotenv()

class PostgreSQLVideo:
    connectionPool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv('POSTGRES_DATABASE'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
        )
    
    @staticmethod
    def updateVideoStatus(videoId, status):
        conn = PostgreSQLVideo.connectionPool.getconn()
        try:
            with conn.cursor() as cursor:
                updateQuery = 'UPDATE public.video SET status = %s WHERE video_id = %s'
                values = (status, videoId)
                cursor.execute(updateQuery, values)
                conn.commit()
        except Exception as e:
            print(e)