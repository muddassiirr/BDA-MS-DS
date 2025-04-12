import schedule
import time
import os

def job():
    os.system("python etl_pipeline.py")

schedule.every().day.at("10:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)
