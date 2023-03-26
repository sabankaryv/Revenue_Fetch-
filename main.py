import os
from multiprocessing import Pool
import subprocess
import mysql.connector as a
import requests
import json
import pprint
import datetime
import datetime
from datetime import date
from datetime import timedelta
from datetime import datetime
import mysql.connector as a
import ipaddress
import DecodingConversions
import re
from threading import Thread
import everflow_rev

from datetime import datetime

now = datetime.now()
pid=19
attempt=1
status=1
e_status=2
current_time = now.strftime("%Y-%m-%d %H:%M:%S")
sdate = date.today()

mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()


# status_query="INSERT INTO DailyProcess_Status(pid,sdate,stime,attempt,status) VALUES(%s, %s, %s, %s,%s) ON DUPLICATE KEY UPDATE attempt=attempt+1;"
# n=cursor.execute(status_query, (pid,sdate,current_time,attempt,status))
# mydb.commit()


subprocess.run('python3 cake_rev_insertion.py && python3 hasoffers_rev.py  && python3 w4_rev.py  && python3 everflow_rev.py && python3 hitpath_rev.py', shell=True)
#subprocess.run('python3 everflow_rev_1.py', shell=True)
#subprocess.run('python3 everflow_rev.py', shell=True)
everflow_rev.read_csv()
# dir_name = "/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update_bkp/csv_files/"
# test = os.listdir(dir_name)
# for item in test:
#     if item.endswith(".txt"):
#         os.remove(os.path.join(dir_name, item))
print("script Completed successfully....")

# status_query_update = "UPDATE DailyProcess_Status SET etime = %s,status=%s WHERE pid = %s and sdate=%s"
# val = (current_time,e_status,pid,sdate)
# cursor.execute(status_query_update,val)
# mydb.commit()







