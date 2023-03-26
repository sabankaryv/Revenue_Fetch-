import requests
import json
import pprint
import datetime
from datetime import date
from datetime import timedelta
from datetime import datetime
import mysql.connector as a
import ipaddress
import DecodingConversions
import re
from requests.auth import HTTPBasicAuth




StatusSquery="INSERT INTO DailyProcess_Status(pid,sdate,stime,attempt,status) VALUES(19,date(now()),now(),1,1) ON DUPLICATE KEY UPDATE attempt=attempt+1,stime=now()";
echo $StatusSquery | mysql -u root -pcmVwc3J2ZmVjMjAxMQ -D fecreports -N

sponsors=`echo "select spons_id from campaign.sponsors where active = 1 and spons_id not in ('vs','350586') " | mysql -u root -D fecreports -p'cmVwc3J2ZmVjMjAxMQ' -N`

#spons
# subid_2=''
# mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
# cursor=mydb.cursor()
# today_date = date.today()
# yesterday = today_date - timedelta(days=1)
# start_date = str(yesterday)
# end_date = str(yesterday)
# # start_date='2023-01-17'
# # end_date='2023-01-17'
# sort = 'False'
# conversion_id = '0'
# camplimit = 500
# campoffset=0
# """