import requests
import json
from datetime import date
from datetime import datetime, timedelta
import pprint
import mysql.connector as a
import ipaddress
import DecodingConversions
import everflow_sponsor_fetch
import re
import os
import sys
import pandas as pd
from pandas.errors import EmptyDataError 
import numpy as np
import io
import fnmatch
import threading
from subprocess import *
headers = {
    'Accept': 'application/json',
    
}
conversion_id=0
subid_2=''
today_date = date.today()
yesterday = today_date - timedelta(days=1)
start_date = str(yesterday)
end_date = str(today_date)
subid_2=""
conversion_id=0
subid_2=''
# start_date ='2023-01-24'
# end_date ='2023-01-24'
mydb = a.connect(host="localhost", user="root", password="swineflu2009",database="Revfetch_5493")
cursor=mydb.cursor()



# try:
# df = pd.read_csv(file, delim_whitespace=True)
# except EmptyDataError:
# df = pd.DataFrame()

# everflow_sponsor = everflow_sponsor_fetch.everflowsponsor("Everflow")
# #print(len(everflow_sponsor))
# for i in range(len(everflow_sponsor)):
# 	url=everflow_sponsor[i]['url']
# 	api_key=everflow_sponsor[i]['affkey']
# 	spon=everflow_sponsor[i]['sponsName']
# 	affiliate_id=everflow_sponsor[i]['spons_id']
# url='https://api.eflow.team/v1/affiliates'
# api_key='TiwWt42OToKp16HmkRvitA'
# spon='SkynestAffiliates_01'
# affiliate_id='2'

# url='https://api.eflow.team/v1/affiliates'
# api_key='bHe5C26gQFmcxf9wtZ0w'
# spon='SkynestAffiliates_10'
# affiliate_id='10'

url='https://api.eflow.team/v1/affiliates'
api_key='USA38fh3QWey8CwxNVUYgQ'
spon='Bizaglo_01'
affiliate_id='2902'

link ='https://api.eflow.team/v1/affiliates/reporting/entity/table/export'
json_data='{"autoRun":"false","from": "'+start_date+'","to": "'+start_date+'","timezone_id": 80,"currency_id": "USD","columns": [{"column":"offer"},{"column":"sub1"},{"column":"sub2"},{"column":"sub3"},{"column":"sub4"},{"column":"sub5"},{"column":"sub5"}],"format":"csv"}'
headers = {'content-type': 'application/json',"X-Eflow-API-Key" : api_key}
api_everflow = requests.post(link, json_data, headers=headers)
raw_data=api_everflow.text
#print(spon+"        "+raw_data)
original_stdout = sys.stdout
with open('/var/www/html/yogesh/Revenue_Report/'+spon+'.txt', 'w') as f:
    sys.stdout = f
    print(raw_data)
    sys.stdout = original_stdout
    try:
    	df = pd.read_csv("/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/"+spon+".txt")
    	#df = pd.read_csv("/var/www/html/yogesh/Revenue_Report/SkynestAffiliates_10.txt")
    	for index, row in df.iterrows():
    		campaign_id=row['network_offer_id']
    		#print(campaign_id)
    		offer_name = row['offer_name']
    		offer_name=cursor._connection.converter.escape(offer_name)
    		subid_1=row['sub1']
    		subid_3=row['sub3']
    		subid_4=row['sub4']
    		subid_5=row['sub5']
    		revenue=row['revenue']
    		List=DecodingConversions.get_list(str(subid_3))
    		fecid=DecodingConversions.convert_fecid(str(subid_3))
    		subjectid=DecodingConversions.convert_subjectid(str(subid_3))
    		user_ip=DecodingConversions.get_userip(str(subid_5))
    		Entity=DecodingConversions.get_entity(str(subid_1))
    		isp=DecodingConversions.get_isp(str(subid_1))
    		rev_date=start_date
    		sponsor=spon
    		oid=campaign_id
    		conversion_id=conversion_id
    		offer_name=offer_name
    		campaign_id=campaign_id
    		subid_1=cursor._connection.converter.escape(subid_1)
    		subid_2=cursor._connection.converter.escape(subid_2)
    		subid_3=cursor._connection.converter.escape(subid_3)
    		subid_4=cursor._connection.converter.escape(subid_4)
    		subid_5=cursor._connection.converter.escape(subid_5)
    		rev=revenue
    		offerid=subid_4
    		List=List
    		isp=isp
    		fecid=fecid
    		subjectid=subjectid
    		ip=user_ip
    		Entity=Entity
    		user_ip=user_ip
    		temp='CPA'
    		sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
    		#print(sql+";")
    		with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/ever.txt', 'a') as f:
    			f.write(sql)
    except EmptyDataError:
    	df = pd.DataFrame()
