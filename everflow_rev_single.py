import requests
import json
from datetime import date
from datetime import datetime, timedelta
import pprint
import mysql.connector as a
import ipaddress
import DecodingConversions
import re
import os
import csv
import sys
import io
import fnmatch
from requests.auth import HTTPBasicAuth
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
end_date = str(yesterday)

subid_2=""
conversion_id=0
subid_2=''
# start_date ='2023-01-18'
# end_date ='2023-01-18'
mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()






response = requests.get('http://offers.codemskyapp.com/offersTool/offer_api/Offerapi/offerInfoApi?api_key=bdevrevrebrehresewer&require=AllPlatformWise&sponsor=Everflow&status=all', auth=HTTPBasicAuth('ims_user', 'oD1W$paXo'))
response = response.text
response= json.loads(response)
Everflow=response['Everflow']
for i in range(len(Everflow)):
	spid=Everflow[i]['spid']
	spon=Everflow[i]['sponsor']
	affiliate_id=Everflow[i]['affiliate_id']
	api_key=Everflow[i]['apikey']
	url=Everflow[i]['account_url']
	if(spon=="MadrivoEverflow_02" or spon=="MadrivoEverflow_01"):
		link ='https://api.eflow.team/v1/affiliates/reporting/entity/table/export'
		json_data='{"autoRun":"false","from": "'+start_date+'","to": "'+start_date+'","timezone_id": 90,"currency_id": "USD","columns": [{"column":"offer"},{"column":"sub1"},{"column":"sub2"},{"column":"sub3"},{"column":"sub4"},{"column":"sub5"},{"column":"sub5"}],"format":"csv"}'
		headers = {'content-type': 'application/json',"X-Eflow-API-Key" : api_key}
		api_everflow = requests.post(link, json_data, headers=headers)
		raw_data=api_everflow.text
		original_stdout = sys.stdout
		with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/csv_files/'+spon+'.txt', 'w') as f:
			f.write(raw_data)
	else:
		link ='https://api.eflow.team/v1/affiliates/reporting/entity/table/export'
		json_data='{"autoRun":"false","from": "'+start_date+'","to": "'+start_date+'","timezone_id": 80,"currency_id": "USD","columns": [{"column":"offer"},{"column":"sub1"},{"column":"sub2"},{"column":"sub3"},{"column":"sub4"},{"column":"sub5"},{"column":"sub5"}],"format":"csv"}'
		headers = {'content-type': 'application/json',"X-Eflow-API-Key" : api_key}
		api_everflow = requests.post(link, json_data, headers=headers)
		raw_data=api_everflow.text
	#	print(spon+"        "+raw_data)
		original_stdout = sys.stdout
		with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/csv_files/'+spon+'.txt', 'w') as f:
			f.write(raw_data)

	