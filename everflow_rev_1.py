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
# start_date ='2023-03-17'
# end_date ='2023-03-17'
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
# everflowsponsor =requests.get('http://ektun.improvearch.com/app/data/python_rev/all_sponsor.php?Everflow')
# everflow = everflowsponsor.text
# everflow= json.loads(everflow)
# for i in range(len(everflow)):
# 	url=everflow[i]['url']
# 	api_key=everflow[i]['affkey']
# 	spon=everflow[i]['sponsName']
# 	affiliate_id=everflow[i]['spons_id']
	if(spon=="MadrivoEverflow_02" or spon=="MadrivoEverflow_01"):
		link ='https://api.eflow.team/v1/affiliates/reporting/entity/table/export'
		json_data='{"autoRun":"false","from": "'+start_date+'","to": "'+start_date+'","timezone_id": 90,"currency_id": "USD","columns": [{"column":"offer"},{"column":"sub1"},{"column":"sub2"},{"column":"sub3"},{"column":"sub4"},{"column":"sub5"},{"column":"sub5"}],"format":"csv"}'
		headers = {'content-type': 'application/json',"X-Eflow-API-Key" : api_key}
		api_everflow = requests.post(link, json_data, headers=headers)
		raw_data=api_everflow.text
		original_stdout = sys.stdout
		print("Downloading file from sponsor "+str(spon))
		with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update_bkp/csv_files/'+spon+'.txt', 'w') as f:
			f.write(raw_data)
			# with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/csv_files/'+spon+'.txt', newline='') as csvfile:
			# 	data = csv.DictReader(csvfile)
			# 	for row in data:
			# 		campaign_id=row['network_offer_id']
			# 		offer_name = row['offer_name']
			# 		offer_name=cursor._connection.converter.escape(offer_name)
			# 		subid_1=row['sub1']
			# 		subid_3=row['sub3']
			# 		subid_4=row['sub4']
			# 		subid_5=row['sub5']
			# 		revenue=row['revenue']
			# 		List=DecodingConversions.get_list(str(subid_3))
			# 		fecid=DecodingConversions.convert_fecid(str(subid_3))
			# 		subjectid=DecodingConversions.convert_subjectid(str(subid_3))
			# 		user_ip=DecodingConversions.get_userip(str(subid_5))
			# 		Entity=DecodingConversions.get_entity(str(subid_1))
			# 		isp=DecodingConversions.get_isp(str(subid_1))
			# 		rev_date=start_date
			# 		sponsor=spon
			# 		oid=campaign_id
			# 		conversion_id=conversion_id
			# 		offer_name=offer_name
			# 		campaign_id=campaign_id
			# 		subid_1=cursor._connection.converter.escape(subid_1)
			# 		subid_2=cursor._connection.converter.escape(subid_2)
			# 		subid_3=cursor._connection.converter.escape(subid_3)
			# 		subid_4=cursor._connection.converter.escape(subid_4)
			# 		subid_5=cursor._connection.converter.escape(subid_5)
			# 		rev=revenue
			# 		offerid=subid_4
			# 		List=List
			# 		isp=isp
			# 		fecid=fecid
			# 		subjectid=subjectid
			# 		ip=user_ip
			# 		Entity=Entity
			# 		user_ip=user_ip
			# 		temp='CPA'
			# 		sql = """INSERT INTO Revfetch_5493.sponser_outpl_new(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
			# 		print(sql+";")
			#Yogii
	else:
		link ='https://api.eflow.team/v1/affiliates/reporting/entity/table/export'
		json_data='{"autoRun":"false","from": "'+start_date+'","to": "'+start_date+'","timezone_id": 80,"currency_id": "USD","columns": [{"column":"offer"},{"column":"sub1"},{"column":"sub2"},{"column":"sub3"},{"column":"sub4"},{"column":"sub5"},{"column":"sub5"}],"format":"csv"}'
		headers = {'content-type': 'application/json',"X-Eflow-API-Key" : api_key}
		api_everflow = requests.post(link, json_data, headers=headers)
		raw_data=api_everflow.text
	#	print(spon+"        "+raw_data)
		original_stdout = sys.stdout
		print("Downloading file from sponsor "+str(spon))
		with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update_bkp/csv_files/'+spon+'.txt', 'w') as f:
			f.write(raw_data)
			#Yogii
			# with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/csv_files/'+spon+'.txt', newline='') as csvfile:
			# 	data = csv.DictReader(csvfile)
			# 	for row in data:
			# 		campaign_id=row['network_offer_id']
			# 		offer_name = row['offer_name']
			# 		offer_name=cursor._connection.converter.escape(offer_name)
			# 		subid_1=row['sub1']
			# 		subid_3=row['sub3']
			# 		subid_4=row['sub4']
			# 		subid_5=row['sub5']
			# 		revenue=row['revenue']
			# 		List=DecodingConversions.get_list(str(subid_3))
			# 		fecid=DecodingConversions.convert_fecid(str(subid_3))
			# 		subjectid=DecodingConversions.convert_subjectid(str(subid_3))
			# 		user_ip=DecodingConversions.get_userip(str(subid_5))
			# 		Entity=DecodingConversions.get_entity(str(subid_1))
			# 		isp=DecodingConversions.get_isp(str(subid_1))
			# 		rev_date=start_date
			# 		sponsor=spon
			# 		oid=campaign_id
			# 		conversion_id=conversion_id
			# 		offer_name=offer_name
			# 		campaign_id=campaign_id
			# 		subid_1=cursor._connection.converter.escape(subid_1)
			# 		subid_2=cursor._connection.converter.escape(subid_2)
			# 		subid_3=cursor._connection.converter.escape(subid_3)
			# 		subid_4=cursor._connection.converter.escape(subid_4)
			# 		subid_5=cursor._connection.converter.escape(subid_5)
			# 		rev=revenue
			# 		offerid=subid_4
			# 		List=List
			# 		isp=isp
			# 		fecid=fecid
			# 		subjectid=subjectid
			# 		ip=user_ip
			# 		Entity=Entity
			# 		user_ip=user_ip
			# 		temp='CPA'
			# 		sql = """INSERT INTO Revfetch_5493.sponser_outpl_new(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
			# 		print(sql+";")