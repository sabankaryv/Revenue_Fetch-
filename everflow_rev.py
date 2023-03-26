import requests
import glob
import csv
import os
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
import threading
import db
import numpy as np
from requests.auth import HTTPBasicAuth
from subprocess import *
headers = {
    'Accept': 'application/json',
    
}


today_date = date.today()
yesterday = today_date - timedelta(days=1)
start_date = str(yesterday)
end_date = str(yesterday)



# start_date ='2023-03-03'
# end_date ='2023-03-03'
mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()

def get_row_count(spon):
    #print(spon)
    mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
    cursor=mydb.cursor()
    #query1="SELECT sponsor FROM fecreports.sponser_outpl_new1"
    # query1 = "DELETE FROM fecreports.sponser_outpl_new1 WHERE sponsor =%s"
    # val = (spon, )
    # cursor.execute(query1,val)
    # mydb.commit()
    # print(cursor.rowcount, "record(s) deleted")

    query="SELECT sponsor,COUNT(*) FROM fecreports.sponser_outpl_new1 WHERE sponsor=%s and rev_date=%s"
    val = (spon,start_date, )
    cursor.execute(query,val)
    col=cursor.fetchall()
    for x in col:
        #print("Sponsor is "+str(x[0])+" count is "+str(x[1]))
        sponsor=x[0]
        count=x[1]
    # print(sponsor)
    # print(count)
    if count>0:
        # print(sponsor)
        # print(count)
        mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
        cursor=mydb.cursor()
        query1 = "DELETE FROM fecreports.sponser_outpl_new1 WHERE sponsor =%s and rev_date=%s"
        val = (spon,start_date, )
        cursor.execute(query1,val)
        mydb.commit()
        print(cursor.rowcount, "record(s) deleted")
    else:
        print("No found")

def get_click(url,spon,api_key,affiliate_id):
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
def read_csv():
	for item in glob.glob("/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update_bkp/csv_files/*"):
		spon=os.path.split(item)[-1]
		spon=spon.replace('.txt', '')
		get_row_count(spon)
		with open(item, newline='') as csvfile:
			data = csv.DictReader(csvfile)
			for row in data:
				campaign_id=row['network_offer_id']
				offer_name = row['offer_name']
				offer_name=cursor._connection.converter.escape(offer_name)
				subid_1=row['sub1']
				subid_2=''
				subid_3=row['sub3']
				subid_4=row['sub4']
				subid_5=row['sub5']
				revenue=row['revenue']
				List=DecodingConversions.get_list(str(subid_3))
				fecid=DecodingConversions.convert_fecid(str(subid_3))
				subjectid=DecodingConversions.convert_subjectid(str(subid_3))
				ip=DecodingConversions.get_userip(str(subid_5))
				Entity=DecodingConversions.get_entity(str(subid_1))
				isp=DecodingConversions.get_isp(str(subid_1))
				rev_date=start_date
				sponsor=spon
				oid=campaign_id
				conversion_id=''
				offer_name=offer_name
				campaign_id=campaign_id
				subid_1=cursor._connection.converter.escape(subid_1)
				subid_2=cursor._connection.converter.escape(subid_2)
				subid_3=cursor._connection.converter.escape(subid_3)
				subid_4=cursor._connection.converter.escape(subid_4)
				subid_5=cursor._connection.converter.escape(subid_5)
				rev=float(revenue)
				#print(type(rev))
				offerid=subid_4
				List=List
				isp=isp
				fecid=fecid
				subjectid=subjectid
				Entity=Entity
				user_ip=''
				temp=''
				# sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
				if(rev>0):
					db.db_insert(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)

def processing_clicks(dataList):
	for Everflow in dataList:
		sponsor=Everflow['sponsor']
		affiliate_id=Everflow['affiliate_id']
		apikey=Everflow['apikey']
		account_url=Everflow['account_url']
		#print(sponsor+"aff id"+str(affiliate_id)+"api key"+str(account_url))
		get_click(account_url,sponsor,apikey,affiliate_id)


if __name__ == '__main__':
		threads = []
		response = requests.get('http://offers.codemskyapp.com/offersTool/offer_api/Offerapi/offerInfoApi?api_key=bdevrevrebrehresewer&require=AllPlatformWise&sponsor=Everflow&status=all', auth=HTTPBasicAuth('ims_user', 'oD1W$paXo'))
		response = response.text
		response= json.loads(response)
		Everflow=response['Everflow']
		num=len(Everflow)
		new_array2 = np.array_split(Everflow,num)
		for i in range(len(new_array2)):
			thread_1_i = threading.Thread(target=processing_clicks, args=(new_array2[i],))
			thread_1_i.start()