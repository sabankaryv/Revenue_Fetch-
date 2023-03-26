import requests
import glob
import os
import csv
import sys
import io
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
import subprocess
import sys
import db

sort = 'False'
headers = {
'Accept' : 'application/json',
}

today_date = date.today()
yesterday = today_date - timedelta(days=1)
Everflow=[]
start_date = str(yesterday)
end_date = str(yesterday)

mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()


def get_W4_New_rev(url,spon,api_key):
	camplimit = 500
	campoffset=0
	link='https://w4api.com/pub/pubs_reports_transactions/get/?min_date='+start_date+'&max_date='+end_date+'&limit='+str(camplimit)+'&key_id='+api_key+'&offset='+str(campoffset)
	request=requests.get(link,headers=headers)
	raw_data = request.text
	parse_json = json.loads(raw_data)
	campaign = parse_json['data']['results']
	for y in campaign:
		campaign_id=y['campaign_id']
		links='https://w4api.com/pub/pubs_conversion_log/get/?key_id='+api_key+'&campaign_id='+str(campaign_id)+'&min_date='+start_date+'&max_date='+end_date+'&limit='+str(camplimit)+'&offset='+str(campoffset)
		request =requests.get(links,headers=headers)
		newhitpathFinal=request.text
		newhitpathdata=json.loads(newhitpathFinal)
		finalRes=newhitpathdata['data']['results']
		for i in finalRes:
			subid_1 =i['sid1']
			campaign_name =  i['campaign_name']
			campaign_name=cursor._connection.converter.escape(campaign_name)
			subid_1 =i['sid1']
			subid_2=''
			subid_3 =i['sid2']
			subid_4 =i['sid3']
			subid_5 =i['sid4']
			rev =i['payout']
			List=DecodingConversions.get_list(str(subid_3))
			fecid=DecodingConversions.convert_fecid(str(subid_3))
			subjectid=DecodingConversions.convert_subjectid(str(subid_3))
			ip=DecodingConversions.get_userip(str(subid_5))
			Entity=DecodingConversions.get_entity(str(subid_1))
			isp=DecodingConversions.get_isp(str(subid_1))
			rev_date=start_date
			sponsor=spon
			oid=campaign_id
			conversion_id=campaign_id
			offer_name=campaign_name
			campaign_id=campaign_id
			subid_1=cursor._connection.converter.escape(subid_1)
			subid_2=cursor._connection.converter.escape(subid_2)
			subid_3=cursor._connection.converter.escape(subid_3)
			subid_4=cursor._connection.converter.escape(subid_4)
			subid_5=cursor._connection.converter.escape(subid_5)
			rev=rev
			offerid=subid_4
			List=List
			isp=isp
			fecid=fecid
			subjectid=subjectid
			Entity=Entity
			user_ip=''
			temp='CPA'
			sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
			VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
			#print(sql+";")
			if(int(rev)>0):
				db.db_insert(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)


def get_cake_rev(url,spon,api_key):
	today_date = date.today()
	yesterday = today_date - timedelta(days=1)
	start_date = str(yesterday)
	end_date = str(today_date)
	headers = {
	'Accept': 'application/json',
	}
	link=url+'/affiliates/api/Reports/Conversions?api_key='+ str(api_key) + '&affiliate_id='+ str(affiliate_id) +'&start_date='+ str(start_date) + '&end_date=' + str(end_date) +'&offer_id=0&campaign_id=0&include_duplicates=false&start_at_row=1&row_limit=0'
	request = requests.get(url + '/affiliates/api/Reports/Conversions?api_key=' + str(api_key) + '&affiliate_id='+ str(affiliate_id) +'&start_date=' + str(start_date) + '&end_date=' + str(end_date) +'&offer_id=0&campaign_id=0&include_duplicates=false&start_at_row=1&row_limit=0',headers=headers)
	raw_data = request.text
	parse_json = json.loads(raw_data)
	data = parse_json['data']
	for i in data:
		oid =  i['offer_id']
		conversion_id= i['conversion_id']
		offer_name=i['offer_name']
		subid_1=i['subid_1']
		subid_2=''
		subid_2=i['subid_2']
		subid_3=i['subid_3']
		#subid_3_lenth=len(subid_3)
		subid_4=i['subid_4']
		subid_5=i['subid_5']
		rev=i['price']
		List=DecodingConversions.get_list(subid_3)
		fecid=DecodingConversions.convert_fecid(subid_3)
		subjectid=DecodingConversions.convert_subjectid(subid_3)
		# print(subid_5)
		ip=DecodingConversions.get_userip(subid_5)
		# print(user_ip)
		Entity=DecodingConversions.get_entity(subid_1)
		isp=DecodingConversions.get_isp(subid_1)
		rev_date=start_date
		sponsor=spon
		oid=oid
		conversion_id=conversion_id
		offer_name=offer_name
		campaign_id=oid
		subid_1=subid_1
		subid_2=subid_2
		subid_3=subid_3
		subid_4=subid_4
		subid_5=subid_5
		rev=rev
		offerid=subid_4
		List=List
		isp=isp
		fecid=fecid
		subjectid=subjectid
		Entity=Entity
		user_ip=''
		temp=''
		sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
		VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
		print(sql+";")
		if(int(rev>0)):
			db.db_insert(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,ip,temp)
def read_csv():
	for item in glob.glob("/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update_bkp/csv_files_single_campaign/*"):
					spon=os.path.split(item)[-1]
					spon=spon.replace('.txt', '')
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
							#--------------converting isp,list,entity,isp,fecid,subjectid--------------# 
							List=DecodingConversions.get_list(str(subid_3))
							fecid=DecodingConversions.convert_fecid(str(subid_3))
							subjectid=DecodingConversions.convert_subjectid(str(subid_3))
							user_ip=DecodingConversions.get_userip(str(subid_5))
							Entity=DecodingConversions.get_entity(str(subid_1))
							isp=DecodingConversions.get_isp(str(subid_1))
							#--------------converting isp,list,entity,isp,fecid,subjectid--------------#
							cdate=start_date
							sponsor=spon
							oid=campaign_id
							conversion_id=0
							offer_name=offer_name
							campaign_id=campaign_id
							subid_1=cursor._connection.converter.escape(subid_1)
							subid_2=cursor._connection.converter.escape(subid_2)
							subid_3=cursor._connection.converter.escape(subid_3)
							subid_4=cursor._connection.converter.escape(subid_4)
							subid_5=cursor._connection.converter.escape(subid_5)
							rev=float(revenue)
							offerid=subid_4
							List=List
							isp=isp
							fecid=fecid
							subjectid=subjectid
							ip=user_ip
							Entity=Entity
							user_ip=user_ip
							temp='CPA'
							sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
							print(sql+";")
							if(rev>0):
								db.db_insert(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
def get_revenue(url,spon,api_key):
    try:

        headers = {
        'Accept' : 'application/json',
        'Authorization':'Bearer ' + api_key
        }
        current_page=1
        link=url + '/api/reports/campaigns?start=' + str(start_date) + '&end=' + str(end_date) +'&currency_id=&search=&category_id'
        # print(link)
        CampaignSummary_link=requests.get(url+'/api/reports/campaigns?start='+start_date+'&end='+end_date+'&currency_id=&search=&category_id',headers=headers)
        parse_Campaign=CampaignSummary_link.text
        #print("Calling Campaign Summary Link "+str(parse_Campaign))
        parse_Camp=json.loads(parse_Campaign)
        campaign = parse_Camp['data']
        #print(campaign)
        for y in campaign:
            campaign_id=y['id']
            # print(campaign_id)
            offer_name=y['name']
            offer_name=cursor._connection.converter.escape(offer_name)
                # #campaign_id=2790
            link=url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(current_page) +'&campaign_id=0&records=250'
                #print(link)
            request =requests.get(url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(current_page) +'&campaign_id=0&records=2500',headers=headers)
            newhitpathFinal=request.text
            newhitpathdata=json.loads(newhitpathFinal)
            meta=newhitpathdata['meta']
            # print(meta)
            page=meta['last_page']
            #print("Pagination "+str(page))
            if page > 1:
                    for i in range(1,page,1):
                    #print("Pages "+str(i))
                    #print(i)
                        link=url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=250'
                        request =requests.get(url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=2500',headers=headers)
                        # myData.update({'link':link})
                        # #myData.update({'click':x['reporting']['total_click']})
                        newhitpathFinal=request.text
                        newhitpathdata=json.loads(newhitpathFinal)
                        finalRes=newhitpathdata['data']
                        #print(finalRes)
                        for i in finalRes:
                            ip =  i['ip']
                            #All subid_3,subid_4,subid_5 Present in c2
                            subid_data =i['c2']
                            subid_2=''
                            subid_data=subid_data.replace('%7C', '|')
                            data=subid_data.replace('%7c', '|')

                            #converting subid_3 data
                            data=subid_data.split('|')[0:3]
                            subid_3 = "|".join(data)
                            # print(subid_3)
                            #converting subid_3 data

                            #converting subid_4 data
                            data=subid_data.split('|')[3:4]
                            subid_4 = "|".join(data)
                            # print(subid_4)
                            #converting subid_4 data End#

                            #converting subid_5 data
                            data=subid_data.split('|')[4:]
                            subid_5 = "|".join(data)
                            # print(subid_data)
                            # print(subid_5)
                            #converting subid_5 data End#
                            subid_1 =i['c1']
                            rev=i['earnings']
                            #print("Collecting revenue for "+str(spon))
                            #print("rev "+rev)
                            oid=campaign_id
                            conversion_id=''
                            offerid=subid_4
                            ##Converting List,Fecid,Subjectid,user_ip,Entity,isp
                            List=DecodingConversions.get_list(str(subid_3))
                            fecid=DecodingConversions.convert_fecid(str(subid_3))
                            subjectid=DecodingConversions.convert_subjectid(subid_3)
                            # print(subid_5)
                            user_ip=DecodingConversions.get_userip(str(subid_5))
                            # print(user_ip)
                            Entity=DecodingConversions.get_entity(str(subid_1))
                            #print(Entity)
                            isp=DecodingConversions.get_isp(str(subid_1))
                            #print(isp)
                            temp=''
                            sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
                            VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,ip,temp)
                            print(sql+";")
                            if(int(rev)>0):
                            	db.db_insert(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,ip,temp)

            for i in range(0,page,1):
                pages=1
                #print("single page"+str(pages))
                link=url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(pages) +'&campaign_id=0&records=250'
                request =requests.get(url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=2500',headers=headers)
                # myData.update({'link':link})
                # print(myData)
                newhitpathFinal=request.text
                newhitpathdata=json.loads(newhitpathFinal)
                finalRes=newhitpathdata['data']
                #print(finalRes)
                for i in finalRes:
                        ip =  i['ip']
                        #All subid_3,subid_4,subid_5 Present in c2
                        subid_data =i['c2']
                        subid_data=subid_data.replace('%7C', '|')
                        data=subid_data.replace('%7c', '|')

                        #converting subid_3 data
                        data=subid_data.split('|')[0:3]
                        subid_3 = "|".join(data)
                        # print(subid_3)
                        #converting subid_3 data
                        subid_2=''
                        #converting subid_4 data
                        data=subid_data.split('|')[3:4]
                        subid_4 = "|".join(data)
                        # print(subid_4)
                        #converting subid_4 data End#

                        #converting subid_5 data
                        data=subid_data.split('|')[4:]
                        subid_5 = "|".join(data)
                        # print(subid_data)
                        # print(subid_5)
                        #converting subid_5 data End#
                        subid_1 =i['c1']
                        rev=i['earnings']
                        oid=campaign_id
                        offerid=subid_4
                        ##Converting List,Fecid,Subjectid,user_ip,Entity,isp
                        List=DecodingConversions.get_list(str(subid_3))
                        fecid=DecodingConversions.convert_fecid(str(subid_3))
                        subjectid=DecodingConversions.convert_subjectid(subid_3)
                        # print(subid_5)
                        user_ip=DecodingConversions.get_userip(str(subid_5))
                        # print(user_ip)
                        Entity=DecodingConversions.get_entity(str(subid_1))
                        #print(Entity)
                        conversion_id=''
                        isp=DecodingConversions.get_isp(str(subid_1))
                        #print(isp)
                        temp=''

                        sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
                        VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,ip,temp)
                        print(sql+";")
                        if(int(rev)>0):
                        	db.db_insert(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,ip,temp)
    except Exception as e:
    	print(e)

mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()
affiliate_id = int(sys.argv[1])
query1="select spons_name,url,login,password,spons_id,affid,affkey,type from campaign.sponsors where active = 1 and spons_id not in ('vs') and affid="+str(affiliate_id)
cursor.execute(query1)
res = cursor.fetchall()
for x in res:
	sponsor_name=x[0]
	url=x[1]
	login=x[2]
	password=x[3]
	spons_id=x[4]
	affid=x[5]
	affkey=x[6]
	platform=x[7]
if platform == 'Everflow':
	spon=x[0]
	url=x[1]
	api_key=x[6]
	if(spon=="MadrivoEverflow_02" or spon=="MadrivoEverflow_01"):
			print("Downloading file for sponsor....... "+str(spon))
			link ='https://api.eflow.team/v1/affiliates/reporting/entity/table/export'
			json_data='{"autoRun":"false","from": "'+start_date+'","to": "'+start_date+'","timezone_id": 90,"currency_id": "USD","columns": [{"column":"offer"},{"column":"sub1"},{"column":"sub2"},{"column":"sub3"},{"column":"sub4"},{"column":"sub5"},{"column":"sub5"}],"format":"csv"}'
			headers = {'content-type': 'application/json',"X-Eflow-API-Key" : api_key}
			api_everflow = requests.post(link, json_data, headers=headers)
			raw_data=api_everflow.text
			original_stdout = sys.stdout
			with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update_bkp/csv_files_single_campaign/'+spon+'.txt', 'w') as f:
				f.write(raw_data)
			read_csv()
	else:
			print("Downloading file for sponsor....... "+str(spon))
			link ='https://api.eflow.team/v1/affiliates/reporting/entity/table/export'
			json_data='{"autoRun":"false","from": "'+start_date+'","to": "'+start_date+'","timezone_id": 80,"currency_id": "USD","columns": [{"column":"offer"},{"column":"sub1"},{"column":"sub2"},{"column":"sub3"},{"column":"sub4"},{"column":"sub5"},{"column":"sub5"}],"format":"csv"}'
			headers = {'content-type': 'application/json',"X-Eflow-API-Key" : api_key}
			api_everflow = requests.post(link, json_data, headers=headers)
			raw_data=api_everflow.text
			original_stdout = sys.stdout
			with open('/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update_bkp/csv_files_single_campaign/'+spon+'.txt', 'w') as f:
				f.write(raw_data)
			read_csv()

elif platform == "new_hitpath":
	spon=x[0]
	url=x[1]
	api_key=x[6]
	get_revenue(url,spon,api_key)
elif platform == "cake":
	spon=x[0]
	url=x[1]
	api_key=x[6]
	get_cake_rev(url,spon,api_key)
elif platform=="W4_new":
	spon=x[0]
	url=x[1]
	api_key=x[6]
	get_W4_New_rev(url,spon,api_key)
# elif platform=="hasoffers":
# 	spon=x[0]
# 	url=x[1]
# 	api_key=x[6]
# 	spons_id=x[4]
# 	get_has_offers_rev(url,spon,api_key)
else:
	print("Please choose correct platform")