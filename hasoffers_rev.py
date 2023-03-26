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
import db
import threading
import numpy as np
from requests.auth import HTTPBasicAuth

myData={}
today_date = date.today()
yesterday = today_date - timedelta(days=1)
start_date = str(yesterday)
end_date = str(yesterday)




# start_date='2023-02-04'
# end_date='2023-02-04'
sort = 'False'
mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()

# print(hasoffers_sponsor_fetch)
headers = {
	'Accept': 'application/json',
	}
url='http://api.hasoffers.com'
method='getStats'
NetworkId='gwm2'
offset=0
conversion=0
temp='Null'

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


def get_revenue(url,spon,api_key,affiliate_id):
	request='http://api.hasoffers.com/v3/Affiliate_Report.json?Method=getStats&api_key='+api_key+'&NetworkId='+affiliate_id+'&page=1&limit=50000&data_start='+start_date+'&data_end='+end_date+'&fields[]=Stat.offer_id&fields[]=Offer.name&fields[]=Stat.affiliate_info1&fields[]=Stat.affiliate_info2&fields[]=Stat.affiliate_info3&fields[]=Stat.affiliate_info4&fields[]=Stat.affiliate_info5&fields[]=Stat.clicks&fields[]=Stat.conversions&fields[]=Stat.payout&sort[Stat.conversions]=desc&filters[Stat.date][conditional]=BETWEEN&filters[Stat.date][values][]='+start_date+'&filters[Stat.date][values][]='+start_date+'&groups[]=Stat.offer_id&groups[]=Stat.affiliate_info1&groups[]=Stat.affiliate_info2&groups[]=Stat.affiliate_info3&groups[]=Stat.affiliate_info4&groups[]=Stat.affiliate_info5&hour_offset=0'
	try:
		request=requests.get(request)
		status_of_code=request.status_code
		if status_of_code==200:
			raw_data = request.text
			parse_json = json.loads(raw_data)
			#print(parse_json)
			data = parse_json['response']
			#print(data)
			yogii = data['data']['data']
			#print(yogii)
			for data in yogii:
				subid_1=data['Stat']['affiliate_info1']
				offer_name=data['Offer']['name']
				oid=data['Stat']['offer_id']
				subid_2=data['Stat']['affiliate_info2']
				subid_3=data['Stat']['affiliate_info3']
				subid_3=subid_3.replace('%7C', '|')
				subid_3=subid_3.replace('%7c', '|')
				#print(subid_3)
				subid_4=data['Stat']['affiliate_info4']
				subid_5=data['Stat']['affiliate_info5']
				subid_5=subid_5.replace('%7C', '|')
				subid_5=subid_5.replace('%7c', '|')

				rev=float(data['Stat']['payout'])
				List=DecodingConversions.get_list(subid_3)
				fecid=DecodingConversions.convert_fecid(subid_3)
				subjectid=DecodingConversions.convert_subjectid(subid_3)
				user_ip=DecodingConversions.get_userip(subid_5)
				Entity=DecodingConversions.get_entity(subid_1)
				isp=DecodingConversions.get_isp(subid_1)

				
				rev_date=start_date
				sponsor=spon
				oid=oid
				conversion_id=oid
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
				ip=user_ip
				Entity=Entity
				user_ip=user_ip
				temp='CPA'
				# sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
				# VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
				# print(sql+";")
				if(int(rev)>0):
					db.db_insert(start_date,spon,oid,conversion_id,offer_name,oid,subid_1,subid_2,subid_3,subid_4,subid_5,rev,subid_4,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
		else:
			print("issue with api for sponsor "+spon+" check for api key= "+api_key+" or url"+account_url+"")
	except Exception as e:
		print(e)



def processing_clicks(dataList):
	for hasoffers in dataList:
		sponsor=hasoffers['sponsor']
		get_row_count(sponsor)
		affiliate_id=hasoffers['affiliate_id']
		apikey=hasoffers['apikey']
		account_url=hasoffers['account_url']
		#print(sponsor+"aff id"+str(affiliate_id)+"api key"+str(account_url))
		get_revenue(account_url,sponsor,apikey,affiliate_id)

if __name__ == '__main__':
	threads = []
	response = requests.get('http://offers.codemskyapp.com/offersTool/offer_api/Offerapi/offerInfoApi?api_key=bdevrevrebrehresewer&require=AllPlatformWise&sponsor=Everflow&status=all', auth=HTTPBasicAuth('ims_user', 'oD1W$paXo'))
	response = response.text
	response= json.loads(response)
	hasoffers=response['hasoffers']
	num=len(hasoffers)
	new_array2 = np.array_split(hasoffers,num)
	for i in range(len(new_array2)):
		thread_1_i = threading.Thread(target=processing_clicks, args=(new_array2[i],))
		thread_1_i.start()


# hasoffers =requests.get('http://ektun.improvearch.com/app/data/python_rev/all_sponsor.php?hasoffers')
# hasoffers = hasoffers.text
# hasoffers= json.loads(hasoffers)
# for i in range(len(hasoffers)):
# 	url=hasoffers[i]['url']
# 	spon=hasoffers[i]['sponsName']
# 	get_row_count(spon)
# 	api=hasoffers[i]['affkey']
# 	affiliate_id=hasoffers[i]['spons_id']
# 	aff_id=hasoffers[i]['aff_id']
# 	request='http://api.hasoffers.com/v3/Affiliate_Report.json?Method=getStats&api_key='+api+'&NetworkId='+aff_id+'&page=1&limit=50000&data_start='+start_date+'&data_end='+end_date+'&fields[]=Stat.offer_id&fields[]=Offer.name&fields[]=Stat.affiliate_info1&fields[]=Stat.affiliate_info2&fields[]=Stat.affiliate_info3&fields[]=Stat.affiliate_info4&fields[]=Stat.affiliate_info5&fields[]=Stat.clicks&fields[]=Stat.conversions&fields[]=Stat.payout&sort[Stat.conversions]=desc&filters[Stat.date][conditional]=BETWEEN&filters[Stat.date][values][]='+start_date+'&filters[Stat.date][values][]='+start_date+'&groups[]=Stat.offer_id&groups[]=Stat.affiliate_info1&groups[]=Stat.affiliate_info2&groups[]=Stat.affiliate_info3&groups[]=Stat.affiliate_info4&groups[]=Stat.affiliate_info5&hour_offset=0'
# 	try:
# 		request=requests.get(request)
# 		status_of_code=request.status_code
# 		if status_of_code==200:
# 			raw_data = request.text
# 			parse_json = json.loads(raw_data)
# 			#print(parse_json)
# 			data = parse_json['response']
# 			#print(data)
# 			yogii = data['data']['data']
# 			#print(yogii)
# 			for data in yogii:
# 				subid_1=data['Stat']['affiliate_info1']
# 				offer_name=data['Offer']['name']
# 				oid=data['Stat']['offer_id']
# 				subid_2=data['Stat']['affiliate_info2']
# 				subid_3=data['Stat']['affiliate_info3']
# 				subid_3=subid_3.replace('%7C', '|')
# 				subid_3=subid_3.replace('%7c', '|')
# 				#print(subid_3)
# 				subid_4=data['Stat']['affiliate_info4']
# 				subid_5=data['Stat']['affiliate_info5']
# 				subid_5=subid_5.replace('%7C', '|')
# 				subid_5=subid_5.replace('%7c', '|')

# 				rev=float(data['Stat']['payout'])
# 				List=DecodingConversions.get_list(subid_3)
# 				fecid=DecodingConversions.convert_fecid(subid_3)
# 				subjectid=DecodingConversions.convert_subjectid(subid_3)
# 				user_ip=DecodingConversions.get_userip(subid_5)
# 				Entity=DecodingConversions.get_entity(subid_1)
# 				isp=DecodingConversions.get_isp(subid_1)

				
# 				rev_date=start_date
# 				sponsor=spon
# 				oid=oid
# 				conversion_id=oid
# 				offer_name=offer_name
# 				campaign_id=oid
# 				subid_1=subid_1
# 				subid_2=subid_2
# 				subid_3=subid_3
# 				subid_4=subid_4
# 				subid_5=subid_5
# 				rev=rev
# 				offerid=subid_4
# 				List=List
# 				isp=isp
# 				fecid=fecid
# 				subjectid=subjectid
# 				ip=user_ip
# 				Entity=Entity
# 				user_ip=user_ip
# 				temp='CPA'
# 				# sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
# 				# VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
# 				# print(sql+";")
# 				if(int(rev)>0):
# 					db.db_insert(start_date,spon,oid,conversion_id,offer_name,oid,subid_1,subid_2,subid_3,subid_4,subid_5,rev,subid_4,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
# 		else:
# 			print("issue with api for sponsor "+spon+" check for api key= "+api+" or url"+url+"")
# 	except Exception as e:
# 		print(e)