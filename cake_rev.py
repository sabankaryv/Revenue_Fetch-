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
import os
import subprocess
import db


today_date = date.today()
yesterday = today_date - timedelta(days=1)
start_date = str(yesterday)
end_date = str(yesterday)
# start_date='2023-01-18'
# end_date='2023-01-18'

sort = 'False'
offerid = '0'

campaign_id = "00000"
headers = {
	'Accept': 'application/json',
	}
subid_2='0'
mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()

cakeSponsor =requests.get('http://ektun.improvearch.com/app/data/python_rev/all_sponsor.php?cakeSponsor')
cakeSponsor = cakeSponsor.text
cakeSponsor= json.loads(cakeSponsor)
for i in range(len(cakeSponsor)):
    url=cakeSponsor[i]['url']
    spon=cakeSponsor[i]['sponsName']
    api_key=cakeSponsor[i]['affkey']
    affiliate_id=cakeSponsor[i]['spons_id']
    link=url+'/affiliates/api/Reports/Conversions?api_key='+ api_key + '&affiliate_id='+ affiliate_id +'&start_date='+ start_date + '&end_date=' + end_date +'&offer_id=0&campaign_id=0&include_duplicates=false&start_at_row=1&row_limit=0'
    #print(link)
    #exit()
    request = requests.get(url + '/affiliates/api/Reports/Conversions?api_key=' + api_key + '&affiliate_id='+ affiliate_id +'&start_date=' + start_date + '&end_date=' + end_date +
     	'&offer_id=0&campaign_id=0&include_duplicates=false&start_at_row=1&row_limit=0',headers=headers)
    print("fetching revenue for sponsor"+str(spon))
    status_of_code=request.status_code
    print("status of api is "+str(status_of_code))
    if status_of_code==200:
        raw_data = request.text
        parse_json = json.loads(raw_data)
        data = parse_json['data']
        for i in data:
            oid =  i['offer_id']
            print(oid)
            # conversion_id= i['conversion_id']
            # offer_name=i['offer_name']
            # subid_1=i['subid_1']
            # subid_2=i['subid_2']
            # subid_3=i['subid_3']
            # #subid_3_lenth=len(subid_3)
            # subid_4=i['subid_4']
            # subid_5=i['subid_5']
            # rev=i['price']
            # print("revenue of sponsor "+str(spon)+" and revenue is "+str(rev))
            # List=DecodingConversions.get_list(subid_3)
            # fecid=DecodingConversions.convert_fecid(subid_3)
            # subjectid=DecodingConversions.convert_subjectid(subid_3)
            # # print(subid_5)
            # ip=DecodingConversions.get_userip(subid_5)
            # # print(user_ip)
            # Entity=DecodingConversions.get_entity(subid_1)
            # isp=DecodingConversions.get_isp(subid_1)
            # rev_date=start_date
            # sponsor=spon
            # oid=oid
            # conversion_id=conversion_id
            # offer_name=offer_name
            # campaign_id=oid
            # subid_1=subid_1
            # subid_2=subid_2
            # subid_3=subid_3
            # subid_4=subid_4
            # subid_5=subid_5
            # rev=rev
            # offerid=subid_4
            # List=List
            # isp=isp
            # fecid=fecid
            # subjectid=subjectid
            # Entity=Entity
            # user_ip=''
            # temp=''
            # sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
            # VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(rev_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
            # print(sql+";")
            #db.db_insert(rev_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
    else:
        print("issue with api for sponsor "+spon+" check api key or url")
