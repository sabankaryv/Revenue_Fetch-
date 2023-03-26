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
import threading
import subprocess
import db
import numpy as np
from requests.auth import HTTPBasicAuth

today_date = date.today()
yesterday = today_date - timedelta(days=1)
start_date = str(yesterday)
end_date = str(today_date)
# start_date='2023-01-18'
# end_date='2023-01-18'
mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()
sort = 'False'
offerid = '0'

campaign_id = "00000"
headers = {
	'Accept': 'application/json',
	}
subid_2='0'
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
    try:
        link=url+'/affiliates/api/Reports/Conversions?api_key='+ api_key + '&affiliate_id='+ affiliate_id +'&start_date='+ start_date + '&end_date=' + end_date +'&offer_id=0&campaign_id=0&include_duplicates=false&start_at_row=1&row_limit=0'
        request = requests.get(url + '/affiliates/api/Reports/Conversions?api_key=' + api_key + '&affiliate_id='+ affiliate_id +'&start_date=' + start_date + '&end_date=' + end_date +'&offer_id=0&campaign_id=0&include_duplicates=false&start_at_row=1&row_limit=0',headers=headers)
        status_of_code=request.status_code
        if status_of_code==200:
            raw_data = request.text
            parse_json = json.loads(raw_data)
            data = parse_json['data']
            for i in data:
                oid =  i['offer_id']
                conversion_id= i['conversion_id']
                offer_name=i['offer_name']
                subid_1=i['subid_1']
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
                # sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
                # VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
                # print(sql+";")
                if(int(rev>0)):
                    db.db_insert(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,ip,temp)
            else:
                  print("issue with api for sponsor "+spon+" check for api key= "+api_key+" or url"+url+"")

    except Exception as e:
          print(e)


def processing_clicks(dataList):
    for cake in dataList:
        sponsor=cake['sponsor']
        get_row_count(sponsor)
        affiliate_id=cake['affiliate_id']
        apikey=cake['apikey']
        account_url=cake['account_url']
        #print(sponsor+"aff id"+str(affiliate_id)+"api key"+str(account_url))
        get_revenue(account_url,sponsor,apikey,affiliate_id)

if __name__ == '__main__':
    threads = []
    response = requests.get('http://offers.codemskyapp.com/offersTool/offer_api/Offerapi/offerInfoApi?api_key=bdevrevrebrehresewer&require=AllPlatformWise&sponsor=Everflow&status=all', auth=HTTPBasicAuth('ims_user', 'oD1W$paXo'))
    response = response.text
    response= json.loads(response)
    cake=response['cake']
    num=len(cake)
    new_array2 = np.array_split(cake,num)
    for i in range(len(new_array2)):
        thread_1_i = threading.Thread(target=processing_clicks, args=(new_array2[i],))
        thread_1_i.start()

# cakeSponsor =requests.get('http://ektun.improvearch.com/app/data/python_rev/all_sponsor.php?cakeSponsor')
# cakeSponsor = cakeSponsor.text
# cakeSponsor= json.loads(cakeSponsor)
# for i in range(len(cakeSponsor)):
#     url=cakeSponsor[i]['url']
#     #print(url)
#     spon=cakeSponsor[i]['sponsName']
#     get_row_count(spon)
#     api_key=cakeSponsor[i]['affkey']
#     affiliate_id=cakeSponsor[i]['spons_id']
#     link=url+'/affiliates/api/Reports/Conversions?api_key='+ api_key + '&affiliate_id='+ affiliate_id +'&start_date='+ start_date + '&end_date=' + end_date +'&offer_id=0&campaign_id=0&include_duplicates=false&start_at_row=1&row_limit=0'
#     #print(link)
#     request = requests.get(url + '/affiliates/api/Reports/Conversions?api_key=' + api_key + '&affiliate_id='+ affiliate_id +'&start_date=' + start_date + '&end_date=' + end_date +
#      	'&offer_id=0&campaign_id=0&include_duplicates=false&start_at_row=1&row_limit=0',headers=headers)
#     raw_data = request.text
#     parse_json = json.loads(raw_data)
#     data = parse_json['data']
#     for i in data:
#         oid =  i['offer_id']
#         conversion_id= i['conversion_id']
#         offer_name=i['offer_name']
#         subid_1=i['subid_1']
#         subid_2=i['subid_2']
#         subid_3=i['subid_3']
#         #subid_3_lenth=len(subid_3)
#         subid_4=i['subid_4']
#         subid_5=i['subid_5']
#         rev=i['price']
#         List=DecodingConversions.get_list(subid_3)
#         fecid=DecodingConversions.convert_fecid(subid_3)
#         subjectid=DecodingConversions.convert_subjectid(subid_3)
#         # print(subid_5)
#         ip=DecodingConversions.get_userip(subid_5)
#         # print(user_ip)
#         Entity=DecodingConversions.get_entity(subid_1)
#         isp=DecodingConversions.get_isp(subid_1)
#         rev_date=start_date
#         sponsor=spon
#         oid=oid
#         conversion_id=conversion_id
#         offer_name=offer_name
#         campaign_id=oid
#         subid_1=subid_1
#         subid_2=subid_2
#         subid_3=subid_3
#         subid_4=subid_4
#         subid_5=subid_5
#         rev=rev
#         offerid=subid_4
#         List=List
#         isp=isp
#         fecid=fecid
#         subjectid=subjectid
#         Entity=Entity
#         user_ip=''
#         temp=''
#         # sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
#         # VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
#         # print(sql+";")
#         if(int(rev>0)):
#             db.db_insert(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,ip,temp)