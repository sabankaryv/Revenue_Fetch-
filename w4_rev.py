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
import db
import numpy as np
import threading
from requests.auth import HTTPBasicAuth


mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()
today_date = date.today()
yesterday = today_date - timedelta(days=1)
start_date = str(yesterday)
end_date = str(yesterday)
# start_date='2023-01-17'
# end_date='2023-01-17'
sort = 'False'
conversion_id = '0'
camplimit = 500
campoffset=0
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
        headers = {
        'Accept' : 'application/json',
        }
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
            #print(finalRes)
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
                # print("------------------------------------------")
                # print(subid_1)
                # print(campaign_name)
                # print(subid_1)
                # print(subid_3)
                # print(subid_4)
                # print(subid_5)
                # print(rev)
                List=DecodingConversions.get_list(str(subid_3))
                fecid=DecodingConversions.convert_fecid(str(subid_3))
                subjectid=DecodingConversions.convert_subjectid(str(subid_3))
                ip=DecodingConversions.get_userip(str(subid_5))
                Entity=DecodingConversions.get_entity(str(subid_1))
                isp=DecodingConversions.get_isp(str(subid_1))
                # print(List)
                # print(fecid)
                # print(subjectid)
                # print(user_ip)
                # print(Entity)
                # print(isp)
                # print("------------------------------------------")
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

                # sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
                # VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
                # #print(sql+";")
                if(int(rev)>0):
                    db.db_insert(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
    except Exception as e:
        print(e)


def processing_clicks(dataList):
    for W4_new in dataList:
        sponsor=W4_new['sponsor']
        get_row_count(sponsor)
        affiliate_id=W4_new['affiliate_id']
        apikey=W4_new['apikey']
        account_url=W4_new['account_url']
        #print(sponsor+"aff id"+str(affiliate_id)+"api key"+str(account_url))
        get_revenue(account_url,sponsor,apikey,affiliate_id)



if __name__ == '__main__':
      threads = []
      response = requests.get('http://offers.codemskyapp.com/offersTool/offer_api/Offerapi/offerInfoApi?api_key=bdevrevrebrehresewer&require=AllPlatformWise&sponsor=Everflow&status=all', auth=HTTPBasicAuth('ims_user', 'oD1W$paXo'))
      response = response.text
      response= json.loads(response)
      W4_new=response['W4_new']
      num=len(W4_new)
      new_array2 = np.array_split(W4_new,num)
      for i in range(len(new_array2)):
        thread_1_i = threading.Thread(target=processing_clicks, args=(new_array2[i],))
        thread_1_i.start()

# try:
#     response = requests.get('http://offers.codemskyapp.com/offersTool/offer_api/Offerapi/offerInfoApi?api_key=bdevrevrebrehresewer&require=AllPlatformWise&sponsor=Everflow&status=all', auth=HTTPBasicAuth('ims_user', 'oD1W$paXo'))
#     response = response.text
#     response= json.loads(response)
#     W4_new=response['W4_new']
#     for i in range(len(W4_new)):
#         spid=W4_new[i]['spid']
#         spon=W4_new[i]['sponsor']
#         affiliate_id=W4_new[i]['affiliate_id']
#         api_key=W4_new[i]['apikey']
#         account_url=W4_new[i]['account_url']
#         headers = {
#         'Accept' : 'application/json',
#         }
#         link='https://w4api.com/pub/pubs_reports_transactions/get/?min_date='+start_date+'&max_date='+end_date+'&limit='+str(camplimit)+'&key_id='+api_key+'&offset='+str(campoffset)
#         request=requests.get(link,headers=headers)
#         raw_data = request.text
#         parse_json = json.loads(raw_data)
#         campaign = parse_json['data']['results']
#         for y in campaign:
#             campaign_id=y['campaign_id']
#             links='https://w4api.com/pub/pubs_conversion_log/get/?key_id='+api_key+'&campaign_id='+str(campaign_id)+'&min_date='+start_date+'&max_date='+end_date+'&limit='+str(camplimit)+'&offset='+str(campoffset)
#             request =requests.get(links,headers=headers)
#             newhitpathFinal=request.text
#             newhitpathdata=json.loads(newhitpathFinal)
#             finalRes=newhitpathdata['data']['results']
#             #print(finalRes)
#             for i in finalRes:
#                 subid_1 =i['sid1']
#                 campaign_name =  i['campaign_name']
#                 campaign_name=cursor._connection.converter.escape(campaign_name)
#                 subid_1 =i['sid1']
#                 subid_3 =i['sid2']
#                 subid_4 =i['sid3']
#                 subid_5 =i['sid4']
#                 rev =i['payout']
#                 # print("------------------------------------------")
#                 # print(subid_1)
#                 # print(campaign_name)
#                 # print(subid_1)
#                 # print(subid_3)
#                 # print(subid_4)
#                 # print(subid_5)
#                 # print(rev)
#                 List=DecodingConversions.get_list(str(subid_3))
#                 fecid=DecodingConversions.convert_fecid(str(subid_3))
#                 subjectid=DecodingConversions.convert_subjectid(str(subid_3))
#                 ip=DecodingConversions.get_userip(str(subid_5))
#                 Entity=DecodingConversions.get_entity(str(subid_1))
#                 isp=DecodingConversions.get_isp(str(subid_1))
#                 # print(List)
#                 # print(fecid)
#                 # print(subjectid)
#                 # print(user_ip)
#                 # print(Entity)
#                 # print(isp)
#                 # print("------------------------------------------")
#                 rev_date=start_date
#                 sponsor=spon
#                 oid=campaign_id
#                 conversion_id=campaign_id
#                 offer_name=campaign_name
#                 campaign_id=campaign_id
#                 subid_1=cursor._connection.converter.escape(subid_1)
#                 subid_2=cursor._connection.converter.escape(subid_2)
#                 subid_3=cursor._connection.converter.escape(subid_3)
#                 subid_4=cursor._connection.converter.escape(subid_4)
#                 subid_5=cursor._connection.converter.escape(subid_5)
#                 rev=rev
#                 offerid=subid_4
#                 List=List
#                 isp=isp
#                 fecid=fecid
#                 subjectid=subjectid
#                 Entity=Entity
#                 user_ip=''
#                 temp='CPA'

#                 # sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
#                 # VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)
#                 # #print(sql+";")
#                 if(int(rev)>0):
#                     db.db_insert(start_date,spon,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,ip,Entity,user_ip,temp)


# except Exception as e:
#     print(e)