import multiprocessing
import time
import requests
import json
from datetime import date
from datetime import datetime, timedelta
import pprint
import mysql.connector as a
import ipaddress
import DecodingConversions
import re
import sys
import db
import threading
from requests.auth import HTTPBasicAuth
import numpy as np

mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()

today_date = date.today()
yesterday = today_date - timedelta(days=1)
start_date = str(yesterday)
end_date = str(yesterday)
# start_date = str(yesterday)
# end_date = start_date
# end_date = str(yesterday)
# start_date ='2023-02-03'
# end_date ='2023-02-03'

conversion_id=1
subid_2=''
sort = 'False'
current_page=1


# def insert_revenue_database(start_date, spon,oid,conversion_id,offer_name,campaign_id,subid_1, subid_2, subid_3,subid_4, subid_5, rev, offerid, list, isp,fecid,subjectid,ip,Entity,user_ip,temp):
#      sql = "INSERT INTO sponser_outpl_new1 (rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#      val = (start_date, spon,oid,conversion_id,offer_name,campaign_id,subid_1, subid_2, subid_3,subid_4, subid_5, rev, offerid, list, isp,fecid,subjectid,user_ip,Entity,user_ip,temp)
#      mycursor.execute(sql, val)
#      mydb.commit()
# def decode_subid_1(subid_1):
#     List=subid_1.split("~")
#     #print(type(list))
#     List_1=int(list[0], 36)
#     List_2=int(list[1], 36)
#     print(List_1)
#     print(List_1)
#     exor=List_1 ^ List_1
#     print(exor)
#     mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
#     cursor=mydb.cursor()
#     query="SELECT uniqueid,offerid,isp FROM redirectiondb_bkp.offerDetails WHERE oid=%s"
#     value=(exor,)
#     cursor.execute(query,val)
#     col=cursor.fetchall()
#     for x in col:
#         print(x[0])



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
        'Authorization':'Bearer ' + api_key
        }
        link=url + '/api/reports/campaigns?start=' + str(start_date) + '&end=' + str(end_date) +'&currency_id=&search=&category_id'
        # print(link)
        CampaignSummary_link=requests.get(url+'/api/reports/campaigns?start='+start_date+'&end='+end_date+'&currency_id=&search=&category_id',headers=headers)
        parse_Campaign=CampaignSummary_link.text
        print("Calling Campaign Summary Link "+str(parse_Campaign))
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
                            print("Collecting revenue for "+str(spon))
                            print("rev "+rev)
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
                            isp=DecodingConversions.get_isp(str(subid_1))
                            #print(isp)
                            temp=''
                            if spon!='INTL_ClixFlow':
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
                        isp=DecodingConversions.get_isp(str(subid_1))
                        #print(isp)
                        temp=''
                        if spon!='INTL_ClixFlow':
                            if(int(rev)>0):
                                db.db_insert(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,ip,temp)

    except Exception as e:
        print(e)

def processing_clicks(dataList):
    for new_hitpath in dataList:
        sponsor=new_hitpath['sponsor']
        get_row_count(sponsor)
        affiliate_id=new_hitpath['affiliate_id']
        apikey=new_hitpath['apikey']
        account_url=new_hitpath['account_url']
        #print(sponsor+"aff id"+str(affiliate_id)+"api key"+str(account_url))
        get_revenue(account_url,sponsor,apikey,affiliate_id)


if __name__ == '__main__':
    threads = []
    response = requests.get('http://offers.codemskyapp.com/offersTool/offer_api/Offerapi/offerInfoApi?api_key=bdevrevrebrehresewer&require=AllPlatformWise&sponsor=Everflow&status=all', auth=HTTPBasicAuth('ims_user', 'oD1W$paXo'))
    response = response.text
    response= json.loads(response)
    new_hitpath=response['new_hitpath']
    num=len(new_hitpath)
    new_array2 = np.array_split(new_hitpath,num)
    for i in range(len(new_array2)):
        thread_1_i = threading.Thread(target=processing_clicks, args=(new_array2[i],))
        thread_1_i.start()
    # threads = []
    # response = requests.get('http://offers.codemskyapp.com/offersTool/offer_api/Offerapi/offerInfoApi?api_key=bdevrevrebrehresewer&require=AllPlatformWise&sponsor=Everflow&status=all', auth=HTTPBasicAuth('ims_user', 'oD1W$paXo'))
    # response = response.text
    # response= json.loads(response)
    # new_hitpath=response['new_hitpath']
    # for i in range(len(new_hitpath)):
    #     spid=new_hitpath[i]['spid']
    #     sponsor=new_hitpath[i]['sponsor']
    #     get_row_count(sponsor)
    #     affiliate_id=new_hitpath[i]['affiliate_id']
    #     apikey=new_hitpath[i]['apikey']
    #     account_url=new_hitpath[i]['account_url']
    #     #print(account_url)

    #     t = threading.Thread(target=get_revenue, args=(account_url,sponsor,apikey,affiliate_id))   
    #     t.start()
    #     threads.append(t)
    #     for t in threads:
    #         t.join()