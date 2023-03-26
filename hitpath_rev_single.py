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

mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()

today_date = date.today()
yesterday = today_date - timedelta(days=1)
# start_date = str(yesterday)
# end_date = str(today_date)
start_date ='2023-02-13'
end_date ='2023-02-13'
conversion_id=1
subid_2=''
sort = 'False'
current_page=1
offerid = '0'

page=[]


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
        #print(parse_Campaign)
        parse_Camp=json.loads(parse_Campaign)
        campaign = parse_Camp['data']
        #print(campaign)
        for y in campaign:
            campaign_id=y['id']
            #print(campaign_id)
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
                    link=url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=250'
                    request =requests.get(url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=2500',headers=headers)
                    newhitpathFinal=request.text
                    newhitpathdata=json.loads(newhitpathFinal)
                    finalRes=newhitpathdata['data']
                    for i in finalRes:
                        rev =  i['earnings']
                        #print(str(campaign_id))
                        subid_1 =i['c1']
                        #print(subid_1)
                        print(str(rev))
                
            else:
                i=1
                #print(str(campaign_id))
                link=url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=250'
                #print(link)
                request =requests.get(url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=2500',headers=headers)
                newhitpathFinal=request.text
                newhitpathdata=json.loads(newhitpathFinal)
                finalRes=newhitpathdata['data']
                for i in finalRes:
                    rev =  i['earnings']
                    print(str(rev))

            #         for i in range(1,page,1):
            #             #print("Pages "+str(page));
            #         #print(i)
            #             link=url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=250'
            #             request =requests.get(url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=2500',headers=headers)
            #             # myData.update({'link':link})
            #             # #myData.update({'click':x['reporting']['total_click']})
            #             newhitpathFinal=request.text
            #             newhitpathdata=json.loads(newhitpathFinal)
            #             finalRes=newhitpathdata['data']
            #             #print(finalRes)
            #             for i in finalRes:
            #                 ip =  i['ip']
            #                 subid_3=i['c2']
            #                 #print("Not Ok"+subid_3)
            #                 subid_3=subid_3.replace('%7C', '|')
            #                 subid_3=subid_3.replace('%7c', '|')
            #                 #print(subid_3)
            #                 subid_3_length=len(subid_3)
            #                 subid_3.split(':')
            #                 sub_str=subid_3.split('|')[0:3]
            #                 subid_3_converted = "|".join(sub_str)
            #                 sub_offerid=subid_3.split('|')[3:4]
            #                 rev=i['earnings']
            #                 #print(rev)
            #                 subid_1 =i['c1']
            #                 # print(subid_1)
            #                 # if '.' in subid_1:
            #                 #     subid_1=decode_subid_1(subid_1)
            #                 #subid_4=''.join(sub_offerid)
            #                 offerid=subid_4
            #                 subid_5_sub=subid_3.split('|')[4:]
            #                 subid_5 = "|".join(subid_5_sub)
            #                 oid=campaign_id
            #                 List=DecodingConversions.get_list(str(subid_3))
            #                 fecid=DecodingConversions.convert_fecid(str(subid_3))
            #                 subjectid=DecodingConversions.convert_subjectid(str(subid_3))
            #                 # print(subid_5)
            #                 user_ip=DecodingConversions.get_userip(str(subid_5))
            #                 # print(user_ip)
            #                 ip=DecodingConversions.get_userip(subid_5)
            #                 Entity=DecodingConversions.get_entity(str(subid_1))
            #                 isp=DecodingConversions.get_isp(str(subid_1))
            #                 temp='CPA'
            #                 # insert_revenue_database(start_date, spon,oid,conversion_id,offer_name,campaign_id,subid_1, subid_2, subid_3,subid_4, subid_5, rev, offerid, list, isp,fecid,subjectid,ip,Entity,user_ip,temp)
            #                 sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
            #                 VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3_converted,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)

            #                 print(sql+";")
            # #                 # db.db_insert(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3_converted,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)

            # for i in range(0,page,1):
            #     pages=1
            #     #print("single page"+str(pages))
            #     link=url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(pages) +'&campaign_id=0&records=250'
            #     request =requests.get(url + '/api/reports/sales-detail/'+ str(campaign_id) +'?&start=' + str(start_date) + '&end=' + str(end_date) + '&page='+ str(i) +'&campaign_id=0&records=2500',headers=headers)
            #     # myData.update({'link':link})
            #     # print(myData)
            #     newhitpathFinal=request.text
            #     newhitpathdata=json.loads(newhitpathFinal)
            #     finalRes=newhitpathdata['data']
            #     #print(finalRes)
            #     for i in finalRes:
            #             ip =  i['ip']
            #             subid_1 =i['c1']
            #             #print("Ok"+str(subid_1))
            #             rev=i['earnings']
            #             #print(rev)
            #             # # subid_1=decode_subid_1(subid_1)
            #             subid_3=i['c2']
                        
            #             subid_3=subid_3.replace('%7C', '|')
            #             subid_3=subid_3.replace('%7c', '|')
            #             #print(subid_3)
            #             subid_3_length=len(subid_3)
            #             subid_3.split(':')
            #             sub_str=subid_3.split('|')[0:3]
            #             subid_3_converted = "|".join(sub_str)
            #             sub_offerid=subid_3.split('|')[3:4]
            #             subid_4=''.join(sub_offerid)
            #             rev=i['earnings']
            #             offerid=subid_4
            #             subid_5_sub=subid_3.split('|')[4:]
            #             subid_5 = "|".join(subid_5_sub)
            #             List=DecodingConversions.get_list(str(subid_3))
            #             fecid=DecodingConversions.convert_fecid(str(subid_3))
            #             subjectid=DecodingConversions.convert_subjectid(subid_3)
            #             # print(subid_5)
            #             user_ip=DecodingConversions.get_userip(str(subid_5))
            #             # print(user_ip)
            #             Entity=DecodingConversions.get_entity(str(subid_1))
            #             isp=DecodingConversions.get_isp(str(subid_1))
            #             temp='CPA'
            #             sql = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
            #             VALUES ('{}', '{}', '{}', '{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}','{}','{}','{}','{}')""".format(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3_converted,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp);
            #             print(sql+";")
            #             # db.db_insert(start_date,spon,campaign_id,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3_converted,subid_4,subid_5,rev,offerid,List,isp,fecid,subjectid,user_ip,Entity,user_ip,temp)

    except Exception as e:
        print(e)
if __name__ == '__main__':
    threads = []
    #affiliate_id = input('Enter affiliate_id of sponsor?\n')     # \n ---> newline  ---> It causes a line break

    # #print(affiliate_id)
    account_url='https://partner.conversionmix.com'
    apikey='eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxIiwianRpIjoiN2FjMTFjYjhkMTJkYTBkYWEyNjk3ZWJjMjdiMWY3N2ZkNWIyNzg4ZWQzZWI3N2IxZmQ0MjNiYTBiYTIzMTM2ZDY4ODM0MzJjN2JkOTc3NWEiLCJpYXQiOjE2NjI0NzA1MjAuODEyNjY0LCJuYmYiOjE2NjI0NzA1MjAuODEyNjY3LCJleHAiOjE2OTQwMDY1MjAuODA2MjM5LCJzdWIiOiIzOTA0MTMiLCJzY29wZXMiOlsiKiJdfQ.GquooMW-ZUvk67VAPSkY3PemuaU-WajPtKGxNmwCLfK7IPn1oREuPCz0hr9NbPTkHd1qzTJT-VjYwsvyB8U9XMfBpdw1j6llqrzc6qB0EiLmnz73usA5hq6urQGQtVSiblhiP_9qQyYqMkeOXwmBh4xNOxBl0HrUtkqrM8iYdeTeYYF8gLvH_lyul7BL4nSK_uH7g4aDMzukn5jqhH3AJQqXgR7BZmsZNZwHmVddeFyG4abkqko7jDzDnVsu9YzlUmg50oN-DT24zb-l0FNt-_ZJv7HHOFDix5i3iR97d4weQnnibybZxzLLq1CuS3RR25v_OzbelYKwJ8gDRIOUdR1vAx-lbMt-UfWXaQZ45Ps1V-WgfjVgxvxU-jsjRQLEXfUjdAF6ygPsVwZhwhO2rIVq7PV3Z8HDYk-7YNhj-X23onG-Kt1nupabTAEbYZS6t-46NnMSzx6Od4pu_DhWIqsCumnImPCCeBHX-GGFigVRUX6LwIVzZxVAjXDRYLpaI88coOp4NCYtu8ThgcAeo4SkxxtgbghOGfp2m89k_0gFdirfsIBr2sVvwLz0W41bZmIHcPb5eRXbA_KUXDjMPzUUP-ggBIwJLXSDt_Jf1JUCnYLxcX0iDBJBx6txpb4mscBq8UFRdy5EWOs80qcTcQwHVGQcov8rFsJtk867m_g'
    sponsor='AddemandDigital_IMS_07'
    affiliate_id='390413'




    # account_url='https://partner.conversionmix.com'
    # apikey='eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxIiwianRpIjoiN2FjMTFjYjhkMTJkYTBkYWEyNjk3ZWJjMjdiMWY3N2ZkNWIyNzg4ZWQzZWI3N2IxZmQ0MjNiYTBiYTIzMTM2ZDY4ODM0MzJjN2JkOTc3NWEiLCJpYXQiOjE2NjI0NzA1MjAuODEyNjY0LCJuYmYiOjE2NjI0NzA1MjAuODEyNjY3LCJleHAiOjE2OTQwMDY1MjAuODA2MjM5LCJzdWIiOiIzOTA0MTMiLCJzY29wZXMiOlsiKiJdfQ.GquooMW-ZUvk67VAPSkY3PemuaU-WajPtKGxNmwCLfK7IPn1oREuPCz0hr9NbPTkHd1qzTJT-VjYwsvyB8U9XMfBpdw1j6llqrzc6qB0EiLmnz73usA5hq6urQGQtVSiblhiP_9qQyYqMkeOXwmBh4xNOxBl0HrUtkqrM8iYdeTeYYF8gLvH_lyul7BL4nSK_uH7g4aDMzukn5jqhH3AJQqXgR7BZmsZNZwHmVddeFyG4abkqko7jDzDnVsu9YzlUmg50oN-DT24zb-l0FNt-_ZJv7HHOFDix5i3iR97d4weQnnibybZxzLLq1CuS3RR25v_OzbelYKwJ8gDRIOUdR1vAx-lbMt-UfWXaQZ45Ps1V-WgfjVgxvxU-jsjRQLEXfUjdAF6ygPsVwZhwhO2rIVq7PV3Z8HDYk-7YNhj-X23onG-Kt1nupabTAEbYZS6t-46NnMSzx6Od4pu_DhWIqsCumnImPCCeBHX-GGFigVRUX6LwIVzZxVAjXDRYLpaI88coOp4NCYtu8ThgcAeo4SkxxtgbghOGfp2m89k_0gFdirfsIBr2sVvwLz0W41bZmIHcPb5eRXbA_KUXDjMPzUUP-ggBIwJLXSDt_Jf1JUCnYLxcX0iDBJBx6txpb4mscBq8UFRdy5EWOs80qcTcQwHVGQcov8rFsJtk867m_g'
    # sponsor='AddemandDigital_IMS_07'
    # affiliate_id='390413'

    t = threading.Thread(target=get_revenue, args=(account_url,sponsor,apikey,affiliate_id))   
    t.start()
    threads.append(t)
    for t in threads:
        t.join()


