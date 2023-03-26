import glob
import csv
import os
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
import threading
from subprocess import *
import db

conversion_id=0
subid_2=''
today_date = date.today()
yesterday = today_date - timedelta(days=1)
# start_date = str(yesterday)
# end_date = str(today_date)
start_date = str(yesterday)
end_date = str(yesterday)
subid_2=""
conversion_id=0
subid_2=''
# start_date ='2023-03-17'
# end_date ='2023-03-17'
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

for item in glob.glob("/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update_bkp/csv_files/*"):
    #print(item)
    spon=os.path.split(item)[-1]
    spon=spon.replace('.txt', '')
    #print(spon)
    get_row_count(spon)
    with open(item, newline='') as csvfile:
        data = csv.DictReader(csvfile)
        for row in data:
            campaign_id=row['network_offer_id']
            offer_name = row['offer_name']
            offer_name=cursor._connection.converter.escape(offer_name)
            subid_1=row['sub1']
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
            conversion_id=conversion_id
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
