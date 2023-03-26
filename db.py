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
import subprocess


mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
cursor=mydb.cursor()


def db_insert(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp):

    try:
        # print(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
        mydb = a.connect(host='127.0.0.1', user="root", password="cmVwc3J2ZmVjMjAxMQ", database="fecreports")
        cursor = mydb.cursor()
        mySql_insert_query = """INSERT INTO fecreports.sponser_outpl_new1(rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s) """
        #print(mySql_insert_query)
        record = (rev_date,sponsor,oid,conversion_id,offer_name,campaign_id,subid_1,subid_2,subid_3,subid_4,subid_5,rev,offerid,list,isp,fecid,subjectid,ip,Entity,user_ip,temp)
        cursor.execute(mySql_insert_query, record)
        mydb.commit()
        print("inserting revenue for "+sponsor+" into fecreports table")

    except a.Error as error:
        print("Failed to insert into MySQL table {}".format(error))

    finally:
        if mydb.is_connected():
            cursor.close()
            mydb.close()


