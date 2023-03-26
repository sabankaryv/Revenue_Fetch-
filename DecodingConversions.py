import requests
import json
from datetime import date
from datetime import datetime, timedelta
import pprint
import mysql.connector as a
import ipaddress
import re
dummy_subjectid= 'blank'
subjectid_length='blank'
dummy_userip= 'blank'
Entity="blank"
gotdata= 'blank'
isp='blank'
entityList = {"ins":"IMS","fcb":"IMS_1","swf":"IMS_1","es1":"ESP_ACC","mbs":"IMS_B","es2":"ESP2","es3":"ESP","nit":"TEAMNIT","pra":"TEAMPRA","dtc":"KARTHIC2","kgf":"KARTHIC2_1","htn":"SACHIN2","gao":"KARTHIC3","ka4":"KARTHIC4","de1":"TEAMJ","tpk":"TEAMK","gty":"TEAML","sai":"SACHIN4","de2":"DEVENDRA1","jmd":"JAYDEEP2","cyu" : "KARTHIC3_1","skl" :"ESP","qxd" : "IMS_B_1","ykr" : "SACHIN4_1","om1" : "IMS","te3" :"ESP_1","phy" : "SACHIN4","dmx":"SACHIN2","fdb": "DEVENDRA2","goa": "KARTHIC6","dqz": "SACHIN5","tok": "KARTHIC4","tik":"TEAMK","kqi":"JAYDEEP2","emz":"YOGESH1","itx":"YOGESH1","itx":"PANKAJ1","kgf":"KARTHIC2_1","ice":"KARTHIC2_1"}




def get_subid_alias(subid_1):
    try:
        alias1 = subid_1.split('_', 1)[1][0:3]
        return alias1
    except IndexError:
        gotdata = 'blank'
        return gotdata
#print(get_subid_alias(subid_1))

def get_entity(subid_1):
    alias=get_subid_alias(subid_1)
    if len(alias)<=3:
        if entityList.get(alias) is not None:
            return entityList.get(alias)
        else:
            return Entity



ispList = {"01": "aol", "03": "att", "05": "bell", "07": "com", "09": "cox", "11": "ctr", "13": "eth", "15": "fb",
               "17": "gm", "19": "hot", "21": "juno", "23": "rr", "25": "sbc", "27": "ver", "29": "yah", "31": "oth",
               "33": "icl", "34": "vm", "35": "cl", "36": "bt"}
def get_isp_alias(subid_1):
    try:
        Last3Char = subid_1[-3:]
        isp = Last3Char[0:2]
        return isp
    except IndexError:
        gotdata = 'blank'
        return gotdata

def get_isp(subid_1):
    alias=get_isp_alias(subid_1)
    if len(alias)<=2:
        if ispList.get(alias) is not None:
            return ispList.get(alias)
        else:
            return isp

def get_list(subid_3):
    try:
        list = subid_3.split('|', 1)[0]
        return list
    except IndexError:
        gotdata = 'blank'
        return gotdata


def get_fecid(subid_3):
    try:
        ok = subid_3.split('|')[1:2]
        if len(ok) <1:
            ok='blank'
            return ok
    except IndexError:
        gotdata = 'blank'
        return gotdata

def convert_fecid(subid_3):
    try:
        fecid=get_fecid(subid_3)
        FecidConv = int(fecid, 36)
        return FecidConv
    except IndexError:
        return gotdata

def get_fecid(subid_3):
    try:
        fecid = subid_3.split('|')[1]
        return fecid
    except IndexError:
        return gotdata



def convert_subjectid(subid_3):
    try:
        subjectid=get_subjectid(subid_3)
        subjectid = int(subjectid, 16)
        if len(str(subjectid))>9:
            return subjectid_length
        return subjectid
    except IndexError:
        return subjectid_length
    except ValueError:
        return subjectid_length

def get_subjectid(subid_3):
    try:
        subjectid = subid_3.split('|')[2]
        return subjectid
    except IndexError:
        return dummy_subjectid


def get_userip(subid_5):
    try:
        userip = subid_5.split('|', 1)[0]
        userIpConversion = int(userip, 32)
        UserIpConv = str(ipaddress.ip_address(userIpConversion))
        return UserIpConv
    except ValueError:
        return dummy_userip