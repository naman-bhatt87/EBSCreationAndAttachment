#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 16:16:30 2019

@author: naman
"""


import os
import sys
import boto3
import time
import pandas as pd


#variables change them to suit your need
fullfilepath = 'inputfile.xlsx'
ACCESS_KEY = input("Enter Access Key- ")
ACCESS_SECRET_KEY = input("Enter Secret Key- ")
REGION = input("Enter Region- ")
outputfile = 'outputfile.xlsx'



#function definitions. 

#####function to convert excel to a dictonary#############
def excel_to_dict(fullfilepath):
    xls = pd.ExcelFile(fullfilepath)
    df = xls.parse(xls.sheet_names[0]).dropna()
    data_dict=df.to_dict('list')
    return data_dict
 

##########function to create a volume and attach it to instance ####################
def create_attach_vol(size, AZ, tag1, tag2, instance_id, dev_name, kms_id):
    vol = conn.create_volume(AvailabilityZone=AZ, Encrypted=True, KmsKeyId=kms_id, Size=int(size))
    # Add a Name tag to the new volume so we can find it.
    conn.create_tags(Resources=[vol["VolumeId"]], Tags=[{'Key':"Name", 'Value':tag1}])
    conn.create_tags(Resources=[vol["VolumeId"]], Tags=[{'Key':"Backgroup", 'Value':tag2}])
    #funciton holgin till we have volume created 
    curr_vol_state = conn.describe_volumes(VolumeIds=[vol["VolumeId"]])["Volumes"][0]["State"]
    while curr_vol_state == 'creating':
        curr_vol_state = conn.describe_volumes(VolumeIds=[vol["VolumeId"]])["Volumes"][0]["State"]
       # print ('Current Volume Status: ', curr_vol_state)
        time.sleep(5)
    
    #### Attach a volume ####
    print("Volume %s is created " %vol["VolumeId"])
    conn.attach_volume(Device=dev_name, InstanceId=instance_id, VolumeId=vol["VolumeId"])
    print("Volume %s is attached to instance %s" %(vol["VolumeId"], instance_id))
    
    return vol["VolumeId"]

####function to create a dictonary instances vs there AZ #######
def populate_AZ(data_dict):
    
    uniqinstances =  set(data_dict['instance_id'])
    azinstancelookup = {}
    for inst in uniqinstances:
        az = conn.describe_instances(InstanceIds=[inst])["Reservations"][0]["Instances"][0]["Placement"]["AvailabilityZone"]
        azinstancelookup[inst] = az
        
    return azinstancelookup


#### function to validate KMS keys ##############
def KMSValidate(data_dict):
    km = boto3.client('kms', aws_access_key_id=ACCESS_KEY, 
                    aws_secret_access_key=ACCESS_SECRET_KEY,
                    region_name=REGION)
    uniqkeys = set(data_dict['kms_key_id'])
    for key in uniqkeys:
        km.describe_key(KeyId=key)
        
####function to itreate over the data ########
def itrerate_create(data_dict, mapazinst):
    to_itr = len(data_dict['vol_size'])
    Vol_id = [] 
    for i in range(to_itr):
        vol_id = create_attach_vol(data_dict['vol_size'][i], mapazinst[data_dict['instance_id'][i]], data_dict['name'][i], data_dict['backupgroup'][i], data_dict['instance_id'][i], data_dict['dev_name'][i], data_dict['kms_key_id'][i])
        Vol_id.append(vol_id)
    
    return Vol_id


#### function to convert add new volid and convert dictonary to volume ##########
def dict_to_excel(data_dict, Vol_id, outputfile):
    #nudata_dict = dict(data_dict)
    data_dict['vol_id']=Vol_id
    nudf = pd.DataFrame(data_dict)
    nudf.to_excel(outputfile)
    


if __name__ == "__main__":
    
    conn = boto3.client('ec2', aws_access_key_id=ACCESS_KEY, 
                        aws_secret_access_key=ACCESS_SECRET_KEY,
                        region_name=REGION)
    
    data_dict = excel_to_dict(fullfilepath)
    print("Validating instance ID ")
    mapazinst = populate_AZ(data_dict)
    print("Validating KMS keys ")
    KMSValidate(data_dict)
    print("All data validated starting with creation and Attachment")
    vol_ids = itrerate_create(data_dict, mapazinst)
    print("creating excel file with details")
    dict_to_excel(data_dict, vol_ids, outputfile)