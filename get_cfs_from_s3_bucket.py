import os
import numpy as np
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.config import Config

cfs_dates = ['20190201','20190325','20190420','20190503','20190514','20190614']

cfs_aws_bucket = 'noaa-cfs-pds'

lead_time = 20


def downloadDirectoryFroms3(bucketName, remoteDirectoryName, endDate):
    s3_resource = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    bucket = s3_resource.Bucket(bucketName) 
    for obj in bucket.objects.filter(Prefix = remoteDirectoryName):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
            
        # Get the forecast date and time portion of the file name
        file_name = obj.key.partition('/')[-1] # Gets name of GRIB file
        if(file_name.find('anl')==-1):
            fxst_time = file_name.partition('.')[0][-10:-1] # Gets 10 characters to the left of the first '.'
            fxst_time_dt = pd.to_datetime(fxst_time,format='%Y%m%d%H') # Converts this to a date time
        
        # Compare date time of current file to end date, or see if it's an analysis file
        if((fxst_time_dt <= endDate) or (file_name.find('anl')!=-1)): 
            try:
                bucket.download_file(obj.key, obj.key) # save to same path
                print('True: downloaded '+obj.key)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print('File '+obj.key+' does not exist')
                else:
                    print('Could not download file '+obj.key)
        else:
            print('False: skipped '+obj.key)
                    
# Get Pandas Timestamp version of beginning and end times
cfs_dates_end_dt = pd.to_datetime(cfs_dates,format='%Y%m%d') + pd.Timedelta(days=lead_time)

for i in np.arange(len(cfs_dates)):

    cfs_dir_name = 'cfs.'+cfs_dates[i]+'/00/6hrly_grib_01/'
    
    print('Downloading data from '+cfs_aws_bucket+'::'+cfs_dir_name)

    downloadDirectoryFroms3(cfs_aws_bucket, cfs_dir_name, cfs_dates_end_dt[i])
