import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config

cfs_dates = ['20190201','20190325','20190420','20190503','20190514','20190614']

cfs_aws_bucket = 'noaa-cfs-pds'

def downloadDirectoryFroms3(bucketName, remoteDirectoryName):
    s3_resource = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    bucket = s3_resource.Bucket(bucketName) 
    for obj in bucket.objects.filter(Prefix = remoteDirectoryName):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key) # save to same path
        print('downloaded '+obj.key)
        
for date in cfs_dates:
    cfs_dir_name = 'cfs.'+date+'/00/6hrly_grib_01/'
    
    print('Downloading data from '+cfs_aws_bucket+'::'+cfs_dir_name)

    downloadDirectoryFroms3(cfs_aws_bucket, cfs_dir_name)