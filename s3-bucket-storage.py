#!/usr/local/bin/python3

# Original script taken from http://www.slsmk.com/getting-the-size-of-an-s3-bucket-using-boto3-for-aws/
# Modified by Jameson Ricks, Jul 28, 2017
# Added human readable output, thread concurrency.


import boto3
import datetime
import concurrent.futures

now = datetime.datetime.now()

cw = boto3.client('cloudwatch')
s3client = boto3.client('s3')

f_col_width = 85

# Get a list of all buckets
allbuckets = s3client.list_buckets()

# Header Line for the output going to standard out
print('Bucket'.ljust(f_col_width) + 'Size in Bytes'.rjust(25))

# Array of different S3 Storage types
storage_types = ['StandardStorage', 'StandardIAStorage', 'ReducedRedundancyStorage', 'GlacierObjectOverhead']
suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'] # For human readable

# Keep running total
total = 0

# Prints human readable sizes (source: https://stackoverflow.com/questions/14996453/python-libraries-to-calculate-human-readable-filesize-from-bytes)
def humansize(nbytes):
    i = 0
    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])

# Function definition for getting all info
def get_bucket_storage(bucket):
    # get global total
    global total
    # For each bucket item, look up the cooresponding metrics from CloudWatch
    for st_type in storage_types:
            response = cw.get_metric_statistics(Namespace='AWS/S3',
                                                MetricName='BucketSizeBytes',
                                                Dimensions=[
                                                    {'Name': 'BucketName',
                                                        'Value': bucket['Name']},
                                                    {'Name': 'StorageType',
                                                        'Value': st_type},
                                                ],
                                                Statistics=['Average'],
                                                Period=3600,
                                                StartTime=(
                                                    now - datetime.timedelta(days=1)).isoformat(),
                                                EndTime=now.isoformat()
                                                )
            # The cloudwatch metrics will have the single datapoint, so we just report on it.
            for item in response["Datapoints"]:
                bucket_name = bucket["Name"] + " (" + st_type + ")"
                # print(bucket_name.ljust(50) + str("{:,}".format(int(item["Average"]))).rjust(25))
                # size(1024)
                print(bucket_name.ljust(f_col_width) +
                      humansize(item["Average"]).rjust(25))
                # Note the use of "{:,}".format.
                # This is a new shorthand method to format output.
                # I just discovered it recently.
                total += int(item["Average"])

# Use multi-threading to make it faster
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executer:
    future_to_bucket = {executer.submit(get_bucket_storage, bucket): bucket for bucket in allbuckets['Buckets']}
    # Iterate through each bucket
    # for future in concurrent.futures.as_completed(future_to_bucket):
    #     pass
    # for bucket in allbuckets['Buckets']:
    #     get_bucket_storage(bucket)

print("\n\nTotal stored in S3:".ljust(f_col_width) + humansize(total).rjust(25))
print("Total bytes stored in S3:".ljust(f_col_width) + str("{:,}".format(int(total))).rjust(25) + " bytes")
