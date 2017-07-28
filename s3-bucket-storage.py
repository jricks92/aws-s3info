import boto3
import datetime
from hurry.filesize import size # For printing out file sizes in human readable format

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

# Keep running total
total = 0

# Iterate through each bucket
for bucket in allbuckets['Buckets']:
    # For each bucket item, look up the cooresponding metrics from CloudWatch
    for st_type in storage_types:
        response = cw.get_metric_statistics(Namespace='AWS/S3',
                                            MetricName='BucketSizeBytes',
                                            Dimensions=[
                                                {'Name': 'BucketName', 'Value': bucket['Name']},
                                                {'Name': 'StorageType', 'Value': st_type},
                                            ],
                                            Statistics=['Average'],
                                            Period=3600,
                                            StartTime=(now-datetime.timedelta(days=1)).isoformat(),
                                            EndTime=now.isoformat()
                                            )
        # The cloudwatch metrics will have the single datapoint, so we just report on it. 
        for item in response["Datapoints"]:
            bucket_name = bucket["Name"] + " (" + st_type + ")"
            # print(bucket_name.ljust(50) + str("{:,}".format(int(item["Average"]))).rjust(25))
            # size(1024)
            print(bucket_name.ljust(f_col_width) + size(item["Average"]).rjust(25))
            # Note the use of "{:,}".format.   
            # This is a new shorthand method to format output.
            # I just discovered it recently. 
            total += int(item["Average"])

print("\n\nTotal stored in S3:".ljust(f_col_width) + size(total).rjust(25))
print("Total bytes stored in S3:".ljust(f_col_width) + str("{:,}".format(int(item["Average"]))).rjust(25) + " bytes")