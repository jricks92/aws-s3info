#!/usr/local/bin/python3

# Original script taken from http://www.slsmk.com/getting-the-size-of-an-s3-bucket-using-boto3-for-aws/
# Modified by Jameson Ricks, Jul 28, 2017
# Added human readable output, thread concurrency.

import sys
import boto3
import datetime
import concurrent.futures


## Global variables
####################
now = datetime.datetime.now()
num_workers = 10
verbose = False
single_thread = False
raw_bytes = False
profile = False

# Output column widths
f_col_width = 85
l_col_width = 25

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

# Default get 
def get_session(**kwargs):
    if ('profile' in kwargs):
        if verbose:
            print("Using %s profile credentials..." % kwargs['profile'])

        # Get session for profile
        session = boto3.Session(profile_name=kwargs['profile'])

        # Get session
        cw_client = session.client('cloudwatch')
        s3_client = session.client('s3')

    else:
        if verbose:
            print("Using default profile credentials...")

        # Get session
        cw_client = boto3.client('cloudwatch')
        s3_client = boto3.client('s3')

    # Get a list of all buckets
    allbuckets = s3_client.list_buckets()

    return (cw_client, s3_client, allbuckets)

# Function definition for getting all bucket info
def get_bucket_storage(bucket, session):
    # get global total
    global total, raw_bytes, verbose

    # Assign session clients
    cw_client = session[0]
    s3_client = session[1]

    # For each bucket item, look up the cooresponding metrics from CloudWatch
    for st_type in storage_types:
            response = cw_client.get_metric_statistics(Namespace='AWS/S3',
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
                
                if raw_bytes:
                    # bucket_bytes = str(int(item["Average"]))
                    bucket_bytes = str("{:,}".format(int(item["Average"])))
                else:
                    bucket_bytes = humansize(item["Average"])
                
                if verbose:
                    print(bucket_name.ljust(f_col_width) +
                          bucket_bytes.rjust(l_col_width))

                # Add to running total
                total += int(item["Average"])

def print_help():
    print("This is the help message for now...")


## Main function
def main(argv):
    # get global variables
    global verbose
    global single_thread
    global raw_bytes
    global profile
    global num_workers

    ###################
    ## PARAMETER OPTIONS

    # Show help
    if ("--help" in argv) or ("-h") in argv:
        print_help()
        sys.exit(0)

    # Set number of concurrent workers
    if any("--workers" in a for a in argv):
        ## pull number of workers from arg and set global variable
        num_workers = int([a for a in argv if "--workers" in a][0][10:])
    
    # Set single threaded mode
    if "--single-thread" in argv:
        single_thread = True

    # Show raw byte values
    if "--raw-bytes" in argv:
        raw_bytes = True

    # Get AWS CLI profile
    if any("--profile" in a for a in argv):
        profile = [a for a in argv if "--profile" in a][0][10:]

    # Turn on verbose mode
    if ("-v" in argv) or ("--verbose" in argv):
        verbose = True
        if raw_bytes:
            size_str = "Size in Bytes"
        else:
            size_str = "Size"
        # Header Line for the output going to standard out
        print('Bucket'.ljust(f_col_width) + size_str.rjust(l_col_width))
        print('-' * (f_col_width + l_col_width))

    ##################
    ## EXECUTION PHASE

    if profile:
        session = get_session(profile=profile)
    else:
        session = get_session()

    # Store returned buckets
    allbuckets = session[2]

    # Execute get bucket storage
    if single_thread:
        for bucket in allbuckets['Buckets']:
                get_bucket_storage(bucket, session)
    else:
        # Use multi-threading to make it faster
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executer:
            future_to_bucket = {executer.submit(get_bucket_storage, bucket, session): bucket for bucket in allbuckets['Buckets']}

    #################
    ## RESULTS

    if verbose:
        print('-' * (f_col_width + l_col_width))

    print("Total stored in S3:".ljust(f_col_width) +
        humansize(total).rjust(l_col_width))
    print("Total bytes stored in S3:".ljust(f_col_width) +
        str("{:,}".format(int(total))).rjust(l_col_width) + " bytes")


if __name__ == "__main__":
    main(sys.argv[1:])
