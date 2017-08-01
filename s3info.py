#!/usr/bin/python

# Original idea taken from http://www.slsmk.com/getting-the-size-of-an-s3-bucket-using-boto3-for-aws/
# Modified by Jameson Ricks, Aug 1, 2017
# Added human readable output, thread concurrency, profile support, and easy ability to pipe total to another program

import sys
import datetime
import concurrent.futures

try:
    import boto3
except ImportError:
    print('''
ERROR:  You must have the boto3 package installed in your python environment to run 
        this script! You can install it by running:
            pip install boto3
        ''')
    sys.exit(1)


## Global variables
####################
now = datetime.datetime.now()
num_workers = 10
quiet = False
single_thread = False
raw_bytes = False
profile = False
no_comma = False
report_mode = False

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
        if not quiet:
            print("Using %s profile credentials..." % kwargs['profile'])

        # Get session for profile
        session = boto3.Session(profile_name=kwargs['profile'])

        # Get session
        cw_client = session.client('cloudwatch')
        s3_client = session.client('s3')

    else:
        if not quiet:
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
    global total, raw_bytes, quiet

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
                if no_comma:
                    bucket_bytes = str(int(item["Average"]))
                else:
                    bucket_bytes = str("{:,}".format(int(item["Average"])))
            else:
                bucket_bytes = humansize(item["Average"])
            
            if not quiet:
                print(bucket_name.ljust(f_col_width) +
                        bucket_bytes.rjust(l_col_width))

            # Add to running total
            total += int(item["Average"])

def print_help():
    print('''usage: ./s3-bucket-storage.py [-h | --help] [-q | --quiet] [--profile=<profile>]
                            [--workers=<# of threads>] [--single-thread] [--raw-bytes] [--no-commas]
                            [--report-mode]
    ''')

    print('''DESCRIPTION
Use this tool to output your total S3 usage in an AWS account. It can display your total usage
in human readable format (e.g., KB, MB, GB, TB, PB) or in total bytes. You can display usage per 
bucket according to type of storage (Standard, Infrequently Accessed, Reduced Redundancy, or
Glacier Objects). Buckets that have multiple types of storage will be listed twice. This tool 
uses concurrent threads to speed up the process. As such, buckets will not be listed
in alphabetical order unless you run with the --single-thread flag. By default, this tool uses 
the default profile stored in your ~/.aws/config file.

NOTE:   You must have the boto3 package installed in your python environment to correctly run this
        script.    

        -h OR --help            Shows this help message.

        -q OR --quiet           Supresses output for each bucket and only shows totals.

        --profile=<profile>     Uses the specified profile stored in your ~/.aws/config file.

        --workers=<number>      Specifies a specific number of threads to parse through each S3
                                bucket (default is 10). More threads may speed up the process if
                                you have a large number of S3 buckets in your account.

        --single-thread         Runs this script using one thread. This is useful if you want to
                                see all your buckets output in alphabetical order. Using this
                                flag will take longer to loop through all your S3 buckets.

        --raw-bytes             Using this option, you can output each bucket size in bytes
                                instead of KB, MB, GB, or PB.
        
        --no-comma OR -nc      Used in conjuction with --raw-bytes, does not output commas in
                                numbers.
                            
        --report-mode           This option only outputs the number of bytes without commas to
                                the console. This allows the output to be piped to a variable,
                                function, etc. This option automatically turns on --quiet,  
                                --raw-bytes, and --no-comma flags.
    ''')


## Main function
def main(argv):

    # get global variables
    global quiet
    global single_thread
    global raw_bytes
    global no_comma
    global profile
    global num_workers
    global report_mode

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
    if ("--raw-bytes" in argv) or ("-r" in argv):
        raw_bytes = True
    
    # Don't output commas
    if ("--no-commas" in argv) or ("--no-comma" in argv) or ("-nc" in argv):
        no_comma = True

    # Get AWS CLI profile
    if any("--profile" in a for a in argv):
        profile = [a for a in argv if "--profile" in a][0][10:]

    # Turn on quiet mode
    if ("-q" in argv) or ("--quiet" in argv):
        quiet = True

    # Turn on report mode
    if "--report-mode" in argv:
        report_mode = True
        quiet = True
        no_comma = True
        raw_bytes = True
    
    if raw_bytes:
        size_str = "Size in Bytes"
    else:
        size_str = "Size"
    # Header Line for the output going to standard out
    header = '\nBucket'.ljust(f_col_width) + size_str.rjust(l_col_width) + "\n"
    header +='-' * (f_col_width + l_col_width)

    ##################
    ## EXECUTION PHASE

    if profile:
        session = get_session(profile=profile)
    else:
        session = get_session()

    # Store returned buckets
    allbuckets = session[2]

    # Print header row
    if not quiet:
        print("Getting S3 bucket information...")
        print(header)

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

    if not quiet:
        print('-' * (f_col_width + l_col_width))

    if report_mode:
        print(total)
    else:
        if raw_bytes:
            if no_comma:
                print("Total bytes stored in S3:".ljust(f_col_width) + str(int(total)).rjust(l_col_width))
            else:
                print("Total bytes stored in S3:".ljust(f_col_width) + str("{:,}".format(int(total))).rjust(l_col_width))
        else:
            print("Total stored in S3:".ljust(f_col_width) +
            humansize(total).rjust(l_col_width))


if __name__ == "__main__":
    main(sys.argv[1:])
