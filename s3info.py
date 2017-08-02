#!/usr/bin/python

#
# Copyright 2017 Jameson Ricks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Original idea taken from http://www.slsmk.com/getting-the-size-of-an-s3-bucket-using-boto3-for-aws
# Added human readable output, thread concurrency, profile support, and easy ability to pipe total to another program

import sys
import datetime
import collections
import concurrent.futures

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print('''
ERROR:  You must have the boto3 package installed in your python environment to run 
        this script! You can install it by running:
            pip install boto3
        ''')
    sys.exit(1)


class Session:
    ## Default variables
    now = datetime.datetime.now()
    num_workers = 10
    quiet = False
    single_thread = False
    raw_bytes = False
    profile = False
    no_comma = False
    report_mode = False
    profile = None
    region_csv = False

    # Output column widths
    f_col_width = 85
    l_col_width = 25

    # Array of different S3 Storage types
    storage_types = ['StandardStorage', 'StandardIAStorage',
                     'ReducedRedundancyStorage', 'GlacierObjectOverhead']
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']  # For human readable

    # Keep running total
    total = 0
    total_objects = 0

    # Strings
    size_str = "Size"
    # Header Line for the output going to standard out
    header = '\nBucket'.ljust(f_col_width) + size_str.rjust(l_col_width) + "\n"
    header += '-' * (f_col_width + l_col_width)

    # Buckets
    all_buckets = {}

    # Results
    results = collections.OrderedDict()

    def get_bucket_region(self, bucket, s3_client):
        try:
            region = s3_client.get_bucket_location(Bucket=bucket)["LocationConstraint"]
        except ClientError as e:
            if not self.quiet:
                print("Error on bucket: %s" % bucket)
                print(e)
            return

        if region == None:
            region = 'us-east-1'

        self.all_buckets[bucket] = region

    # Gets the correct cloudwatch client depending on the region
    def get_cloudwatch_client(self, bucket, session):
        if session.region_name == self.all_buckets[bucket]:
            cw_client = session.client('cloudwatch')
            return cw_client
        else:
            new_session = boto3.Session(region_name=self.all_buckets[bucket])
            cw_client = new_session.client('cloudwatch')
            return cw_client

    # Outputs the results of the process
    def print_results(self):

        if self.region_csv:
            self.print_regions_csv()
            return

        # Print header line
        if not self.quiet:
            print(self.header)
        # Sort the buckets
        results_sorted = collections.OrderedDict(sorted(self.results.items()))
        for bucket in results_sorted:
            for st_type in results_sorted[bucket]:
                if st_type != "NumberOfObjects":
                    # Print storage type with bucket
                    bucket_name = "%s (%s)" % (bucket, st_type)
                    # Append number of items per bucket
                    bucket_bytes = "(" + str(results_sorted[bucket]["NumberOfObjects"])
                    if int(results_sorted[bucket]["NumberOfObjects"]) > 1:
                        bucket_bytes += " Items) "
                    else:
                        bucket_bytes += " Item) "
                    # Check arguments to ensure correct output
                    if self.raw_bytes:
                        if self.no_comma:
                            bucket_bytes += str(results_sorted[bucket][st_type])
                        else:
                            bucket_bytes += str("{:,}".format(results_sorted[bucket][st_type]))
                    else:
                        bucket_bytes += humansize(results_sorted[bucket][st_type], self.suffixes)
                    # Print out each line
                    if not self.quiet:
                        print(bucket_name.ljust(self.f_col_width) +
                            bucket_bytes.rjust(self.l_col_width))
        
        if not self.quiet:
            print('-' * (self.f_col_width + self.l_col_width))

        if self.report_mode:
            print(self.total)
        else:
            if self.raw_bytes:
                if self.no_comma:
                    print("Total bytes stored in S3:".ljust(self.f_col_width) + str(int(self.total)).rjust(self.l_col_width))
                else:
                    print("Total bytes stored in S3:".ljust(self.f_col_width) + str("{:,}".format(int(self.total))).rjust(self.l_col_width))
            else:
                print("Total stored in S3:".ljust(self.f_col_width) + humansize(self.total, self.suffixes).rjust(self.l_col_width))

    # Prints the region totals in csv format
    def print_regions_csv(self):
        header_line = "Region,"
        for st_type in self.storage_types:
            header_line += st_type + ","

        header_line += "Total Files,Total Bytes,"

        print(header_line)

        results = dict(self.results)

        totals = collections.OrderedDict()

        for bucket in self.all_buckets:
            if bucket in results:
                # Check if region name is already stored in dictionary
                if self.all_buckets[bucket] not in totals:
                    # Region Totals
                    totals[self.all_buckets[bucket]] = {
                        "StandardStorage": 0,
                        "StandardIAStorage": 0,
                        "ReducedRedundancyStorage": 0,
                        "GlacierObjectOverhead": 0,
                        "TotalFiles": 0,
                        "TotalBytes": 0,
                    }

                if 'StandardStorage' in results[bucket]:
                    totals[self.all_buckets[bucket]]['StandardStorage'] += results[bucket]['StandardStorage']
                    totals[self.all_buckets[bucket]]['TotalBytes'] += results[bucket]['StandardStorage']

                if 'StandardIAStorage' in results[bucket]:
                    totals[self.all_buckets[bucket]]['StandardIAStorage'] += results[bucket]['StandardIAStorage']
                    totals[self.all_buckets[bucket]]['TotalBytes'] += results[bucket]['StandardIAStorage']

                if 'ReducedRedundancyStorage' in results[bucket]:
                    totals[self.all_buckets[bucket]]['ReducedRedundancyStorage'] += results[bucket]['ReducedRedundancyStorage']
                    totals[self.all_buckets[bucket]]['TotalBytes'] += results[bucket]['ReducedRedundancyStorage']

                if 'GlacierObjectOverhead' in results[bucket]:
                    totals[self.all_buckets[bucket]]['GlacierObjectOverhead'] += results[bucket]['GlacierObjectOverhead']
                    totals[self.all_buckets[bucket]]['TotalBytes'] += results[bucket]['GlacierObjectOverhead']

                if 'NumberOfObjects' in results[bucket]:
                    totals[self.all_buckets[bucket]]['TotalFiles'] += results[bucket]['NumberOfObjects']


        for region in totals:
            line = "%s," % region
            line += "%s," % totals[region]['StandardStorage']
            line += "%s," % totals[region]['StandardIAStorage']
            line += "%s," % totals[region]['ReducedRedundancyStorage']
            line += "%s," % totals[region]['GlacierObjectOverhead']
            line += "%s," % totals[region]['TotalFiles']
            line += "%s," % totals[region]['TotalBytes']
            print(line)


    # Function definition for getting all bucket info
    def get_bucket_storage(self, bucket, aws_session):
        # Get correct CloudWatch client
        cw_client = self.get_cloudwatch_client(bucket, aws_session)
        # For each bucket item, look up the total size from CloudWatch
        for st_type in self.storage_types:
            response = cw_client.get_metric_statistics(Namespace='AWS/S3',
                                                        MetricName='BucketSizeBytes',
                                                        Dimensions=[
                                                            {'Name': 'BucketName',
                                                            'Value': bucket},
                                                            {'Name': 'StorageType',
                                                            'Value': st_type},
                                                        ],
                                                        Statistics=['Average'],
                                                        Period=3600,
                                                        StartTime=(
                                                            self.now - datetime.timedelta(days=1)).isoformat(),
                                                       EndTime=self.now.isoformat()
                                                        )
            # The cloudwatch metrics will have the single datapoint, so we just report on it.
            for item in response["Datapoints"]:
                # Create a blank dictionary if we don't have anything yet.
                if bucket not in self.results:
                    self.results[bucket] = {}

                self.results[bucket][st_type] = int(item["Average"])

                # Add to running total
                self.total += int(item["Average"])
        
        # For each bucket item, look up the total size from CloudWatch
        response = cw_client.get_metric_statistics(Namespace='AWS/S3',
                                                    MetricName='NumberOfObjects',
                                                    Dimensions=[
                                                        {'Name': 'BucketName',
                                                        'Value': bucket},
                                                        {'Name': 'StorageType',
                                                        'Value': 'AllStorageTypes'},
                                                    ],
                                                    Statistics=['Average'],
                                                    Period=3600,
                                                    StartTime=(
                                                        self.now - datetime.timedelta(days=1)).isoformat(),
                                                    EndTime=self.now.isoformat()
                                                    )
        # The cloudwatch metrics will have the single datapoint, so we just report on it.
        for item in response["Datapoints"]:
            # Create a blank dictionary if we don't have anything yet.
            if bucket not in self.results:
                self.results[bucket] = {}

            self.results[bucket]["NumberOfObjects"] = int(item["Average"])

            # Add to running total
            self.total_objects += int(item["Average"])

# Gets all s3 buckets in region for session
def get_s3_buckets(session_obj, session):

    if not session_obj.quiet:
        print("Getting S3 bucket information...")

    s3_client = get_s3_client(session)

    # Get bucket location
    if session_obj.single_thread:
        for bucket in s3_client.list_buckets()['Buckets']:
            session_obj.get_bucket_region(bucket['Name'], s3_client)
    else:
        # Use multi-threading to make it faster
        with concurrent.futures.ThreadPoolExecutor(max_workers=session_obj.num_workers) as executer:
            future_to_bucket = {executer.submit(session_obj.get_bucket_region, bucket['Name'], s3_client): bucket for bucket in s3_client.list_buckets()['Buckets']}

# Loops through each s3 Bucket
def list_bucket_info(session_obj, session):
    if session_obj.single_thread:
        for bucket in session_obj.all_buckets:
                session_obj.get_bucket_storage(bucket, session)
    else:
        # Use multi-threading to make it faster
        with concurrent.futures.ThreadPoolExecutor(max_workers=session_obj.num_workers) as executer:
            future_to_bucket = {executer.submit(
                session_obj.get_bucket_storage, bucket, session): bucket for bucket in session_obj.all_buckets}


# For parsing input arguments
def parse_args(argv, session):
     # Show help
    if ("--help" in argv) or ("-h") in argv:
        print_help()
        sys.exit(0)

    # Set number of concurrent workers
    if any("--workers" in a for a in argv):
        ## pull number of workers from arg and set global variable
        session.num_workers = int(
            [a for a in argv if "--workers" in a][0][10:])

    # Set single threaded mode
    if "--single-thread" in argv:
        session.single_thread = True

    # Show raw byte values
    if ("--raw-bytes" in argv) or ("-r" in argv):
        session.raw_bytes = True

    # Don't output commas
    if ("--no-commas" in argv) or ("--no-comma" in argv) or ("-nc" in argv):
        session.no_comma = True

    # Get AWS CLI profile
    if any("--profile" in a for a in argv):
        session.profile = [a for a in argv if "--profile" in a][0][10:]

    # Turn on quiet mode
    if ("-q" in argv) or ("--quiet" in argv):
        session.quiet = True

    # Turn on report mode
    if "--report-mode" in argv:
        session.report_mode = True
        session.quiet = True
        session.no_comma = True
        session.raw_bytes = True

    if "--region-csv" in argv:
        session.report_mode = True
        session.quiet = True
        session.no_comma = True
        session.raw_bytes = True
        session.region_csv = True

    if session.raw_bytes:
        session.size_str = "Size in Bytes"

# Prints human readable sizes (source: https://stackoverflow.com/questions/14996453/python-libraries-to-calculate-human-readable-filesize-from-bytes)
def humansize(nbytes, suffixes):
    i = 0
    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])

# Getting the AWS session
def get_boto_session(session):
    aws_session = boto3.Session()

    if session.profile:
        # Get session for profile
        aws_session = boto3.Session(profile_name=session.profile)

    return aws_session

# Get S3 Client
def get_s3_client(aws_session):
    return aws_session.client('s3')


def print_help():
    print('''usage: ./s3-info.py    [-h | --help] [-q | --quiet] [--profile=<profile>]
                        [--workers=<# of threads>] [--single-thread] [--raw-bytes] 
                        [--no-commas] [--report-mode]
    ''')

    print('''DESCRIPTION
Use this tool to output your total S3 usage in an AWS account. It can display your total usage
in human readable format (e.g., KB, MB, GB, TB, PB) or in total bytes. You can display usage per 
bucket according to type of storage (Standard, Infrequently Accessed, Reduced Redundancy, or
Glacier Objects). Buckets that have multiple types of storage will be listed twice. This tool 
uses concurrent threads to speed up the process. As such, buckets will not be listed
in alphabetical order unless you run with the --single-thread flag. By default, this tool uses 
the default profile stored in your ~/.aws/config file. IMPORTANT: Make sure you have a default
region associated with your profile!

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
        
        --no-comma OR -nc       Used in conjuction with --raw-bytes, does not output commas in
                                numbers.

        --region-csv            Prints out a csv format of aggregated data by region. This option 
                                automatically turns on --quiet mode. It's best to pipe this output
                                to a file. Example:

                                        ./s3info.py --region-csv > s3-report.csv

                                NOTE: This mode will suppress all AccessDenied errors for buckets. 
                                Make sure the access keys/profile you are using has permission to 
                                read all your buckets.
                            
        --report-mode           This option only outputs the number of bytes without commas to
                                the console. This allows the output to be piped to a variable,
                                function, etc. This option automatically turns on --quiet,  
                                --raw-bytes, and --no-comma flags. NOTE: This mode will suppress 
                                all AccessDenied errors for buckets. Make sure the access keys/profile
                                you are using has permission to read all your buckets.
    ''')


## Main function
def main(argv):

    session = Session()

    parse_args(argv, session)

    aws_session = get_boto_session(session)

    get_s3_buckets(session, aws_session)

    list_bucket_info(session, aws_session)

    session.print_results()


if __name__ == "__main__":
    main(sys.argv[1:])
