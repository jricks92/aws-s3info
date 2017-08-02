AWS S3 Info
===========

## Description
It's hard to find your total S3 usage in your account sometimes. I've written a python script to help with that.

Use this tool to output your total S3 usage in an AWS account. It can display your total usage
in human readable format (e.g., KB, MB, GB, TB, PB) or in total bytes. You can display usage per 
bucket according to type of storage (Standard, Infrequently Accessed, Reduced Redundancy, or
Glacier Objects). Buckets that have multiple types of storage will be listed twice. This tool 
uses concurrent threads to speed up the process. As such, buckets will not be listed
in alphabetical order unless you run with the --single-thread flag. By default, this tool uses 
the default profile stored in your ~/.aws/config file.

## Usage
```
usage: ./s3info.py  [-h | --help] [-q | --quiet] [--profile=<profile>]
                    [--workers=<# of threads>] [--single-thread] [--raw-bytes] 
                    [--no-commas] [--report-mode]

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
                                you are using has permission to read all your buckets..
```