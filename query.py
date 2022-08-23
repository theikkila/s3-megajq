import argparse
import concurrent.futures
from botocore.client import Config
import boto3
import time
import tempfile

config = Config(connect_timeout=5, read_timeout=5, retries={'max_attempts': 1})

s3 = boto3.client('s3', config=config)

parser = argparse.ArgumentParser(description='MegaJQ runs SQL-statements through S3-prefixes parallel')

parser.add_argument("bucket", help="S3 bucket where select will be run")
parser.add_argument("prefix", help="S3 key prefix")
parser.add_argument("query", help="SQL-query, see https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html")
parser.add_argument("output", help="Output file")

args = parser.parse_args()
print(args)

BUCKET = args.bucket
PREFIX = args.prefix
query = args.query

OUTPUT = args.output

out = open(OUTPUT, 'w')

def get_object_keys(bucket, prefix):
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in page_iterator:
        for obj in page['Contents']:
            yield obj['Key']

TOTAL_BYTES_SCANNED = 0
TOTAL_BYTES_PROCESSED = 0
TOTAL_BYTES_RETURNED = 0
MB = 1024*1024
GB = 1024*MB

current_jobs = 0
MAX_JOBS = 30

def query_bucket(bucket, k, query):
    global current_jobs
    while True:
        if current_jobs < MAX_JOBS:
            current_jobs += 1
            break
        time.sleep(0.1)
    fp = tempfile.TemporaryFile(mode='w+')
    r = s3.select_object_content(
                Bucket=BUCKET,
                Key=k,
                ExpressionType='SQL',
                Expression=query,
                InputSerialization = {'JSON': {"Type": "LINES"}, "CompressionType": "NONE"},
                OutputSerialization = {'JSON': {"RecordDelimiter": "\n"}},
        )
    stats = None
    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            fp.write(records)
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            # print(event)
            stats = statsDetails
    r['Payload'].close()
    fp.seek(0)
    return stats, fp

start_time = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_JOBS) as executor:
    #for k in get_object_keys(BUCKET, PREFIX):
    future_to_result = {executor.submit(query_bucket, BUCKET, k, query): k for k in get_object_keys(BUCKET, PREFIX)}
    total_objects = len(future_to_result)
    processed_objects = 0
    for future in concurrent.futures.as_completed(future_to_result):
        try:
            statsDetails, events = future.result()
            TOTAL_BYTES_SCANNED += statsDetails.get('BytesScanned', 0)
            TOTAL_BYTES_PROCESSED += statsDetails.get('BytesProcessed', 0)
            TOTAL_BYTES_RETURNED += statsDetails.get('BytesReturned', 0)
            scanned = statsDetails.get('BytesScanned', 0)//MB
            processed = statsDetails.get('BytesProcessed', 0)//MB
            returned = statsDetails.get('BytesReturned', 0)/MB
            speed = TOTAL_BYTES_PROCESSED/(time.time()-start_time)
            # print("== Stats details scanned: {}MB processed: {}MB returned: {}MB ".format(scanned, processed, returned))
            print("== Total {}/{} scanned ({}MB/s): {}GB processed: {}GB returned: {}MB ".format(processed_objects, total_objects, speed//MB, TOTAL_BYTES_SCANNED//GB, TOTAL_BYTES_PROCESSED//GB, TOTAL_BYTES_RETURNED//MB))
            processed_objects += 1
            for event in events.readlines():
                out.write(event)
            events.close()
            current_jobs -= 1
            # print(current_jobs)

        except Exception as exc:
            print('generated an exception: %s' % (exc))
            #raise exc