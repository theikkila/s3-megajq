## MegaJQ

Use S3 SELECT parallel for multiple objects.


## Installation

```
pip install -r requirements.txt
```

## Usage
```
python query.py -h
usage: query.py [-h] bucket prefix query output

MegaJQ runs SQL-statements through S3-prefixes parallel

positional arguments:
  bucket      S3 bucket where select will be run
  prefix      S3 key prefix
  query       SQL-query, see
              https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-
              select-sql-reference-select.html
  output      Output file

optional arguments:
  -h, --help  show this help message and exit
```

## Example

```
python query.py event-bucket-prod 'prod-events/yyyy=2020/mm=04/dd=03/' "select * from s3Object[*] r WHERE r.meta.path like 'seek-123'" output.jsonl

== Total 221/240 scanned (4148.0MB/s): 118GB processed: 118GB returned: 66MB 
== Total 222/240 scanned (4160.0MB/s): 119GB processed: 119GB returned: 66MB 
== Total 223/240 scanned (4172.0MB/s): 119GB processed: 119GB returned: 67MB 
== Total 224/240 scanned (4184.0MB/s): 120GB processed: 120GB returned: 67MB 
== Total 225/240 scanned (4178.0MB/s): 120GB processed: 120GB returned: 67MB 
== Total 226/240 scanned (4185.0MB/s): 120GB processed: 120GB returned: 67MB 
== Total 227/240 scanned (4194.0MB/s): 120GB processed: 120GB returned: 68MB 
== Total 228/240 scanned (4202.0MB/s): 121GB processed: 121GB returned: 68MB 
== Total 229/240 scanned (4208.0MB/s): 121GB processed: 121GB returned: 68MB 
== Total 230/240 scanned (4215.0MB/s): 121GB processed: 121GB returned: 68MB 
== Total 231/240 scanned (4221.0MB/s): 121GB processed: 121GB returned: 68MB 
== Total 232/240 scanned (4226.0MB/s): 121GB processed: 121GB returned: 68MB 
== Total 233/240 scanned (4231.0MB/s): 121GB processed: 121GB returned: 69MB 
== Total 234/240 scanned (4237.0MB/s): 122GB processed: 122GB returned: 69MB 
== Total 235/240 scanned (4243.0MB/s): 122GB processed: 122GB returned: 69MB 
== Total 236/240 scanned (4252.0MB/s): 122GB processed: 122GB returned: 69MB 
== Total 237/240 scanned (4257.0MB/s): 122GB processed: 122GB returned: 69MB 
== Total 238/240 scanned (4263.0MB/s): 122GB processed: 122GB returned: 69MB 
== Total 239/240 scanned (4267.0MB/s): 122GB processed: 122GB returned: 70MB 
== Total 240/240 scanned (4272.0MB/s): 123GB processed: 123GB returned: 70MB 
```