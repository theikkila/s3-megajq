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
```