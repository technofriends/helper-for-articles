#!/usr/bin/env python
import boto3
from faker import Faker
import random
import time
import json

DeliveryStreamName = 'product_ratings-kfh'
client = boto3.client('firehose')
fake = Faker()

product_ids = [
    "T-100001",
    "T-100002",
    "T-100003",
    "T-100004",
    "M-110001",
    "M-110002",
    "M-110003",
    "M-110004",
    "D-120001",
    "D-120002",
    "E-190032",
    "E-190132",
    "E-190232",
    "E-190332"

];

record = {}
while True:

    record['user'] = fake.name();
    if random.randint(1,100) < 5:
        record['product_id'] = "VAIBHAVPANDEY-10001";
        record['rating'] = random.randint(8500,9500);
    else:
        record['product_id'] = random.choice(product_ids);
        record['rating'] = random.randint(1, 1000);
    record['timestamp'] = time.time();
    response = client.put_record(
        DeliveryStreamName=DeliveryStreamName,
        Record={
            'Data': json.dumps(record)
        }
    )
    print('Record: ' + str(record));
