import boto3
import json
import time

QUEUE_URL = ""
REGION = "ap-south-1"

session = boto3.Session(profile_name="", region_name="ap-south-1")
sqs = session.client("sqs")

print("Waiting for SQS messages... Upload a file to S3 now.\n")

while True:
    resp = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20
    )

    if "Messages" not in resp:
        print("No message yet...")
        continue

    msg = resp["Messages"][0]
    body = json.loads(msg["Body"])

    print("\n=== RAW MESSAGE ===")
    print(json.dumps(body, indent=2))

    record = body["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    print("\nParsed values:")
    print("Bucket:", bucket)
    print("Key:", key)

    # delete message so it doesn't repeat
    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=msg["ReceiptHandle"]
    )

    print("\nMessage processed and deleted.")
    break
