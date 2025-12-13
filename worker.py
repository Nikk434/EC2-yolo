import boto3
import os
import time
from ultralytics import YOLO
from botocore.exceptions import ClientError

print("Starting YOLO worker...")

# env vars
QUEUE_URL = os.environ.get("QUEUE_URL")
INPUT_BUCKET = os.environ.get("INPUT_BUCKET")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
MODEL_PATH = os.environ.get("MODEL_PATH")
AWS_REGION = os.environ.get("AWS_REGION")

print("ENV CHECK")
print("QUEUE_URL:", QUEUE_URL)
print("INPUT_BUCKET:", INPUT_BUCKET)
print("OUTPUT_BUCKET:", OUTPUT_BUCKET)
print("MODEL_PATH:", MODEL_PATH)
print("AWS_REGION:", AWS_REGION)

if not QUEUE_URL or not INPUT_BUCKET or not OUTPUT_BUCKET:
    raise ValueError("Missing required environment variables")

print("Creating AWS clients...")

# AWS clients
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

print("Loading YOLO model...")
model = YOLO(MODEL_PATH)
print("Model loaded successfully")

def process_image(key):
    print("Processing image:", key)

    local_input = "/tmp/input.jpg"
    local_output_dir = "/tmp/output"

    print("Downloading from S3...")
    s3.download_file(INPUT_BUCKET, key, local_input)
    print("Downloaded to:", local_input)

    print("Running YOLO inference...")
    model.predict(
        local_input,
        conf=0.8,
        iou=0.5,
        save=True,
        project=local_output_dir,
        name="result",
        exist_ok=True
    )
    print("YOLO inference done")

    out_dir = os.path.join(local_output_dir, "result")
    print("Checking output dir:", out_dir)

    if not os.path.exists(out_dir):
        print("Output directory not found")
        return

    files = os.listdir(out_dir)
    print("Output files:", files)

    if not files:
        print("No output generated")
        return

    output_path = os.path.join(out_dir, files[0])
    print("Uploading result to S3:", output_path)

    s3.upload_file(output_path, OUTPUT_BUCKET, key)
    print("Upload complete")

def worker_loop():
    print("Entering worker loop...")

    while True:
        print("Waiting for SQS message...")
        msgs = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        if "Messages" not in msgs:
            print("No messages received")
            continue

        msg = msgs["Messages"][0]
        receipt = msg["ReceiptHandle"]

        print("Message received")

        try:
            body = eval(msg["Body"])
            record = body["Records"][0]
            key = record["s3"]["object"]["key"]
            print("S3 object key:", key)
        except Exception as e:
            print("Failed to parse SQS message:", e)
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
            continue

        try:
            process_image(key)
        except Exception as e:
            print("Error during processing:", e)

        print("Deleting SQS message")
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)

        print("Sleeping...\n")
        time.sleep(1)

if __name__ == "__main__":
    worker_loop()
