import boto3
import os
import time
from ultralytics import YOLO
from botocore.exceptions import ClientError

# env vars
QUEUE_URL = os.environ.get("QUEUE_URL")
INPUT_BUCKET = os.environ.get("INPUT_BUCKET")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
MODEL_PATH = os.environ.get("MODEL_PATH")
AWS_REGION = os.environ.get("AWS_REGION")

if not QUEUE_URL or not INPUT_BUCKET or not OUTPUT_BUCKET:
    raise ValueError("Missing required environment variables")

# AWS clients
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

# load model once
model = YOLO(MODEL_PATH)

def process_image(key):
    local_input = "/tmp/input.jpg"
    local_output_dir = "/tmp/output"

    # download image
    s3.download_file(INPUT_BUCKET, key, local_input)

    # run YOLO
    model.predict(
        local_input,
        conf=0.8,
        iou=0.5,
        save=True,
        project=local_output_dir,
        name="result",
        exist_ok=True
    )

    # find output file
    out_dir = os.path.join(local_output_dir, "result")
    files = os.listdir(out_dir)
    if not files:
        print("No output generated")
        return

    output_path = os.path.join(out_dir, files[0])

    # upload to output bucket
    s3.upload_file(output_path, OUTPUT_BUCKET, key)

    print(f"Done: {key}")

def worker_loop():
    while True:
        msgs = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        if "Messages" not in msgs:
            continue

        msg = msgs["Messages"][0]
        receipt = msg["ReceiptHandle"]

        body = eval(msg["Body"])
        record = body["Records"][0]
        key = record["s3"]["object"]["key"]

        try:
            process_image(key)
        except Exception as e:
            print("Error:", e)

        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)

        time.sleep(1)

if __name__ == "__main__":
    worker_loop()
