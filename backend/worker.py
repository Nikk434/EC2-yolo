import boto3
import os
import time
from ultralytics import YOLO
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

print("Starting YOLO processor (single image mode)...")

# Environment variables
INPUT_BUCKET = os.environ.get("INPUT_BUCKET")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
MODEL_PATH = os.environ.get("MODEL_PATH")
AWS_REGION = os.environ.get("AWS_REGION")
IMAGE_KEY = os.environ.get("IMAGE_KEY")  # Optional: specify exact key, e.g. "input.jpg"

print("ENV CHECK")
print("INPUT_BUCKET:", INPUT_BUCKET)
print("OUTPUT_BUCKET:", OUTPUT_BUCKET)
print("MODEL_PATH:", MODEL_PATH)
print("AWS_REGION:", AWS_REGION)
print("IMAGE_KEY (optional):", IMAGE_KEY)

if not INPUT_BUCKET or not OUTPUT_BUCKET or not MODEL_PATH:
    raise ValueError("Missing required environment variables: INPUT_BUCKET, OUTPUT_BUCKET, MODEL_PATH")

# AWS clients
print("Creating AWS clients...")
s3 = boto3.client("s3", region_name=AWS_REGION)

print("Loading YOLO model...")
model = YOLO(MODEL_PATH)
print("Model loaded successfully")

def get_single_object_key(bucket):
    """Find the single object in the bucket (assumes only one image exists)"""
    response = s3.list_objects_v2(Bucket=bucket, MaxKeys=10)
    if 'Contents' not in response or len(response['Contents']) == 0:
        raise ValueError(f"No objects found in bucket {bucket}")
    if len(response['Contents']) > 1:
        print(f"Warning: Multiple objects found in {bucket}. Picking the first one.")
    
    key = response['Contents'][0]['Key']
    print(f"Found object in bucket: {key}")
    return key

def process_image(key):
    print(f"Processing image: {key}")

    local_input = "/tmp/input.jpg"
    local_output_dir = "/tmp/output"

    # Ensure /tmp exists (safe on Windows too in most cases, or use current dir)
    os.makedirs("/tmp", exist_ok=True)
    os.makedirs(local_output_dir, exist_ok=True)

    print("Downloading from S3...")
    s3.download_file(INPUT_BUCKET, key, local_input)
    print("Downloaded to:", local_input)

    print("Running YOLO inference...")
    results = model.predict(
        source=local_input,
        conf=0.8,
        iou=0.5,
        save=True,
        project=local_output_dir,
        name="result",
        exist_ok=True
    )
    print("YOLO inference completed")

    # Find the output image (usually the one with labels drawn)
    out_dir = os.path.join(local_output_dir, "result")
    if not os.path.exists(out_dir):
        print("Output directory not found!")
        return

    files = [f for f in os.listdir(out_dir) if f.endswith(('.jpg', '.jpeg', '.png'))]
    if not files:
        print("No output image generated")
        return

    output_path = os.path.join(out_dir, files[0])
    print(f"Result image ready: {output_path}")

    output_key = key  # Save with same name, or modify if needed: f"result_{key}"
    print(f"Uploading result to s3://{OUTPUT_BUCKET}/{output_key}")
    s3.upload_file(output_path, OUTPUT_BUCKET, output_key)
    print("Upload complete!")

def main():
    print("Starting single-image processing...")

    # Determine which image to process
    if IMAGE_KEY:
        key = IMAGE_KEY.strip()
        print(f"Using specified IMAGE_KEY: {key}")
    else:
        print("No IMAGE_KEY provided. Auto-detecting single object in input bucket...")
        key = get_single_object_key(INPUT_BUCKET)

    try:
        process_image(key)
        print("Processing completed successfully!")
    except Exception as e:
        print("Error during processing:", str(e))

if __name__ == "__main__":
    main()