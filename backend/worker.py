import os
import json
import ssl
import time
import boto3
from ultralytics import YOLO
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# ==========================================================
# ENV + BOOTSTRAP
# ==========================================================
load_dotenv("D:/EC2-yolo/backend/.env.local")

print("[BOOT] YOLO Worker starting (SQS driven)")

AWS_PROFILE = os.getenv("AWS_PROFILE")
AWS_REGION = os.getenv("AWS_REGION")

INPUT_BUCKET = os.getenv("INPUT_BUCKET")
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

MODEL_PATH = os.getenv("MODEL_PATH")

POLL_WAIT_TIME = int(os.getenv("POLL_WAIT_TIME", 20))
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", 120))

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "yolo/detection/op")

ROOT_CA_PATH = os.getenv("CERT_ROOT_CA")
CERT_PATH = os.getenv("CERT_DEVICE")
KEY_PATH = os.getenv("CERT_KEY")

required = [
    AWS_PROFILE, AWS_REGION,
    INPUT_BUCKET, OUTPUT_BUCKET, SQS_QUEUE_URL,
    MODEL_PATH,
    MQTT_BROKER, ROOT_CA_PATH, CERT_PATH, KEY_PATH
]

if not all(required):
    raise RuntimeError("Missing required environment variables")

print(f"[BOOT] Using AWS profile: {AWS_PROFILE}")

# ==========================================================
# AWS SESSION (PROFILE EXPLICIT)
# ==========================================================
session = boto3.Session(
    profile_name=AWS_PROFILE,
    region_name=AWS_REGION
)

s3 = session.client("s3")
sqs = session.client("sqs")

# ==========================================================
# MQTT SETUP
# ==========================================================
print("[BOOT] Connecting to AWS IoT Core")

mqtt_client = mqtt.Client(client_id="yolo-worker-ec2")
mqtt_client.tls_set(
    ca_certs=ROOT_CA_PATH,
    certfile=CERT_PATH,
    keyfile=KEY_PATH,
    tls_version=ssl.PROTOCOL_TLSv1_2
)

mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
mqtt_client.loop_start()

print("[BOOT] MQTT connected")

# ==========================================================
# YOLO LOAD
# ==========================================================
print("[BOOT] Loading YOLO model")
model = YOLO(MODEL_PATH)
print("[BOOT] Model ready")

# ==========================================================
# HELPERS
# ==========================================================
def process_message(message):
    body = json.loads(message["Body"])

    # S3 event → SQS format
    record = body["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    print(f"[JOB] Processing s3://{bucket}/{key}")

    local_input = "/tmp/input.jpg"
    output_dir = "/tmp/output"
    json_path = "/tmp/result.json"

    os.makedirs(output_dir, exist_ok=True)

    print("[S3] Downloading image")
    s3.download_file(bucket, key, local_input)

    print("[YOLO] Running inference")
    results = model.predict(
        source=local_input,
        conf=0.8,
        iou=0.5,
        save=True,
        project=output_dir,
        name="result",
        exist_ok=True,
        verbose=False
    )

    detections = []
    for r in results:
        if r.boxes:
            for box in r.boxes:
                detections.append({
                    "class_name": r.names[int(box.cls)],
                    "confidence": round(float(box.conf), 3)
                })

    status = "rec" if detections else "unrec"

    payload = {
        "image_key": key,
        "status": status,
        "detections": detections
    }

    out_img_dir = os.path.join(output_dir, "result")
    annotated_img = next(
        f for f in os.listdir(out_img_dir)
        if f.lower().endswith((".jpg", ".png"))
    )

    annotated_path = os.path.join(out_img_dir, annotated_img)

    print("[S3] Uploading outputs")
    s3.upload_file(annotated_path, OUTPUT_BUCKET, key)
    s3.upload_file(
        json_path,
        OUTPUT_BUCKET,
        os.path.splitext(key)[0] + ".json"
    )

    print("[MQTT] Publishing event")
    mqtt_client.publish(MQTT_TOPIC, json.dumps(payload), qos=1)

    print("[JOB] Done:", key)


# ==========================================================
# MAIN LOOP (LONG POLL)
# ==========================================================
print("[RUN] Waiting for SQS messages")

while True:
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=POLL_WAIT_TIME,
        VisibilityTimeout=VISIBILITY_TIMEOUT
    )

    messages = response.get("Messages", [])

    if not messages:
        continue

    message = messages[0]

    try:
        process_message(message)
        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message["ReceiptHandle"]
        )
        print("[SQS] Message deleted")
    except Exception as e:
        print("[ERROR]", e)
        print("[SQS] Message will retry after visibility timeout")
