import boto3
import os
import json
import ssl
from ultralytics import YOLO
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

load_dotenv()

print("Starting YOLO processor (AWS IoT Core + S3 + Secure Certs)...")

# === Environment Variables ===
INPUT_BUCKET = os.environ.get("INPUT_BUCKET")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
MODEL_PATH = os.environ.get("MODEL_PATH")
AWS_REGION = os.environ.get("AWS_REGION")
IMAGE_KEY = os.environ.get("IMAGE_KEY")  # Optional: specific image key

# MQTT Settings
MQTT_BROKER = os.environ.get("MQTT_BROKER")          # Required: your AWS IoT endpoint
MQTT_PORT = int(os.environ.get("MQTT_PORT", 8883))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "yolo/detections/test")

# Certificate paths from .env (secure & flexible)
ROOT_CA_PATH = os.environ.get("CERT_ROOT_CA")
CERT_PATH = os.environ.get("CERT_DEVICE")
KEY_PATH = os.environ.get("CERT_KEY")

# Print env check (never prints secrets)
print("ENV CHECK")
print("INPUT_BUCKET:", INPUT_BUCKET)
print("OUTPUT_BUCKET:", OUTPUT_BUCKET)
print("MODEL_PATH:", MODEL_PATH)
print("AWS_REGION:", AWS_REGION)
print("IMAGE_KEY:", IMAGE_KEY)
print("MQTT_BROKER:", MQTT_BROKER)
print("MQTT_PORT:", MQTT_PORT)
print("MQTT_TOPIC:", MQTT_TOPIC)
print("Cert paths configured:", bool(ROOT_CA_PATH and CERT_PATH and KEY_PATH))

# Validate required vars
required = [INPUT_BUCKET, OUTPUT_BUCKET, MODEL_PATH, MQTT_BROKER, ROOT_CA_PATH, CERT_PATH, KEY_PATH]
if not all(required):
    raise ValueError("Missing one or more required environment variables")

# Check if cert files exist
for path, name in [(ROOT_CA_PATH, "Root CA"), (CERT_PATH, "Device Cert"), (KEY_PATH, "Private Key")]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{name} not found at: {path}")

# === AWS S3 Client ===
print("Creating S3 client...")
s3 = boto3.client("s3", region_name=AWS_REGION)

# === Secure MQTT Client for AWS IoT Core ===
print("Setting up secure MQTT client...")
mqtt_client = mqtt.Client(client_id="yolo-worker-client-001")
mqtt_client.tls_set(
    ca_certs=ROOT_CA_PATH,
    certfile=CERT_PATH,
    keyfile=KEY_PATH,
    tls_version=ssl.PROTOCOL_TLSv1_2
)

try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()  # Background thread for reliable connection
    print(f"Successfully connected to AWS IoT Core: {MQTT_BROKER}:{MQTT_PORT}")
except Exception as e:
    raise ConnectionError(f"Failed to connect to AWS IoT Core: {e}")

# === Load YOLO Model ===
print("Loading YOLO model...")
model = YOLO(MODEL_PATH)
print("Model loaded successfully!")

# === Helper Functions ===
def get_single_object_key(bucket):
    response = s3.list_objects_v2(Bucket=bucket, MaxKeys=10)
    if 'Contents' not in response or len(response['Contents']) == 0:
        raise ValueError(f"No objects found in bucket {bucket}")
    if len(response['Contents']) > 1:
        print(f"Warning: Multiple objects in {bucket}. Using the first one.")
    key = response['Contents'][0]['Key']
    print(f"Selected image: {key}")
    return key

def process_image(key):
    print(f"\n--- Processing image: {key} ---")

    local_input = "/tmp/input.jpg"
    local_output_dir = "/tmp/output"
    json_output_path = "/tmp/result.json"

    os.makedirs("/tmp", exist_ok=True)
    os.makedirs(local_output_dir, exist_ok=True)

    # Download image
    print("Downloading from S3...")
    s3.download_file(INPUT_BUCKET, key, local_input)

    # Run inference
    print("Running YOLO inference...")
    results = model.predict(
        source=local_input,
        conf=0.8,
        iou=0.5,
        save=True,
        project=local_output_dir,
        name="result",
        exist_ok=True,
        verbose=False
    )
    print("Inference complete")

    # Find annotated image
    out_dir = os.path.join(local_output_dir, "result")
    annotated_files = [f for f in os.listdir(out_dir) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
    if not annotated_files:
        print("No annotated image generated")
        return
    annotated_path = os.path.join(out_dir, annotated_files[0])

    # Build detection payload
    detections = []
    for r in results:
        if r.boxes is not None:
            for box in r.boxes:
                class_name = r.names[int(box.cls)]
                confidence = round(float(box.conf), 3)
                detections.append({
                    "class_name": class_name,
                    "confidence": confidence
                })

    status = "rec" if detections else "unrec"
    payload = {
        "status": status,
        "detections": detections
    }

    # Save JSON locally
    with open(json_output_path, "w") as f:
        json.dump(payload, f, indent=2)

    # Upload to S3
    output_image_key = key
    output_json_key = os.path.splitext(key)[0] + ".json"

    print("Uploading annotated image to S3...")
    s3.upload_file(annotated_path, OUTPUT_BUCKET, output_image_key)
    print("Uploading JSON payload to S3...")
    s3.upload_file(json_output_path, OUTPUT_BUCKET, output_json_key)

    # Publish to AWS IoT Core MQTT
    print(f"Publishing detection event to topic: {MQTT_TOPIC}")
    try:
        result = mqtt_client.publish(MQTT_TOPIC, json.dumps(payload), qos=1)
        result.wait_for_publish()
        print("Successfully published to AWS IoT Core!")
    except Exception as e:
        print(f"MQTT publish failed: {e}")

# === Main Execution ===
def main():
    print("\n=== Starting Single Image Processing ===")
    try:
        key = IMAGE_KEY.strip() if IMAGE_KEY else get_single_object_key(INPUT_BUCKET)
        process_image(key)
        print("\n=== Processing Completed Successfully! ===")
    except Exception as e:
        print(f"\n!!! Error during processing: {e} !!!")
    finally:
        print("Cleaning up MQTT connection...")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("MQTT disconnected.")

if __name__ == "__main__":
    main()