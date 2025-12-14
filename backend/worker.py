import boto3
import os
import json
import ssl
import time
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
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 5))  # seconds

# MQTT Settings
MQTT_BROKER = os.environ.get("MQTT_BROKER")          # AWS IoT endpoint
MQTT_PORT = int(os.environ.get("MQTT_PORT", 8883))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "yolo/detections/test")

# Certificate paths
ROOT_CA_PATH = os.environ.get("CERT_ROOT_CA")
CERT_PATH = os.environ.get("CERT_DEVICE")
KEY_PATH = os.environ.get("CERT_KEY")

# Validate required vars
required = [INPUT_BUCKET, OUTPUT_BUCKET, MODEL_PATH, MQTT_BROKER, ROOT_CA_PATH, CERT_PATH, KEY_PATH, AWS_REGION]
if not all(required):
    raise ValueError("Missing one or more required environment variables")

# Check if cert files exist
for path, name in [(ROOT_CA_PATH, "Root CA"), (CERT_PATH, "Device Cert"), (KEY_PATH, "Private Key")]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{name} not found at: {path}")

# === AWS S3 Client ===
print("Creating S3 client...")
s3 = boto3.client("s3", region_name=AWS_REGION)

# === Secure MQTT Client ===
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
    mqtt_client.loop_start()
    print(f"Connected to AWS IoT Core: {MQTT_BROKER}:{MQTT_PORT}")
except Exception as e:
    raise ConnectionError(f"Failed to connect to AWS IoT Core: {e}")

# === Load YOLO Model ===
print("Loading YOLO model...")
model = YOLO(MODEL_PATH)
print("Model loaded successfully!")

# === Helper Functions ===

def get_single_object_key(bucket):
    """Return the first image object in the bucket, if any."""
    response = s3.list_objects_v2(Bucket=bucket, MaxKeys=10)
    if 'Contents' not in response or len(response['Contents']) == 0:
        raise ValueError("No images found in input bucket.")
    # Filter image files
    for obj in response['Contents']:
        key = obj['Key']
        if key.lower().endswith(('.jpg', '.jpeg', '.png')):
            print(f"Selected image for processing: {key}")
            return key
    raise ValueError("No valid image found in input bucket.")

def delete_previous_output(key):
    """Delete previous output image and JSON from output bucket."""
    try:
        s3.delete_object(Bucket=OUTPUT_BUCKET, Key=key)
        s3.delete_object(Bucket=OUTPUT_BUCKET, Key=os.path.splitext(key)[0] + ".json")
        print(f"Deleted previous output image and JSON: {key}")
    except Exception as e:
        print(f"No previous output to delete or error: {e}")

def process_image(key):
    print(f"\n--- Processing image: {key} ---")

    local_input = "/tmp/input.jpg"
    local_output_dir = "/tmp/output"
    json_output_path = "/tmp/result.json"

    os.makedirs("/tmp", exist_ok=True)
    os.makedirs(local_output_dir, exist_ok=True)

    # Download image
    print("Downloading image from S3...")
    s3.download_file(INPUT_BUCKET, key, local_input)

    # Run YOLO inference
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

    # Publish to MQTT
    print(f"Publishing detection event to topic: {MQTT_TOPIC}")
    try:
        result = mqtt_client.publish(MQTT_TOPIC, json.dumps(payload), qos=1)
        result.wait_for_publish()
        print("Successfully published to AWS IoT Core!")
    except Exception as e:
        print(f"MQTT publish failed: {e}")

    # Delete input image after processing
    try:
        s3.delete_object(Bucket=INPUT_BUCKET, Key=key)
        print(f"Deleted input image from S3: {key}")
    except Exception as e:
        print(f"Failed to delete input image: {e}")

# === Continuous Main Loop ===
def main_loop():
    print("=== Starting Continuous YOLO Worker ===")
    try:
        while True:
            try:
                key = get_single_object_key(INPUT_BUCKET)
                delete_previous_output(key)
                process_image(key)
            except ValueError as ve:
                print(ve)
            except Exception as e:
                print(f"Error processing image: {e}")
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping worker...")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("MQTT disconnected.")

if __name__ == "__main__":
    main_loop()
