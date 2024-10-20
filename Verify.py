import time
import json
import boto3
import sqlite3
from pymodbus.client import ModbusTcpClient
import RPi.GPIO as GPIO
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
import os
import base64



# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = 'your-s3-bucket-name'
folder_name = 'smart-meter-data'  # Folder in S3

# SQLite Database setup
sqlite_db = 'local_smart_meter_data.db'

# Create a SQLite connection
conn = sqlite3.connect(sqlite_db)
cursor = conn.cursor()

# Create a table to store power data if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS power_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT UNIQUE,
    power_value REAL
)
''')
conn.commit()

# Set up Modbus TCP client
modbus_client = ModbusTcpClient('192.168.1.100', port=502)  # Replace with your meter's IP and port

# Connect to the meter
modbus_client.connect()


# Function to read total active power from the smart meter

# Function to decrypt data


# AES key should be securely stored and retrieved

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString']

        # convert the secret from json

        secret_dict = json.loads(secret)
        kms_key_id = secret_dict.get('KMSKeyId')

        return kms_key_id
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

# Get the AES key securely from AWS Secrets Manager


aes_key = get_secret('my_aes_key')

if aes_key is None:
    raise Exception("Unable to retrieve AES key from Secrets Manager")
iv = os.urandom(16)  # Generate IV for each encryption, but keep it stored as well


def read_power_data():
    try:
        # Ensure you check connection status
        if not modbus_client.connect():
            print("Failed to connect to the Modbus client.")
            return None
        result = modbus_client.read_input_registers(3053, count=2, unit=1)
        if not result.isError():
            power_value = result.registers[0] / 10.0
            return power_value
        else:
            print("Error reading power data from the smart meter.")
            return None
    except Exception as e:
        print(f"Error reading from Modbus: {e}")
        return None

# Update SQL statement to use INSERT OR REPLACE
def save_to_local_db(timestamp, power_value):
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO power_data (timestamp, power_value)
            VALUES (?, ?)
        ''', (timestamp, power_value))
        conn.commit()
        print(f"Saved to local DB: {timestamp}, {power_value} kW")
    except Exception as e:
        print(f"Error saving to local DB: {e}")

# Function to decrypt data using AWS KMS


def decrypt_data_kms(encrypted_data, kms_encrypted_iv):
    try:
        # Decrypt the IV using KMS
        kms_client = boto3.client('kms')
        decrypted_iv_response = kms_client.decrypt(
            CiphertextBlob=base64.b64decode(kms_encrypted_iv)
        )
        decrypted_iv = decrypted_iv_response['Plaintext']

        # Create AES cipher with the fixed AES key and decrypted IV
        cipher = Cipher(algorithms.AES(aes_key), modes.CBC(decrypted_iv))
        decryptor = cipher.decryptor()

        # Decrypt the data
        padded_data = decryptor.update(base64.b64decode(encrypted_data)) + decryptor.finalize()

        # Unpad the data
        unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
        data = unpadder.update(padded_data) + unpadder.finalize()

        # Convert bytes back to string
        return data.decode()

    except Exception as e:
        print(f"Error during decryption: {e}")
        return None


def download_data_from_s3():
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if 'Contents' not in response:
            print("No data found in cloud storage.")
            return []

        downloaded_data = []

        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key.endswith('.json'):
                file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                file_content = file_obj['Body'].read().decode('utf-8')
                data = json.loads(file_content)

                # Ensure the necessary keys exist in the JSON
                if 'encrypted_data' not in data or 'iv' not in data:
                    print(f"Error: Missing keys in data from {file_key}")
                    continue

                encrypted_data = data['encrypted_data']
                iv = data['iv']  # Keep iv as a string to pass to KMS decryption
                encrypted_data_bytes = base64.b64decode(encrypted_data)

                decrypted_data = decrypt_data_kms(encrypted_data_bytes, iv)
                if decrypted_data:
                    downloaded_data.append((decrypted_data.split(",")[0], decrypted_data.split(",")[1]))
        return downloaded_data

    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return []



def verify_data():
    try:
        # Fetch local data from SQLite
        cursor.execute('SELECT timestamp, power_value FROM power_data')
        local_data = cursor.fetchall()

        # Download data from cloud for verification
        cloud_data = download_data_from_s3()

        # Convert both to dictionaries for easier comparison by timestamp
        local_dict = {entry[0]: entry[1] for entry in local_data}
        cloud_dict = {entry[0]: entry[1] for entry in cloud_data}

        error_flag = False
        for timestamp, local_value in local_dict.items():
            if timestamp not in cloud_dict:
                print(f"Error: Timestamp {timestamp} not found in cloud data.")
                error_flag = True
            elif local_value != cloud_dict[timestamp]:
                print(
                    f"Error: Data mismatch at {timestamp}. Local value: {local_value}, Cloud value: {cloud_dict[timestamp]}")
                error_flag = True

        # Check for timestamps in cloud that are not in local data
        for timestamp in cloud_dict:
            if timestamp not in local_dict:
                print(f"Error: Timestamp {timestamp} in cloud data not found in local storage.")
                error_flag = True

        if not error_flag:
            print("Data verification successful. No discrepancies found.")
        else:
            print("Data verification failed. Discrepancies found.")

    except Exception as e:
        print(f"Error verifying data: {e}")


# Simulated loop to collect data from the smart meter
try:
    while True:
        # Read the power data from the smart meter
        current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        power_value = read_power_data()

        if power_value is not None:
            # Save locally obtained data to SQLite
            save_to_local_db(current_timestamp, power_value)

        # Wait for 60 seconds before the next reading
        time.sleep(1)

except KeyboardInterrupt:
    print("Data collection stopped by user")

finally:
    # Run verification process before exiting
    verify_data()

    # Close Modbus and SQLite connections
    modbus_client.close()
    conn.close()