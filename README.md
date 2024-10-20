import time
import sqlite3
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
import RPi.GPIO as GPIO
import requests  # If you need to make HTTP requests to download data from the cloud

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
modbus_client = ModbusClient('192.168.1.100', port=502)  # Replace with your meter's IP and port

# Connect to the meter
modbus_client.connect()

# GPIO Setup for Power Sensor
POWER_SENSOR_PIN = 18  # GPIO pin for digital power sensor
GPIO.setmode(GPIO.BCM)
GPIO.setup(POWER_SENSOR_PIN, GPIO.IN)  # Set up the GPIO pin

# Function to read total active power from the smart meter
def read_power_data():
    try:
        # Example: Reading total active power (Modbus register 3053)
        result = modbus_client.read_input_registers(3053, count=1, unit=1)  # unit=1 is the Modbus slave ID
        if not result.isError():
            power_value = result.registers[0] / 10.0  # Convert to kW
            return power_value
        else:
            print("Error reading power data from the smart meter.")
            return None
    except Exception as e:
        print(f"Error reading from Modbus: {e}")
        return None

# Function to read from the power sensor
def read_power_sensor():
    # Here we assume a digital sensor that returns HIGH (1) or LOW (0)
    return GPIO.input(POWER_SENSOR_PIN)

# Function to save locally obtained data to SQLite
def save_to_local_db(timestamp, power_value):
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO power_data (timestamp, power_value)
            VALUES (?, ?)
        ''', (timestamp, power_value))
        conn.commit()
        print(f"Saved to local DB: {timestamp}, {power_value} kW")
    except Exception as e:
        print(f"Error saving to local DB: {e}")


# Set up GPIO
OUTPUT_PIN = 23  # Choose an appropriate GPIO pin number
GPIO.setmode(GPIO.BCM)  # Use Broadcom pin numbering
GPIO.setup(OUTPUT_PIN, GPIO.OUT)  # Set the pin as an output

def logic_gate(state):
    if state == 1:
        print("Logic Gate Activated: Proceeding with operations.")
        GPIO.output(OUTPUT_PIN, GPIO.HIGH)  # Set GPIO pin high
    elif state == 0:
        print("Logic Gate Deactivated: Initiating error handling procedures.")
        GPIO.output(OUTPUT_PIN, GPIO.LOW)   # Set GPIO pin low


power_within_threshold = False  # Indicates whether the power value is within the threshold
power_sensor_match = False       # Indicates whether the power sensor matches the smart meter value
power_exceeds_threshold = False   # Indicates whether the power value exceeds the threshold

# Define your threshold value
threshold_value = 5  # Replace with your desired threshold value

# Main loop to collect data from the smart meter and power sensor
try:
    while True:
        current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        p_1 = read_power_data()  # Read from smart meter
        p_2 = read_power_sensor()  # Read from power sensor (HIGH/LOW)

        if p_1 is not None:
            # Save locally obtained data to SQLite
            save_to_local_db(current_timestamp, p_1)

            # Check the condition for power sensor logic
            if p_1 <= threshold_value:
                power_within_threshold = True
                if p_1 == p_2:
                    power_sensor_match = True  # Power sensor matches smart meter value
                else:
                    power_sensor_match = False  # Mismatch between power sensor and smart meter value
            else:
                power_exceeds_threshold = True  # Power value exceeds threshold; set flag

            # Use these flags to control the logic gate
            if power_exceeds_threshold:
                logic_gate(0)  # Trigger logic gate deactivation (0)
            elif power_within_threshold and power_sensor_match:
                logic_gate(1)  # Trigger logic gate activation (1)
            else:
                logic_gate(0)  # Trigger logic gate deactivation due to mismatch (0)

        # Perform verification and get boolean result
        verification_result = verify_data()

        # Call the logic gate function with the verification result
        logic_gate(int(verification_result))  # Convert verification result to int (1 or 0)

        # Wait for a specified interval before the next reading
        time.sleep(60)

except KeyboardInterrupt:
    print("Data collection stopped by user")

finally:
    # Close Modbus and SQLite connections
    modbus_client.close()
    conn.close()
    GPIO.cleanup()  # Clean up GPIO settings

