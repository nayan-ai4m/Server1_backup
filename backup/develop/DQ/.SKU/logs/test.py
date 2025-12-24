import logging
from pycomm3 import LogixDriver
import time
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('pycomm3').setLevel(logging.CRITICAL)

def read_and_print_plc_tags(plc_ip):
    # Define tag names to match your PLC
    tag_names = [
        "MX02_HT_Transfer_TS",
        "HTloop3_Level",
        "HTloop3_Primary_Tank",
        "HTloop3_Secondary_Tank",
        "MX02_Batch_No_D",
        "MX02_Batch_Mfg_Date",
        "MX02_Batch_SKU",
        "MX02_Shift_D",
        "HT_CM5801.RunStopStatus"
    ]

    # Attempt to connect to the PLC
    try:
        logging.info(f"Connecting to PLC at {plc_ip}")
        with LogixDriver(plc_ip) as plc:
            logging.info("PLC connection established")

            # Read tags in a loop
            while True:
                try:
                    # Dictionary to store tag values
                    tags = {"timestamp": datetime.datetime.now()}

                    # Read each tag
                    for tag in tag_names:
                        try:
                            result = plc.read(tag)
                            if result is not None and result.value is not None:
                                tags[tag] = result.value
                            else:
                                tags[tag] = None
                                logging.warning(f"Failed to read tag: {tag}")
                        except Exception as e:
                            tags[tag] = None
                            logging.error(f"Error reading tag {tag}: {e}")

                    # Print the collected data
                    print("\n=== PLC Tag Values ===")
                    for key, value in tags.items():
                        print(f"{key}: {value}")

                    # Wait before the next read (e.g., every 5 seconds)
                    time.sleep(5)

                except Exception as e:
                    logging.error(f"Error during tag reading: {e}")
                    time.sleep(5)  # Wait before retrying

    except Exception as e:
        logging.error(f"PLC connection failed: {e}")
        time.sleep(5)  # Wait before attempting to reconnect

def main():
    plc_ip = '141.141.142.11'  # Replace with your PLC IP address
    read_and_print_plc_tags(plc_ip)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Program terminated by user")
