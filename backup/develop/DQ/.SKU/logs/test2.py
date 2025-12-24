import logging
from pycomm3 import LogixDriver
import time
import datetime

# Configure logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logging.getLogger('pycomm3').setLevel(logging.WARNING)
logging.getLogger('pycomm3.cip_driver').setLevel(logging.WARNING)
logger = logging.getLogger("loop4_sku_reader")

def read_and_print_plc_tags(plc_ip):
    """Read PLC tags and print their values."""
    # Define tag names to match your PLC
    tag_names = [
        "MX03_HT_Transfer_TS",
        "HTloop4_Level",
        "HTloop4_Primary_Tank",
        "HTloop4_Secondary_Tank",
        "MX03_Batch_No_D",
        "MX03_Batch_Mfg_date",
        "MX03_Batch_SKU",
        "MX03_Shift_D"
    ]

    logger.info(f"Connecting to PLC at {plc_ip}")
    
    while True:
        try:
            with LogixDriver(plc_ip) as plc:
                logger.info("PLC connection established")

                while True:
                    try:
                        # Read all tags at once
                        tag_values = plc.read(*tag_names)
                        tags = {"timestamp": datetime.datetime.now()}

                        # Process tag values
                        for i, tag in enumerate(tag_values):
                            if tag.error:
                                logger.warning(f"Error reading tag {tag.tag}: {tag.error}")
                                tags[tag_names[i]] = None
                            else:
                                tags[tag_names[i]] = tag.value

                        # Print the collected data
                        print("\n=== PLC Tag Values ===")
                        for key, value in tags.items():
                            print(f"{key}: {value}")

                        # Wait before the next read (e.g., every 5 seconds)
                        time.sleep(5)

                    except Exception as e:
                        logger.error(f"Error during tag reading: {e}")
                        time.sleep(5)  # Wait before retrying

        except Exception as e:
            logger.error(f"PLC connection error: {e}")
            time.sleep(10)  # Wait before attempting to reconnect

def main():
    plc_ip = '141.141.142.31'  # PLC IP from your code
    read_and_print_plc_tags(plc_ip)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Program terminated by user")
