import logging
from pycomm3 import LogixDriver
import time

# Configure logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logging.getLogger('pycomm3').setLevel(logging.WARNING)
logging.getLogger('pycomm3.cip_driver').setLevel(logging.WARNING)
logger = logging.getLogger("loop4_sku_writer")

def write_plc_tag(plc_ip, tag_name, value):
    """Write a specific value to the specified PLC tag and verify."""
    logger.info(f"Connecting to PLC at {plc_ip}")
    
    try:
        with LogixDriver(plc_ip) as plc:
            logger.info("PLC connection established")

            try:
                # Write the value to the tag
                plc.write(tag_name, value)
                logger.info(f"Successfully wrote '{value}' to {tag_name}")

                # Read back the tag to confirm
                time.sleep(0.5)  # Brief delay to ensure write is processed
                result = plc.read(tag_name)
                if result is not None and result.value is not None:
                    if result.value == value:
                        logger.info(f"Verified: {tag_name} = {result.value}")
                    else:
                        logger.warning(f"Verification failed: {tag_name} = {result.value}, expected {value}")
                else:
                    logger.warning(f"Failed to read back {tag_name} after writing")
                
                return True

            except Exception as e:
                logger.error(f"Error writing to {tag_name}: {e}")
                return False

    except Exception as e:
        logger.error(f"PLC connection error: {e}")
        return False

def main():
    plc_ip = '141.141.142.31'  # PLC IP from your setup
    tag_name = 'MX03_Batch_SKU'  # Tag to write to
    value_to_write = 'CP1RSSFG69785815'  # Specific value to write
    success = write_plc_tag(plc_ip, tag_name, value_to_write)
    
    if not success:
        logger.error("Failed to complete the write operation")
        exit(1)
    else:
        logger.info("Write operation completed successfully")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Program terminated by user")
        exit(0)
