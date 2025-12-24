import sys
import logging
from pycomm3 import LogixDriver

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PLCDatabaseHandler:
    def __init__(self, plc_ip):
        self.plc_ip = plc_ip
        self.required_tags = ['MC28_STATE', 'MC29_STATE']
        self.verify_plc_tags()

    def verify_plc_tags(self):
        try:
            with LogixDriver(self.plc_ip) as plc:
                available_tags = plc.get_tag_list()
                missing_tags = [tag for tag in self.required_tags if tag not in available_tags]
                if missing_tags:
                    logger.error(f"Missing tags in PLC: {missing_tags}")
                    sys.exit(1)
                logger.info("All required tags are present in the PLC")
        except Exception as e:
            logger.error(f"Failed to connect to PLC or retrieve tags: {e}")
            sys.exit(1)

# Start the script
handler = PLCDatabaseHandler('141.141.143.20')
