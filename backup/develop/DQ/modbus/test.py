from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
from datetime import datetime

class ModbusTagPrinter:
    def __init__(self):
        self.modbus_config = {"ip": "141.141.143.9", "port": 502}
        self.holding_registers = {'values': [
            {"name": "horizontal_sealing_time", "register_address": 170, "data_type": 16},
            {"name": "horizontal_sealer_stroke_1", "register_address": 207, "data_type": 16},
            {"name": "horizontal_sealer_stroke_2", "register_address": 208, "data_type": 16},
            {"name": "vertical_sealer_stroke_1", "register_address": 209, "data_type": 16},
            {"name": "vertical_sealer_stroke_2", "register_address": 210, "data_type": 16},
            {"name": "batch_cut_on_degree", "register_address": 130, "data_type": 16},
            {"name": "batch_cut_off_degree", "register_address": 132, "data_type": 16},
            {"name": "vertical_sealer_front_1_setpoint", "register_address": 1, "data_type": 32},
            {"name": "horizontal_sealer_front_1_setpoint", "register_address": 55, "data_type": 32},
            {"name": "speed", "register_address": 100, "data_type": 16}
        ]}
        self.input_registers = {'values': [
            {"name": "cld_a", "register_address": 138, "data_type": 16},
            {"name": "cld_b", "register_address": 139, "data_type": 16},
            {"name": "cld_c", "register_address": 140, "data_type": 16},
            {"name": "vertical_sealer_front_1_temp", "register_address": 1, "data_type": 16},
            {"name": "vertical_sealer_front_2_temp", "register_address": 2, "data_type": 16},
            {"name": "vertical_sealer_front_3_temp", "register_address": 3, "data_type": 16},
            {"name": "vertical_sealer_front_4_temp", "register_address": 4, "data_type": 16},
            {"name": "vertical_sealer_front_5_temp", "register_address": 5, "data_type": 16},
            {"name": "vertical_sealer_front_6_temp", "register_address": 6, "data_type": 16},
            {"name": "vertical_sealer_front_7_temp", "register_address": 7, "data_type": 16},
            {"name": "vertical_sealer_front_8_temp", "register_address": 8, "data_type": 16},
            {"name": "vertical_sealer_front_9_temp", "register_address": 9, "data_type": 16},
            {"name": "vertical_sealer_front_10_temp", "register_address": 10, "data_type": 16},
            {"name": "vertical_sealer_front_11_temp", "register_address": 11, "data_type": 16},
            {"name": "vertical_sealer_front_12_temp", "register_address": 12, "data_type": 16},
            {"name": "vertical_sealer_front_13_temp", "register_address": 13, "data_type": 16},
            {"name": "vertical_sealer_rear_1_temp", "register_address": 14, "data_type": 16},
            {"name": "vertical_sealer_rear_2_temp", "register_address": 15, "data_type": 16},
            {"name": "vertical_sealer_rear_3_temp", "register_address": 16, "data_type": 16},
            {"name": "vertical_sealer_rear_4_temp", "register_address": 17, "data_type": 16},
            {"name": "vertical_sealer_rear_5_temp", "register_address": 18, "data_type": 16},
            {"name": "vertical_sealer_rear_6_temp", "register_address": 19, "data_type": 16},
            {"name": "vertical_sealer_rear_7_temp", "register_address": 20, "data_type": 16},
            {"name": "vertical_sealer_rear_8_temp", "register_address": 21, "data_type": 16},
            {"name": "vertical_sealer_rear_9_temp", "register_address": 22, "data_type": 16},
            {"name": "vertical_sealer_rear_10_temp", "register_address": 23, "data_type": 16},
            {"name": "vertical_sealer_rear_11_temp", "register_address": 24, "data_type": 16},
            {"name": "vertical_sealer_rear_12_temp", "register_address": 25, "data_type": 16},
            {"name": "vertical_sealer_rear_13_temp", "register_address": 26, "data_type": 16},
            {"name": "horizontal_servo_position", "register_address": 81, "data_type": 32},
            {"name": "vertical_servo_position", "register_address": 79, "data_type": 32},
            {"name": "rotational_valve_position", "register_address": 77, "data_type": 32},
            {"name": "fill_piston_position", "register_address": 75, "data_type": 32},
            {"name": "web_puller_position", "register_address": 83, "data_type": 32},
            {"name": "cam_position", "register_address": 85, "data_type": 32},
            {"name": "horizontal_sealer_front_1_temp", "register_address": 27, "data_type": 16},
            {"name": "horizontal_sealer_rear_1_temp", "register_address": 28, "data_type": 16},
            {"name": "state", "register_address": 97, "data_type": 16},
            {"name": "position", "register_address": 85, "data_type": 16}
        ]}
        self.modbus_client = None

    def connect_to_modbus(self):
        try:
            self.modbus_client = ModbusTcpClient(self.modbus_config["ip"], port=self.modbus_config["port"])
            self.modbus_client.connect()
            if not self.modbus_client.is_socket_open():
                print("Failed to connect to Modbus server.")
                return False
            return True
        except Exception as e:
            print(f"Modbus connection error: {e}")
            return False

    def read_modbus_register(self, register_address, data_type, is_holding=True):
        try:
            response = self.modbus_client.read_holding_registers(register_address, 1 if data_type == 16 else 2) if is_holding else self.modbus_client.read_input_registers(register_address, 1 if data_type == 16 else 2)
            if response.isError():
                print(f"Error reading register {register_address}: Response error")
                return None
            return response.registers[0] if data_type == 16 else BinaryPayloadDecoder.fromRegisters(response.registers, byteorder=Endian.BIG, wordorder=Endian.LITTLE).decode_32bit_float()
        except Exception as e:
            print(f"Error reading Modbus register {register_address}: {e}")
            return None

    def run(self):
        if not self.connect_to_modbus():
            print("Exiting due to Modbus connection failure.")
            return

        while True:
            try:
                data_values = {
                    item['name']: self.read_modbus_register(item['register_address'], item['data_type'], is_holding=reg_type == 'holding')
                    for reg_type in ['holding', 'input']
                    for item in getattr(self, f"{reg_type}_registers")['values']
                }
                data_values['timestamp'] = str(datetime.now())

                # Print tag values in a formatted way
                print("\n=== Modbus Tag Values ===")
                print(f"Timestamp: {data_values['timestamp']}")
                print("\nHolding Registers:")
                for item in self.holding_registers['values']:
                    tag_name = item['name']
                    value = data_values.get(tag_name, None)
                    print(f"{tag_name}: {value if value is not None else 'None'}")
                print("\nInput Registers:")
                for item in self.input_registers['values']:
                    tag_name = item['name']
                    value = data_values.get(tag_name, None)
                    print(f"{tag_name}: {value if value is not None else 'None'}")
                print("=======================\n")

            except Exception as e:
                print(f"Error in main loop: {e}")
                self.modbus_client.close()
                if not self.connect_to_modbus():
                    print("Reconnection failed. Exiting.")
                    break

    def close_connections(self):
        if self.modbus_client:
            self.modbus_client.close()
        print("Modbus connection closed.")

if __name__ == "__main__":
    modbus_printer = ModbusTagPrinter()
    try:
        modbus_printer.run()
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        modbus_printer.close_connections()
