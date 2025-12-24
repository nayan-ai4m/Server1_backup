import psycopg2
import pandas as pd
import time


def check_status():
    config = {
        "dbname": "hul",
        "user": "postgres",
        "password": "ai4m2024",
        "host": "100.96.244.68",
        "port": "5432"
    }

    try:
        # Connect to the database
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Query with table name mc17 directly
        query = """
            SELECT status, status_code
            FROM mc17
            ORDER BY timestamp DESC
            LIMIT 3000
        """
        cur.execute(query)

        # Put results in DataFrame
        df = pd.DataFrame(cur.fetchall(), columns=['status', 'status_code'])

        # Check if all rows satisfy condition
        if ((df['status'] == 1) & (df['status_code'] == 0)).all():
            print("Machine is running")
            return True
        else:
            print("Machine is not running")
            return False

    except Exception as e:
        print("Error:", e)
        return False

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


def get_latest_amb_temp():
    config = {
        "dbname": "hul",
        "user": "postgres",
        "password": "ai4m2024",
        "host": "100.96.244.68",
        "port": "5432"
    }

    query = """
        SELECT plant_params->>'PLANT_TEMPERATURE' AS plant_temperature
        FROM dark_cascade_overview
        ORDER BY timestamp DESC
        LIMIT 1;
    """
    try:
        conn = psycopg2.connect(**config)
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            if result and result[0] is not None:
                try:
                    return float(result[0])
                except ValueError:
                    return result[0]
            else:
                return None
    except Exception as e:
        print("Error fetching plant temperature:", e)
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_avg_sealer_temps():
    config = {
        "dbname": "hul",
        "user": "postgres",
        "password": "ai4m2024",
        "host": "100.96.244.68",
        "port": "5432"
    }

    query = """
        SELECT
            AVG(hor_sealer_front_1_temp) AS avg_front_temp,
            AVG(hor_sealer_rear_1_temp) AS avg_rear_temp
        FROM (
            SELECT hor_sealer_front_1_temp, hor_sealer_rear_1_temp
            FROM mc17
            ORDER BY timestamp DESC
            LIMIT 100
        ) sub;
    """
    try:
        conn = psycopg2.connect(**config)
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            if result:
                avg_front = float(result[0]) if result[0] is not None else None
                avg_rear = float(result[1]) if result[1] is not None else None
                return avg_front, avg_rear
            else:
                return None, None
    except Exception as e:
        print("Error fetching sealer temperature averages:", e)
        return None, None
    finally:
        if 'conn' in locals():
            conn.close()


def get_avg_hor_pressure():
    config = {
        "dbname": "hul",
        "user": "postgres",
        "password": "ai4m2024",
        "host": "100.96.244.68",
        "port": "5432"
    }

    query = """
        SELECT
            AVG(hor_pressure) AS avg_hor_pressure
        FROM (
            SELECT hor_pressure
            FROM mc17
            WHERE cam_position BETWEEN 150 AND 210
            ORDER BY timestamp DESC
            LIMIT 30
        ) sub;
    """
    try:
        conn = psycopg2.connect(**config)
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            if result and result[0] is not None:
                return float(result[0])
            else:
                return None
    except Exception as e:
        print("Error fetching average hor_pressure:", e)
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def calc_jaw_temp(avg_front, avg_rear, T_eff_front, T_eff_rear):
    Tjaw_front = avg_front * T_eff_front
    Tjaw_rear = avg_rear * T_eff_rear
    return Tjaw_front, Tjaw_rear


def get_init_heat_energy(init_Tjaw_front, init_Tjaw_rear, init_T_lam):
    init_front_energy = init_Tjaw_front - init_T_lam
    init_rear_energy = init_Tjaw_rear - init_T_lam
    return init_front_energy, init_rear_energy


def get_curr_heat_energy(Tjaw_front, Tjaw_rear, latest_lam_temp):
    curr_front_energy = Tjaw_front - latest_lam_temp
    curr_rear_energy = Tjaw_rear - latest_lam_temp
    return curr_front_energy, curr_rear_energy


def check_heat_energy_diff(init_front_energy, init_rear_energy, curr_front_energy, curr_rear_energy):
    try:
        front_heat_energy_diff = curr_front_energy - init_front_energy
        rear_heat_energy_diff = curr_rear_energy - init_rear_energy

        if front_heat_energy_diff < 5:
            print("Low front heat energy")  # Activate temp TP
        elif rear_heat_energy_diff < 5:
            print("Low rear heat energy")  # Activate temp TP
        else:
            print("Sufficient heat energy is transferred")

        return front_heat_energy_diff, rear_heat_energy_diff
    except Exception as e:
        print("Error fetching heat energy difference:", e)
        return None, None


def check_press_energy_diff(avg_pressure, sugg_Pressure, pressure_thresh):
    try:
        press_energy_diff = sugg_Pressure - avg_pressure

        if abs(press_energy_diff) < pressure_thresh:
            print("Sealing pressure is within range...")
        elif avg_pressure > (sugg_Pressure + pressure_thresh):
            print("Sealing pressure is increased.")  # Activate pressure TP
        else:
            print("Sealing pressure is decreased.")  # Activate pressure TP

        return press_energy_diff
    except Exception as e:
        print("Error fetching press_energy_diff:", e)
        return None


def main():
    sugg_T_front = 150
    sugg_T_rear = 149
    sugg_Pressure = 2.1
    init_T_lam = 63.0
    T_eff_front = 0.87
    T_eff_rear = 0.88
    pressure_thresh = 0.3

    # Initial jaw temps
    init_Tjaw_front = sugg_T_front * T_eff_front
    init_Tjaw_rear = sugg_T_rear * T_eff_rear

    while True:
        try:
            running = check_status()

            if running:
                latest_lam_temp = get_latest_amb_temp()
                if latest_lam_temp is not None:
                    latest_lam_temp += 30

                avg_front, avg_rear = get_avg_sealer_temps()
                avg_pressure = get_avg_hor_pressure()

                Tjaw_front, Tjaw_rear = calc_jaw_temp(avg_front, avg_rear, T_eff_front, T_eff_rear)

                init_front_energy, init_rear_energy = get_init_heat_energy(init_Tjaw_front, init_Tjaw_rear, init_T_lam)
                curr_front_energy, curr_rear_energy = get_curr_heat_energy(Tjaw_front, Tjaw_rear, latest_lam_temp)

                front_heat_energy_diff, rear_heat_energy_diff = check_heat_energy_diff(
                    init_front_energy, init_rear_energy, curr_front_energy, curr_rear_energy
                )

                press_energy_diff = check_press_energy_diff(avg_pressure, sugg_Pressure, pressure_thresh)

                print(front_heat_energy_diff, rear_heat_energy_diff, press_energy_diff)
            else:
                print("Waiting for machine to start.")

            time.sleep(5)
        except Exception as e:
            print("Error in main function:", e)
            time.sleep(5)


if __name__ == "__main__":
    main()

