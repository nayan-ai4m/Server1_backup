import psycopg2
import pandas as pd

# 🔹 Database connection details
DB_CONFIG = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "localhost",   # or your DB host
    "port": 5432
}

# 🔹 Template SQL query (use {mc} as placeholder for mc17, mc18, etc.)
QUERY_TEMPLATE = """
WITH filtered_data AS (
    SELECT
        timestamp,
        {mc}->>'HMI_Hor_Sealer_Strk_1' AS hor_sealer_strk_1,
        {mc}->>'HMI_Hor_Sealer_Strk_2' AS hor_sealer_strk_2,
        ({mc}->'HMI_Hor_Seal_Front_27'->>'SetValue')::float AS hor_seal_front_temp_set,
        ({mc}->'HMI_Hor_Seal_Rear_28'->>'SetValue')::float AS hor_seal_rear_temp_set
    FROM loop3_checkpoints
    WHERE
        timestamp >= NOW() - INTERVAL '40 days'
        AND {mc}->>'HMI_Hor_Sealer_Strk_1' IS NOT NULL
        AND {mc}->>'HMI_Hor_Sealer_Strk_2' IS NOT NULL
        AND {mc}->'HMI_Hor_Seal_Front_27'->>'SetValue' IS NOT NULL
        AND {mc}->'HMI_Hor_Seal_Rear_28'->>'SetValue' IS NOT NULL
),
ranked_data AS (
    SELECT
        timestamp,
        hor_sealer_strk_1,
        hor_sealer_strk_2,
        hor_seal_front_temp_set,
        hor_seal_rear_temp_set,
        LAG(hor_sealer_strk_1) OVER (ORDER BY timestamp) AS prev_hor_strk_1,
        LAG(hor_sealer_strk_2) OVER (ORDER BY timestamp) AS prev_hor_strk_2,
        LAG(hor_seal_front_temp_set) OVER (ORDER BY timestamp) AS prev_front_temp,
        LAG(hor_seal_rear_temp_set) OVER (ORDER BY timestamp) AS prev_rear_temp
    FROM filtered_data
)
SELECT
    timestamp,
    hor_sealer_strk_1,
    hor_sealer_strk_2,
    hor_seal_front_temp_set,
    hor_seal_rear_temp_set
FROM ranked_data
WHERE
    hor_sealer_strk_1 != prev_hor_strk_1
    OR hor_sealer_strk_2 != prev_hor_strk_2
    OR hor_seal_front_temp_set != prev_front_temp
    OR hor_seal_rear_temp_set != prev_rear_temp
    OR prev_hor_strk_1 IS NULL
ORDER BY timestamp DESC;
"""

def export_csv(mc):
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        # Format query with mc17, mc18, ...
        query = QUERY_TEMPLATE.format(mc=mc)

        df = pd.read_sql_query(query, conn)

        # Save unique CSV per machine
        output_file = f"/home/ai4m/develop/{mc}_output.csv"
        df.to_csv(output_file, index=False)

        print(f"✅ Data for {mc} exported successfully to {output_file}")

    except Exception as e:
        print(f"❌ Error exporting {mc}:", e)

    finally:
        if 'conn' in locals():
            conn.close()

# 🔹 Run export for mc17 → mc22
for mc in ["mc17", "mc18", "mc19", "mc20", "mc21", "mc22"]:
    export_csv(mc)

