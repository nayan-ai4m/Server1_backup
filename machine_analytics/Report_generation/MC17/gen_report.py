import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pytz
import json
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter


############################## Function to fetch data using query ###########################################

def fetch_status_groups(host, dbname, user, password, table='mc17', port=5432, timezone='Asia/Kolkata'):

    # Get current time and time 8 hours 2 minutes ago in proper timezone
    tz = pytz.timezone(timezone)
    current_time = datetime.now(tz)
    start_time = current_time - timedelta(hours=8, minutes=0)

    # Format as ISO string with timezone info
    start_ts_str = start_time.isoformat()
    end_ts_str = current_time.isoformat()

    # Create connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

    # SQL query using formatted timestamps
    query = f"""
    WITH ranked AS (
        SELECT 
            timestamp,
            status,
            status_code,
            LAG(status) OVER (ORDER BY timestamp) AS prev_status,
            LAG(status_code) OVER (ORDER BY timestamp) AS prev_status_code
        FROM {table}
        WHERE timestamp BETWEEN '{start_ts_str}' AND '{end_ts_str}'
    ),
    changes AS (
        SELECT
            timestamp,
            status,
            status_code,
            CASE 
                WHEN status != prev_status OR status_code != prev_status_code 
                     OR prev_status IS NULL OR prev_status_code IS NULL
                THEN 1 ELSE 0 
            END AS is_change
        FROM ranked
    ),
    groups AS (
        SELECT
            timestamp,
            status,
            status_code,
            SUM(is_change) OVER (ORDER BY timestamp) AS group_id
        FROM changes
    )
    SELECT
        MIN(timestamp) AS time_start,
        MAX(timestamp) AS time_end,
        status,
        status_code
    FROM groups
    GROUP BY group_id, status, status_code
    ORDER BY time_start ASC;
    """

    try:
        df = pd.read_sql_query(query, engine)

        # Shift timestamps by 5 hours 30 minutes
        time_shift = pd.Timedelta(hours=5, minutes=30)
        df['time_start'] = pd.to_datetime(df['time_start']) + time_shift
        df['time_end'] = pd.to_datetime(df['time_end']) + time_shift

        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()


############################# Function to get fault codes from json #####################################

# Load JSON data

def get_json(json_file_path):

    try:

        with open(json_file_path, 'r') as f:
            data = json.load(f)



        if isinstance(data, list):
            codes = pd.DataFrame(data)


        elif isinstance(data, dict) and 'faults' in data:
            codes = pd.DataFrame(data['faults'])

        else:
            raise ValueError("Unexpected JSON format: please provide a list of dictionaries or specify the correct key.")


        return codes
    
    except Exception as e:
        print(f"Error in get_json: {e}")
        #return pd.DataFrame()

##############################  Get plot ######################################################


def save_state_diagram(df, filename='state_diagram.png'):
    # Ensure datetime parsing

    try:
        df['time_start'] = pd.to_datetime(df['time_start'])
        df['time_end'] = pd.to_datetime(df['time_end'])

        # Add color column
        df['color'] = df.apply(lambda row: 'green' if row['status'] == 1.0 and row['status_code'] == 0.0 else 'red', axis=1)

        # Plot timeline
        fig, ax = plt.subplots(figsize=(14, 2.5))

        for _, row in df.iterrows():
            ax.barh(
                y="Machine", 
                left=row["time_start"], 
                width=(row["time_end"] - row["time_start"]), 
                height=0.4, 
                color=row["color"]
            )

        # X-axis formatting
        ax.set_title("Machine Status Timeline (UTC)")
        ax.set_yticks([])
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
        ax.set_xlim(df["time_start"].min(), df["time_end"].max())

        # Legend
        ax.legend(handles=[
            plt.Line2D([0], [0], color='green', lw=4, label='Running'),
            plt.Line2D([0], [0], color='red', lw=4, label='Stopped')
        ])

        plt.tight_layout()
        plt.savefig(filename)
        plt.close()  # Close plot to prevent re-display in interactive mode

        print('Plot saved')

        #print(df)

        return df
    
    except Exception as e:
        print(f"Error in save_state_diagram: {e}")
        return pd.DataFrame()
        
    
    


######################### Get state dataframe ######################################


def get_state_dataframe(df,codes):

    try:

    # Map status codes
        state_df = df.merge(codes[['fault_code', 'message']], left_on='status_code', right_on='fault_code', how='left')

        # Optionally drop the redundant fault_code column after merge
        state_df = state_df.drop(columns=['fault_code'])
        
        state_df.loc[(state_df['status'] == 1) & (state_df['status_code'] == 0), 'message'] = 'Running'

        # Calculate duration in HH:MM:SS (total hours even beyond 24)
        state_df['duration'] = (state_df['time_end'] - state_df['time_start']).dt.total_seconds().apply(
        lambda x: '{:02}:{:02}:{:02}'.format(int(x // 3600), int((x % 3600) // 60), int(x % 60))
        )

        state_df = state_df.drop(columns=['color'])

            # Convert to datetime
        state_df['time_start'] = pd.to_datetime(state_df['time_start'])
        state_df['time_end'] = pd.to_datetime(state_df['time_end'])

        # Remove values after seconds
        state_df['time_start'] = state_df['time_start'].dt.floor('min')
        state_df['time_end'] = state_df['time_end'].dt.floor('min')

        #state_df.head()
        return state_df
    
    except Exception as e:
        print(f"Error in get_state_dataframe: {e}")

        return pd.DataFrame()


################################ Calculate total machine running duration ###########################

def calc_run(state_df):

    try:
        state_df['duration'] = pd.to_timedelta(state_df['duration'])

        # Filter rows where machine is running
        running_durations = state_df[(state_df['status'] == 1) & (state_df['status_code'] == 0)]['duration']

        # Calculate total seconds
        total_seconds = running_durations.dt.total_seconds().sum()

        # Convert to HH:MM:SS format
        hhmmss = '{:02}:{:02}:{:02}'.format(int(total_seconds // 3600),
                                            int((total_seconds % 3600) // 60),
                                            int(total_seconds % 60))

        print(f"Machine running duration: {hhmmss}")

        return hhmmss
    
    except Exception as e:
        print(f"Error in calc_run: {e}")

        return None

######################################### Calculate total stoppage duration #########################

def calc_stop(state_df):

    try:

        state_df['duration'] = pd.to_timedelta(state_df['duration'])

        # Filter rows where machine is NOT running
        stopped_durations = state_df[~((state_df['status'] == 1) & (state_df['status_code'] == 0))]['duration']

        # Calculate total seconds and convert to HH:MM:SS format
        total_seconds = stopped_durations.dt.total_seconds().sum()
        hhmmss = '{:02}:{:02}:{:02}'.format(int(total_seconds // 3600),
                                            int((total_seconds % 3600) // 60),
                                            int(total_seconds % 60))

        print(f"Machine stopped duration: {hhmmss}")

        return hhmmss
    
    except Exception as e:
        print(f"Error in save_state_diagram: {e}")
        return None


############################### Get reasonwise stoppages ########################################

def get_reasionwise_stoppage(state_df):

    try:

        stoppage_df = state_df[~((state_df['status'] == 1) & (state_df['status_code'] == 0))]

        stoppage_df['duration'] = pd.to_timedelta(stoppage_df['duration'])


        aggregated_df = stoppage_df.groupby('message')['duration'].sum().reset_index()


        aggregated_df['total_stoppage_duration'] = aggregated_df['duration'].apply(lambda x: str(x).split()[2])

    
        aggregated_df = aggregated_df[['message', 'total_stoppage_duration']]
        
        aggregated_df['frequency'] = aggregated_df['message'].apply(lambda x: stoppage_df[stoppage_df['message'] == x].shape[0])

        aggregated_df = aggregated_df.sort_values(by='frequency', ascending=False)

        return aggregated_df
    
    except Exception as e:
        print(f"Error in get_reasonwise_stoppage: {e}")
        return None
    
    


############################## Get highest stoppage reasons ###################################

def get_top_stoppage_reasons(df):

    try:
        if df.empty or 'message' not in df.columns or 'total_stoppage_duration' not in df.columns or 'frequency' not in df.columns:
            raise ValueError("DataFrame must contain 'message', 'total_stoppage_duration', and 'frequency' columns.")

        # Row with max frequency
        freq_row = df.loc[df['frequency'].idxmax()]
        freq_message = freq_row['message']
        freq_value = freq_row['frequency']

        # Row with max duration
        duration_row = df.loc[df['total_stoppage_duration'].idxmax()]
        duration_message = duration_row['message']
        duration_value = duration_row['total_stoppage_duration']

        return freq_message, freq_value, duration_message, duration_value
    
    except Exception as e:
        print(f"Error in get_top_stoppage_reasons: {e}")
        return None, None, None, None


############################ Shift analysis ##################################################

def create_shift_analysis_df(running_sum, stoppage_sum, duration_message, freq_message):

    try:

        data = {
            "Parameter": [
                "Total running duration",
                "Total stoppage duration",
                "Highest duration stoppage reason",
                "Most frequent stoppage reason"
            ],
            "Output": [
                running_sum,
                stoppage_sum,
                duration_message,
                freq_message
            ]
        }

        shift_analysis = pd.DataFrame(data)
        return shift_analysis
    
    except Exception as e:
        print(f"Error in create_shift_analysis_df: {e}")
        
        return pd.DataFrame()


################################## Execution ###################################################




def main():
        
    try: 

        data = fetch_status_groups(
            host='localhost',
            dbname='hul',
            user='postgres',
            password='ai4m2024'
        )

        #print(data)

        codes = get_json('loop3.json')

        df = save_state_diagram(data, filename='state_diagram.png')

        #print(df)
        state_df = get_state_dataframe(df, codes)

        print(state_df)

        running_sum = calc_run(state_df)
        print(running_sum)

        stoppage_sum = calc_stop(state_df)

        print(stoppage_sum)

        reasonwise_stoppage = get_reasionwise_stoppage(state_df)

        print(reasonwise_stoppage)

        freq_message, freq_value, duration_message, duration_value = get_top_stoppage_reasons(reasonwise_stoppage)

        shift_analysis = create_shift_analysis_df(running_sum, stoppage_sum, duration_message, freq_message)

        print(shift_analysis)

        state_df.to_csv("state_df.csv", index=False)
        reasonwise_stoppage.to_csv("reasonwise_stoppage.csv", index=False)
        shift_analysis.to_csv("shift_analysis.csv", index=False)

    except Exception as e:
        print(f"Error in main(): {e}")

if __name__ == "__main__":
    main()



    











