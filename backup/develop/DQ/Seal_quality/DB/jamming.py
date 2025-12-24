import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from scipy.signal import savgol_filter, find_peaks
from datetime import datetime, timedelta
import csv
import os
import psycopg2
import time

# Updated database connection parameters
conn_params = {
    'dbname': 'short_data_hul',  # Updated database name
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

def fetch_latest_data(limit=300):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    query = f"""
    SELECT pulling_servo_current,
    cam_position,
    spare1,
    timestamp,
    status
    FROM mc19_short_data
    ORDER BY timestamp DESC LIMIT {limit};
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Updated column names
    columns = ['pulling_servo_current', 'cam_position', 'spare1', 'timestamp', 'status']
    df = pd.DataFrame(rows, columns=columns)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df.sort_values('timestamp')

def log_alert_to_database(timestamp, avg_current, baseline_avg, rate_of_change, reason):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO jamming_data 
    (timestamp, average_current_value, baseline_average_value, rate_of_change, reason)
    VALUES (%s, %s, %s, %s, %s);
    """
    
    try:
        cursor.execute(insert_query, (
            timestamp,
            float(avg_current),
            float(baseline_avg),
            float(rate_of_change),
            reason
        ))
        conn.commit()
    except Exception as e:
        print(f"Database error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def calculate_average_and_roc(df):
    mask = (df['cam_position'] >= 50) & (df['cam_position'] <= 250)
    range_data = df[mask]
    current_avg = range_data['pulling_servo_current'].mean()  # Updated column name
    one_sec_ago = df['timestamp'].max() - timedelta(seconds=1)  # Updated column name
    past_data = range_data[range_data['timestamp'] < one_sec_ago]  # Updated column name
    past_avg = past_data['pulling_servo_current'].mean()  # Updated column name
    rate_of_change = (current_avg - past_avg) / 3  # per second
    return current_avg, rate_of_change

def calculate_baseline(df):
    mask = (df['cam_position'] >= 50) & (df['cam_position'] <= 250)
    range_data = df[mask]
    baseline = range_data.groupby('cam_position')['pulling_servo_current'].mean()  # Updated column name
    return baseline, range_data

def adaptive_savgol_filter(data, window_length, polyorder):
    n_points = len(data)
    if n_points < window_length:
        window_length = n_points if n_points % 2 != 0 else n_points - 1
    if window_length < polyorder + 1:
        polyorder = window_length - 1 if window_length > 1 else 0
    
    if window_length > 2 and polyorder >= 0:
        return savgol_filter(data, window_length, polyorder)
    else:
        return data

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the layout
app.layout = html.Div([
    html.H1("Jamming Analysis Dashboard", style={'textAlign': 'center'}),
    
    # Status indicators
    html.Div([
        html.Div(id='machine-status', style={'fontSize': '20px', 'margin': '10px'}),
        html.Div(id='latest-timestamp', style={'fontSize': '16px', 'margin': '10px'}),
        html.Div(id='current-metrics', style={'fontSize': '16px', 'margin': '10px'})
    ]),
    
    # Main graph
    dcc.Graph(id='live-graph'),
    
    # Interval component for updates
    dcc.Interval(
        id='interval-component',
        interval=300,  # in milliseconds (300ms = 0.3s)
        n_intervals=0
    ),
    
    # Store components for state management
    dcc.Store(id='previous-machine-status'),
    dcc.Store(id='status-logged'),
    dcc.Store(id='waiting-for-restart')
])

@app.callback(
    [Output('live-graph', 'figure'),
     Output('machine-status', 'children'),
     Output('latest-timestamp', 'children'),
     Output('current-metrics', 'children'),
     Output('previous-machine-status', 'data'),
     Output('status-logged', 'data'),
     Output('waiting-for-restart', 'data')],
    [Input('interval-component', 'n_intervals')],
    [dash.dependencies.State('previous-machine-status', 'data'),
     dash.dependencies.State('status-logged', 'data'),
     dash.dependencies.State('waiting-for-restart', 'data')]
)
def update_graph(n_intervals, prev_status, status_logged, waiting_for_restart):
    # Fetch latest data
    display_data = fetch_latest_data(limit=1000)
    latest_timestamp = display_data['timestamp'].iloc[-1]  # Updated column name
    current_machine_status = display_data['status'].iloc[-1]  # Updated column name
    
    # Initialize states if None
    if prev_status is None:
        prev_status = current_machine_status
    if status_logged is None:
        status_logged = False
    if waiting_for_restart is None:
        waiting_for_restart = False
    
    # Handle machine status changes
    if current_machine_status != prev_status:
        if current_machine_status == 0 and not status_logged:
            log_alert(latest_timestamp, 0, 0, 0, "Machine Stopped")
            status_logged = True
            waiting_for_restart = False
        elif current_machine_status == 1 and not waiting_for_restart:
            waiting_for_restart = True
            time.sleep(2)  # Reduced for demonstration, adjust as needed
            if not status_logged:
                log_alert(latest_timestamp, 0, 0, 0, "Machine Started")
                status_logged = True
            waiting_for_restart = False
    
    # Create the figure
    fig = go.Figure()
    
    if current_machine_status == 0 or waiting_for_restart:
        # Machine stopped view
        fig.add_annotation(
            text="Machine Stopped<br>Analysis Paused",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=20, color="white"),
        )
        fig.update_layout(
            plot_bgcolor='gray',
            paper_bgcolor='gray'
        )
    else:
        # Normal operation view
        baseline_data = fetch_latest_data(limit=10000)
        last_600_cycles = baseline_data['spare1'].unique()[-600:]  # Updated column name
        baseline_df = baseline_data[baseline_data['spare1'].isin(last_600_cycles)]  # Updated column name
        baseline, baseline_points = calculate_baseline(baseline_df)
        
        # Add traces
        fig.add_trace(go.Scatter(
            x=baseline_points['cam_position'],
            y=baseline_points['pulling_servo_current'],  # Updated column name
            mode='markers',
            name='Baseline Data',
            marker=dict(color='lightblue', size=5, opacity=0.3)
        ))
        
        last_5_cycles = display_data['spare1'].unique()[-5:]  # Updated column name
        filtered_df = display_data[display_data['spare1'].isin(last_5_cycles)]  # Updated column name
        
        fig.add_trace(go.Scatter(
            x=filtered_df['cam_position'],
            y=filtered_df['pulling_servo_current'],  # Updated column name
            mode='markers',
            name='Current Data',
            marker=dict(color='blue', size=5)
        ))
        
        # Calculate metrics for annotations
        current_avg, rate_of_change = calculate_average_and_roc(filtered_df)
        
        fig.update_layout(
            title=f'Cycle ID: {display_data["spare1"].max()}',  # Updated column name
            xaxis_title='Cam Position',
            yaxis_title='Servo Current',
            showlegend=True
        )
    
    # Update status messages
    machine_status_text = f"Machine Status: {'Running' if current_machine_status == 1 else 'Stopped'}"
    timestamp_text = f"Latest Timestamp: {latest_timestamp}"
    
    # Calculate and display metrics
    if current_machine_status == 1 and not waiting_for_restart:
        current_avg, rate_of_change = calculate_average_and_roc(display_data)
        metrics_text = f"Current Average: {current_avg:.2f}A | Rate of Change: {rate_of_change:.2f}A/s"
    else:
        metrics_text = "Metrics paused"
    
    return fig, machine_status_text, timestamp_text, metrics_text, current_machine_status, status_logged, waiting_for_restart

if __name__ == '__main__':
    app.run_server(debug=True)
