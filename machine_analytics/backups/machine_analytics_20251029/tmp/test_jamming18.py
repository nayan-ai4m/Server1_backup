import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from scipy.signal import savgol_filter, find_peaks
from datetime import datetime, timedelta
import csv
import os
import psycopg2
import time


machine = 18

# Database connection parameters
conn_params = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': '192.168.4.11',
    'port': '5432'
}

def fetch_latest_data(limit=300):                                                                     
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    query = f"""
    SELECT web_puller_current/10 as web_puller_current,
    cam_position,
    spare1,
    timestamp,
    status
    FROM mc17_short_data -- Edit machine name
    ORDER BY timestamp DESC LIMIT {limit};
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    columns = ['web_puller_current', 'cam_position', 'spare1', 'timestamp', 'status']
    df = pd.DataFrame(rows, columns=columns)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df.sort_values('timestamp')

def log_alert(timestamp, avg_current, baseline_avg, rate_of_change, reason, machine):
    # Log to CSV
    csv_file = 'alert_log.csv'
    file_exists = os.path.isfile(csv_file)
    
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['timestamp', 'average_current_value', 'baseline_average_value', 'rate_of_change', 'reason', 'machine'])
        writer.writerow([timestamp, avg_current, baseline_avg, rate_of_change, reason, machine])
    
    # Log to database
    log_alert_to_database(timestamp, avg_current, baseline_avg, rate_of_change, reason, machine)

def log_alert_to_database(timestamp, avg_current, baseline_avg, rate_of_change, reason, machine):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO jamming_data 
    (timestamp, average_current_value, baseline_average_value, rate_of_change, reason, machine)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    try:
        cursor.execute(insert_query, (
            timestamp,
            float(avg_current),
            float(baseline_avg),
            float(rate_of_change),
            reason, 
            float(machine)
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
    
    # Handle empty dataframe case
    if range_data.empty:
        return 0, 0
        
    current_avg = range_data['web_puller_current'].mean()
    one_sec_ago = df['timestamp'].max() - timedelta(seconds=1)
    past_data = range_data[range_data['timestamp'] < one_sec_ago]
    
    # Handle empty past_data case
    if past_data.empty:
        return current_avg, 0
        
    past_avg = past_data['web_puller_current'].mean()
    rate_of_change = (current_avg - past_avg) / 3
    return current_avg, rate_of_change

def calculate_baseline(df):
    mask = (df['cam_position'] >= 50) & (df['cam_position'] <= 250)
    range_data = df[mask]
    
    # Handle empty dataframe case
    if range_data.empty:
        return pd.Series(dtype=float), range_data
        
    baseline = range_data.groupby('cam_position')['web_puller_current'].mean()
    return baseline, range_data

def adaptive_savgol_filter(data, window_length, polyorder):
    # Handle empty data
    if len(data) == 0:
        return []
        
    n_points = len(data)
    if n_points < window_length:
        window_length = n_points if n_points % 2 != 0 else n_points - 1
    if window_length < polyorder + 1:
        polyorder = window_length - 1 if window_length > 1 else 0
    
    if window_length > 2 and polyorder >= 0:
        return savgol_filter(data, window_length, polyorder)
    else:
        return data

def find_baseline_peaks(baseline):
    # Handle empty baseline
    if len(baseline) == 0:
        return [], []
        
    peaks, _ = find_peaks(baseline.values, height=0, distance=5)
    
    # Handle no peaks found
    if len(peaks) == 0:
        return [], []
        
    return baseline.index[peaks], baseline.values[peaks]

# Initialize Dash app
app = dash.Dash(__name__, 
                meta_tags=[{"name": "viewport", 
                           "content": "width=device-width, initial-scale=1"}],
                suppress_callback_exceptions=True)

# Custom CSS for full width
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                margin: 0;
                padding: 0;
                width: 100%;
                height: 100vh;
            }
            #react-entry-point {
                width: 100%;
                height: 100%;
            }
            .main-container {
                width: 98vw;
                margin: 0 auto;
                padding: 10px;
            }
            .graph-container {
                width: 100%;
                height: calc(100vh - 200px);
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Define the layout with full-width styling
app.layout = html.Div(className='main-container', children=[
    # Header
    html.Div([
        html.H1("Jamming Analysis Dashboard", 
                style={'textAlign': 'center', 'margin': '10px'})
    ]),
    
    # Status Panel
    html.Div([
        html.Div([
            html.Div(id='machine-status', 
                    style={'fontSize': '20px', 'fontWeight': 'bold'}),
            html.Div(id='latest-timestamp', 
                    style={'fontSize': '16px'}),
            html.Div(id='current-metrics', 
                    style={'fontSize': '16px'}),
            html.Div(id='alert-text', 
                    style={'fontSize': '18px', 'color': 'red', 'fontWeight': 'bold'})
        ], style={
            'backgroundColor': '#f8f9fa',
            'padding': '15px',
            'borderRadius': '5px',
            'marginBottom': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        })
    ]),
    
    # Graph Container
    html.Div([
        dcc.Graph(
            id='live-graph',
            style={
                'height': 'calc(100vh - 200px)',  # Responsive height
                'width': '100%'  # Full width
            },
            config={'displayModeBar': True}
        )
    ], className='graph-container'),
    
    # Interval and Store components
    dcc.Interval(id='interval-component', interval=5000, n_intervals=0),  # Changed to 5000ms
    dcc.Store(id='previous-machine-status', data=None),
    dcc.Store(id='status-logged', data=False),
    dcc.Store(id='waiting-for-restart', data=False)
])

@app.callback(
    [Output('live-graph', 'figure'),
     Output('machine-status', 'children'),
     Output('latest-timestamp', 'children'),
     Output('current-metrics', 'children'),
     Output('alert-text', 'children'),
     Output('previous-machine-status', 'data'),
     Output('status-logged', 'data'),
     Output('waiting-for-restart', 'data')],
    [Input('interval-component', 'n_intervals')],
    [State('previous-machine-status', 'data'),
     State('status-logged', 'data'),
     State('waiting-for-restart', 'data')]
)
def update_graph(n_intervals, prev_status, status_logged, waiting_for_restart):
    try:
        # Fetch latest data
        display_data = fetch_latest_data(limit=1000)
        
        # Handle empty dataframe case
        if display_data.empty:
            # Return simple figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No Data Available",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=30, color="white"),
            )
            fig.update_layout(
                plot_bgcolor='gray',
                paper_bgcolor='gray',
                autosize=True,
                margin=dict(l=50, r=50, t=50, b=50),
                height=800
            )
            return (fig, "Machine Status: Unknown", "Latest Timestamp: None", 
                    "No data available", "", None, False, False)
        
        latest_timestamp = display_data['timestamp'].iloc[-1]
        current_machine_status = display_data['status'].iloc[-1]
        
        # Initialize states
        if prev_status is None:
            prev_status = current_machine_status
        if status_logged is None:
            status_logged = False
        if waiting_for_restart is None:
            waiting_for_restart = False
            
        # Create base figure with full-width layout
        fig = go.Figure()
        
        # Configure layout for full width
        fig.update_layout(
            autosize=True,
            margin=dict(l=50, r=50, t=50, b=50),
            height=800
        )
        
        if current_machine_status == 0 or waiting_for_restart:
            # Machine stopped view
            fig.add_annotation(
                text="Machine Stopped<br>Analysis Paused",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=30, color="white"),
            )
            fig.update_layout(
                plot_bgcolor='gray',
                paper_bgcolor='gray'
            )
            metrics_text = "Metrics paused"
            alert_text = ""
        else:
            # Normal operation view
            baseline_data = fetch_latest_data(limit=10000)
            
            # Handle empty baseline data
            if baseline_data.empty:
                fig.add_annotation(
                    text="No Baseline Data Available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False,
                    font=dict(size=30, color="black"),
                )
                metrics_text = "No metrics available"
                alert_text = ""
            else:
                # Get unique cycle IDs
                unique_cycles = baseline_data['spare1'].unique()
                if len(unique_cycles) > 0:
                    last_600_cycles = unique_cycles[-min(600, len(unique_cycles)):]
                    baseline_df = baseline_data[baseline_data['spare1'].isin(last_600_cycles)]
                    baseline, baseline_points = calculate_baseline(baseline_df)
                    
                    # Get recent data - check if unique cycles exist
                    if len(unique_cycles) >= 5:
                        last_5_cycles = unique_cycles[-5:]
                    else:
                        last_5_cycles = unique_cycles
                        
                    filtered_df = display_data[display_data['spare1'].isin(last_5_cycles)]
                    
                    # Calculate metrics
                    current_avg, rate_of_change = calculate_average_and_roc(filtered_df)
                    
                    # Handle empty baseline
                    if len(baseline) > 0:
                        baseline_avg = baseline.mean()
                    else:
                        baseline_avg = 0
                        
                    avg_difference = current_avg - baseline_avg
                    
                    # Add all traces if we have data
                    if not baseline_points.empty:
                        fig.add_trace(go.Scatter(
                            x=baseline_points['cam_position'],
                            y=baseline_points['web_puller_current'],
                            mode='markers',
                            name='Baseline Data',
                            marker=dict(color='lightblue', size=5, opacity=0.3)
                        ))
                    
                    if not filtered_df.empty:
                        fig.add_trace(go.Scatter(
                            x=filtered_df['cam_position'],
                            y=filtered_df['web_puller_current'],
                            mode='markers',
                            name='Current Data',
                            marker=dict(color='blue', size=5)
                        ))
                    
                    if len(baseline) > 0:
                        fig.add_trace(go.Scatter(
                            x=baseline.index,
                            y=baseline.values,
                            mode='lines',
                            name='Baseline',
                            line=dict(color='purple')
                        ))
                    
                    # Add mean lines
                    fig.add_trace(go.Scatter(
                        x=[50, 250],
                        y=[current_avg, current_avg],
                        mode='lines',
                        name=f'Current Avg ({current_avg:.2f})',
                        line=dict(color='red', dash='dash')
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=[50, 250],
                        y=[baseline_avg, baseline_avg],
                        mode='lines',
                        name=f'Baseline Avg ({baseline_avg:.2f})',
                        line=dict(color='green', dash='dot')
                    ))
                    
                    # Add smoothed line if we have filtered data
                    if not filtered_df.empty:
                        best_values = filtered_df.groupby('cam_position')['web_puller_current'].mean().reset_index()
                        if not best_values.empty:
                            best_values = best_values.sort_values('cam_position')
                            window_length = min(15, len(best_values) - 1)
                            if window_length % 2 == 0 and window_length > 0:
                                window_length -= 1
                            if window_length > 0:
                                smoothed_current = adaptive_savgol_filter(best_values['web_puller_current'], max(3, window_length), 1)
                                
                                fig.add_trace(go.Scatter(
                                    x=best_values['cam_position'],
                                    y=smoothed_current,
                                    mode='lines',
                                    name='Smoothed Best Fit',
                                    line=dict(color='orange')
                                ))
                    
                    # Add peaks if baseline has data
                    if len(baseline) > 0:
                        peak_positions, peak_values = find_baseline_peaks(baseline)
                        if len(peak_positions) > 0:
                            fig.add_trace(go.Scatter(
                                x=peak_positions,
                                y=peak_values,
                                mode='markers',
                                name='Baseline Peaks',
                                marker=dict(color='red', size=10)
                            ))
                    
                    # Add analysis region
                    fig.add_shape(
                        type="rect",
                        x0=50, x1=250,
                        y0=0, y1=2.2,
                        fillcolor="lightblue",
                        opacity=0.3,
                        layer="below",
                        line_width=0,
                    )
                    
                    # Check alert conditions
                    alert_condition_1 = current_avg > baseline_avg + 1
                    alert_condition_2 = rate_of_change > 1
                    alert_condition_3 = current_avg > 3
                    alert_condition = alert_condition_1 or alert_condition_2 or alert_condition_3
                    
                    if alert_condition:
                        reasons = []
                        if alert_condition_1:
                            reasons.append("Current avg > Baseline avg + 1A")
                        if alert_condition_2:
                            reasons.append("Rate of change > 1A/s")
                        if alert_condition_3:
                            reasons.append("Current avg > 3A")
                        reason_str = " & ".join(reasons)
                        alert_text = f"ALERT: {reason_str}"
                        
                        # Log alert
                        log_alert(latest_timestamp, current_avg, baseline_avg, rate_of_change, reason_str, machine)
                        
                        fig.update_layout(plot_bgcolor='mistyrose')
                    else:
                        alert_text = ""
                        fig.update_layout(plot_bgcolor='white')
                    
                    # Format metrics text
                    metrics_text = [
                        f"Current Average (50-250): {current_avg:.4f} A",
                        f"Rate of Change (1s): {rate_of_change:.4f} A/s",
                        f"Baseline Average: {baseline_avg:.4f} A",
                        f"Avg Difference: {avg_difference:.4f} A"
                    ]
                    metrics_text = html.Div([html.P(m, style={'margin': '5px 0'}) for m in metrics_text])
                    
                    # Update figure layout
                    try:
                        cycle_id = display_data["spare1"].max()
                        fig.update_layout(
                            title=f'Cycle ID: {cycle_id}',
                            xaxis_title=f'Machine {machine} Cam Position',
                            yaxis_title=f'Machine {machine} Servo Current',
                            showlegend=True,
                            legend=dict(
                                yanchor="top",
                                y=0.99,
                                xanchor="left",
                                x=0.01,
                                bgcolor='rgba(255, 255, 255, 0.8)'
                            )
                        )
                    except:
                        fig.update_layout(
                            title='No Cycle ID Available',
                            xaxis_title=f'Machine {machine} Cam Position',
                            yaxis_title=f'Machine {machine} Servo Current',
                            showlegend=True,
                            legend=dict(
                                yanchor="top",
                                y=0.99,
                                xanchor="left",
                                x=0.01,
                                bgcolor='rgba(255, 255, 255, 0.8)'
                            )
                        )
                else:
                    # No cycles available
                    fig.add_annotation(
                        text="No Cycle Data Available",
                        xref="paper", yref="paper",
                        x=0.5, y=0.5,
                        showarrow=False,
                        font=dict(size=30, color="black"),
                    )
                    metrics_text = "No metrics available"
                    alert_text = ""
                    fig.update_layout(plot_bgcolor='white')
        
        # Handle machine status changes
        machine_status_text = f"Machine Status: {'Running' if current_machine_status == 1 else 'Stopped'}"
        timestamp_text = f"Latest Timestamp: {latest_timestamp}"
        
        if current_machine_status != prev_status:
            if current_machine_status == 0 and not status_logged:
                try:
                    log_alert(latest_timestamp, 0, 0, 0, "Machine Stopped", machine)
                    status_logged = True
                    waiting_for_restart = False
                except Exception as e:
                    print(f"Error logging machine stop: {e}")
            elif current_machine_status == 1 and not waiting_for_restart:
                waiting_for_restart = True
                # Reduced sleep time to avoid callback timeouts
                time.sleep(0.5)
                if not status_logged:
                    try:
                        log_alert(latest_timestamp, 0, 0, 0, "Machine Started", machine)
                        status_logged = True
                    except Exception as e:
                        print(f"Error logging machine start: {e}")
                waiting_for_restart = False
        
        if current_machine_status != prev_status:
            status_logged = False
        
        # Ensure the figure has x and y axes defined
        fig.update_xaxes(range=[0, 360])
        fig.update_yaxes(range=[0, 5])
        
        return (fig, machine_status_text, timestamp_text, metrics_text, alert_text,
                current_machine_status, status_logged, waiting_for_restart)
                
    except Exception as e:
        # Error handling
        print(f"Error in callback: {e}")
        # Create simple error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=20, color="red"),
        )
        fig.update_layout(
            plot_bgcolor='lightgray',
            paper_bgcolor='lightgray',
            autosize=True,
            margin=dict(l=50, r=50, t=50, b=50),
            height=800
        )
        return (fig, "Machine Status: Error", "Latest Timestamp: Error", 
                "Error in data processing", f"Error: {str(e)}", 
                prev_status, status_logged, waiting_for_restart)

if __name__ == '__main__':
    # Run the server on all available network interfaces
    try:
        app.run_server(debug=True, host='0.0.0.0', port=9000 + machine, dev_tools_ui=False)
    except Exception as e:
        print(f"Server error: {e}")
