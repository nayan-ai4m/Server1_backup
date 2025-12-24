import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.figure_factory as ff
import plotly.graph_objs as go
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

class StatusAnalyzer:
    def __init__(self):
        self.conn_params = {
            'dbname': 'hul',
            'user': 'postgres',
            'password': 'ai4m2024',
            'host': 'localhost',
            'port': '5432'
        }
        self.table_name = 'mc19'

    def connect(self):
        return psycopg2.connect(**self.conn_params)

    def get_consolidated_status_durations(self, start_time=None, end_time=None):
        with self.connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = f"""
                    WITH status_periods AS (
                        SELECT 
                            timestamp,
                            status,
                            status_code,
                            LEAD(timestamp) OVER (ORDER BY timestamp) as next_timestamp,
                            LAG(status_code) OVER (ORDER BY timestamp) as prev_status_code
                        FROM {self.table_name}
                        WHERE 1=1
                        {f"AND timestamp >= '{start_time}'" if start_time else ""}
                        {f"AND timestamp <= '{end_time}'" if end_time else ""}
                        ORDER BY timestamp
                    ),
                    status_changes AS (
                        SELECT 
                            timestamp,
                            status,
                            status_code,
                            next_timestamp,
                            CASE 
                                WHEN status_code != prev_status_code 
                                OR prev_status_code IS NULL 
                                THEN 1 
                                ELSE 0 
                            END as is_change_point
                        FROM status_periods
                    ),
                    grouped_periods AS (
                        SELECT 
                            timestamp,
                            status,
                            status_code,
                            next_timestamp,
                            SUM(is_change_point) OVER (ORDER BY timestamp) as group_id
                        FROM status_changes
                    )
                    SELECT 
                        MIN(timestamp) as start_time,
                        MAX(next_timestamp) as end_time,
                        status,
                        status_code,
                        EXTRACT(EPOCH FROM (MAX(next_timestamp) - MIN(timestamp))) as duration_seconds
                    FROM grouped_periods
                    WHERE next_timestamp IS NOT NULL
                    GROUP BY group_id, status, status_code
                    ORDER BY MIN(timestamp);
                """
                cur.execute(query)
                return cur.fetchall()

# Initialize the Dash app
app = dash.Dash(__name__)

# Create instance of StatusAnalyzer
analyzer = StatusAnalyzer()

# Default time range
DEFAULT_START = "2025-01-22"
DEFAULT_START_TIME = "00:00:00"
DEFAULT_END = "2025-01-22"
DEFAULT_END_TIME = "23:53:00"

# Define color scheme for status codes
COLOR_SCHEME = {
    'Machine Status 0.0': '#2ecc71',   # Green for Running/Good Status
    'Machine Status 2.0': '#3498db',   # Blue
    'Machine Status 19.0': '#e74c3c',  # Red
    'Machine Status 20.0': '#f1c40f',  # Yellow
    'Machine Status 21.0': '#9b59b6',  # Purple
    'Machine Status 77.0': '#e67e22',  # Orange
    'Machine Status 78.0': '#1abc9c',  # Turquoise
    'Machine Status 81.0': '#34495e',  # Dark Blue
    'Machine Status 82.0': '#d35400',  # Dark Orange
    'Machine Status 83.0': '#27ae60',  # Dark Green
    'Machine Status 84.0': '#8e44ad',  # Dark Purple
    'Machine Status 201.0': '#c0392b', # Dark Red
    'Machine Status 202.0': '#16a085', # Sea Green
}

# Layout
app.layout = html.Div([
    html.H1('Machine Status Timeline', 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 30}),
    
    # Time range selector
    html.Div([
        html.Div([
            html.Label('Start Date:', style={'fontSize': 16, 'marginRight': 10}),
            dcc.DatePickerSingle(
                id='start-date',
                date=DEFAULT_START,
                style={'fontSize': 16}
            ),
            html.Label('Start Time:', style={'fontSize': 16, 'marginLeft': 20, 'marginRight': 10}),
            dcc.Input(
                id='start-time',
                type='text',
                value=DEFAULT_START_TIME,
                style={'fontSize': 16}
            ),
        ], style={'marginBottom': 10}),
        
        html.Div([
            html.Label('End Date:', style={'fontSize': 16, 'marginRight': 10}),
            dcc.DatePickerSingle(
                id='end-date',
                date=DEFAULT_END,
                style={'fontSize': 16}
            ),
            html.Label('End Time:', style={'fontSize': 16, 'marginLeft': 20, 'marginRight': 10}),
            dcc.Input(
                id='end-time',
                type='text',
                value=DEFAULT_END_TIME,
                style={'fontSize': 16}
            ),
        ]),
    ], style={'marginBottom': 30, 'textAlign': 'center'}),
    
    # Timeline graph
    html.Div([
        dcc.Graph(id='status-timeline')
    ])
], style={'padding': '20px'})

@app.callback(
    Output('status-timeline', 'figure'),
    [Input('start-date', 'date'),
     Input('start-time', 'value'),
     Input('end-date', 'date'),
     Input('end-time', 'value')]
)
def update_timeline(start_date, start_time, end_date, end_time):
    start_datetime = f"{start_date} {start_time}"
    end_datetime = f"{end_date} {end_time}"
    
    sequence = analyzer.get_consolidated_status_durations(start_datetime, end_datetime)
    
    # Create timeline data with proper sorting
    timeline_data = []
    for entry in sequence:
        status_label = f"Machine Status {entry['status_code']}.0"
        timeline_data.append(dict(
            Task=status_label,
            Start=entry['start_time'],
            Finish=entry['end_time'],
            Resource=status_label,
            Status_Code=float(entry['status_code'])
        ))
    
    # Convert to DataFrame and sort
    df_timeline = pd.DataFrame(timeline_data)
    df_timeline = df_timeline.sort_values(by='Status_Code', ascending=True)
    
    # Assign colors to statuses
    unique_statuses = df_timeline['Task'].unique()
    default_colors = ['#2ecc71', '#3498db', '#e74c3c', '#f1c40f', '#9b59b6', '#e67e22', 
                     '#1abc9c', '#34495e', '#d35400', '#27ae60', '#8e44ad', '#c0392b']
    colors = {}
    color_idx = 0
    
    for status in unique_statuses:
        if status in COLOR_SCHEME:
            colors[status] = COLOR_SCHEME[status]
        else:
            colors[status] = default_colors[color_idx % len(default_colors)]
            color_idx += 1
    
    # Create Gantt chart
    fig = ff.create_gantt(
        df_timeline,
        colors=colors,
        index_col='Resource',
        show_colorbar=True,
        group_tasks=True,
        showgrid_x=True,
        showgrid_y=True,
        height=600,
    )
    
    # Update layout
    fig.update_layout(
        title=None,
        xaxis_title=None,
        yaxis_title=None,
        yaxis={'categoryorder': 'category ascending'},
        plot_bgcolor='rgba(240,240,240,0.5)',
        paper_bgcolor='white',
        font={'size': 12},
        showlegend=True,
        margin=dict(l=100, r=20, t=20, b=20)
    )
    
    # Update axes
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        showline=True,
        linewidth=1,
        linecolor='Gray'
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        showline=True,
        linewidth=1,
        linecolor='Gray'
    )
    
    return fig

if __name__ == '__main__':
    app.run_server(debug=True, port=8000)
