#!/usr/bin/env python3
"""
Sync Analysis Visualization (Read-only) - Improved Version
Reads PLC data from PostgreSQL (table: mc25_fast)
and visualizes synchronization between key machine axes.
Saves plots as PNG files instead of displaying them

Improvements:
- Sliding window correlation for temporal resolution
- Cross-correlation to detect time lags
- Threshold filtering to reduce noise impact
"""

import sys
import psycopg2
import pandas as pd
import numpy as np
from collections import deque
from scipy import signal
from scipy.stats import pearsonr
import matplotlib.pyplot as plt
import time
import os

# ============================
# Database Configuration
# ============================
DB_CONFIG = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "100.96.244.68",
    "port": "5432"
}

# ============================
# Sync Analysis Configuration
# ============================
NOISE_THRESHOLD_PERCENTILE = 10  # Ignore movements below 10th percentile
SLIDING_WINDOW_SIZE = 10  # Size of sliding window for correlation
MAX_LAG_SAMPLES = 20  # Maximum lag to check in cross-correlation
UPDATE_INTERVAL = 1  # Seconds between updates
OUTPUT_DIR = "sync_analysis_plots"  # Directory to save plots

# ============================
# Database Functions
# ============================

def get_latest_cycle_id():
    """Fetch the latest cycle_id from mc25_fast table."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT cycle_id FROM mc25_fast ORDER BY timestamp DESC LIMIT 1;")
            result = cur.fetchone()
            return result[0] if result else None

def fetch_data_by_cycle_id(cycle_id):
    """Fetch relevant columns for the given cycle_id."""
    query = """
        SELECT 
            horizontal_sealer_position,
            vertical_sealer_position,
            piston_position_right,
            piston_position_left,
            rotary_valve_position,
            puller_tq*(-1.0) AS puller_tq,
            timestamp,
            cycle_id,
            status
        FROM mc25_fast
        WHERE cycle_id = %s
        ORDER BY timestamp DESC;
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql_query(query, conn, params=(cycle_id,))
    return df

# ============================
# Improved Analysis Functions
# ============================

def apply_threshold_filter(series, percentile=NOISE_THRESHOLD_PERCENTILE):
    """
    Apply threshold filtering to reduce noise impact.
    Movements below the percentile threshold are set to zero.
    """
    series = np.array(series)
    diff = np.diff(series)
    threshold = np.percentile(np.abs(diff), percentile)
    
    # Set small movements to zero
    diff[np.abs(diff) < threshold] = 0
    
    # Reconstruct the filtered series
    filtered = np.zeros_like(series)
    filtered[0] = series[0]
    for i in range(1, len(series)):
        filtered[i] = filtered[i-1] + diff[i-1]
    
    return filtered

def calculate_cross_correlation(series1, series2, max_lag=MAX_LAG_SAMPLES):
    """
    Calculate cross-correlation to find optimal time lag between signals.
    Returns: max correlation value, optimal lag, and lag-compensated correlation
    """
    # Normalize the series
    s1 = (series1 - np.mean(series1)) / (np.std(series1) + 1e-10)
    s2 = (series2 - np.mean(series2)) / (np.std(series2) + 1e-10)
    
    # Calculate cross-correlation
    correlation = signal.correlate(s2, s1, mode='full')
    lags = signal.correlation_lags(len(s2), len(s1), mode='full')
    
    # Find the lag with maximum correlation within the specified range
    lag_mask = (lags >= -max_lag) & (lags <= max_lag)
    valid_corr = correlation[lag_mask]
    valid_lags = lags[lag_mask]
    
    if len(valid_corr) > 0:
        max_corr_idx = np.argmax(np.abs(valid_corr))
        max_corr = valid_corr[max_corr_idx]
        optimal_lag = valid_lags[max_corr_idx]
        
        # Normalize correlation by length
        max_corr_normalized = max_corr / len(series1)
        
        # Calculate correlation with lag compensation
        if optimal_lag > 0:
            # series2 lags series1
            if optimal_lag < len(series1):
                corr_value, _ = pearsonr(series1[:-optimal_lag], series2[optimal_lag:])
            else:
                corr_value = 0
        elif optimal_lag < 0:
            # series1 lags series2
            lag = abs(optimal_lag)
            if lag < len(series1):
                corr_value, _ = pearsonr(series1[lag:], series2[:-lag])
            else:
                corr_value = 0
        else:
            corr_value, _ = pearsonr(series1, series2)
        
        return max_corr_normalized, optimal_lag, abs(corr_value) * 100
    
    return 0, 0, 0

def sliding_window_correlation(series1, series2, window_size=SLIDING_WINDOW_SIZE):
    """
    Calculate correlation in sliding windows for better temporal resolution.
    Returns: average correlation percentage and list of windowed correlations
    """
    if len(series1) < window_size or len(series2) < window_size:
        return 0, []
    
    correlations = []
    
    for i in range(len(series1) - window_size + 1):
        window1 = series1[i:i + window_size]
        window2 = series2[i:i + window_size]
        
        # Check if there's variation in the window (avoid constant values)
        if np.std(window1) > 1e-10 and np.std(window2) > 1e-10:
            corr, _ = pearsonr(window1, window2)
            correlations.append(abs(corr))
        else:
            correlations.append(0)
    
    avg_correlation = np.mean(correlations) * 100 if correlations else 0
    return avg_correlation, correlations

def comprehensive_sync_analysis(series1, series2, label=""):
    """
    Perform comprehensive synchronization analysis combining all methods.
    Returns dictionary with sync metrics.
    """
    # Apply threshold filtering
    filtered1 = apply_threshold_filter(series1)
    filtered2 = apply_threshold_filter(series2)
    
    # Calculate sliding window correlation
    sliding_corr, window_corrs = sliding_window_correlation(filtered1, filtered2)
    
    # Calculate cross-correlation and lag
    max_corr, optimal_lag, lag_compensated_corr = calculate_cross_correlation(filtered1, filtered2)
    
    # Calculate original in-phase percentage (for comparison)
    diff1 = np.diff(filtered1)
    diff2 = np.diff(filtered2)
    mask = (np.abs(diff1) > 0) & (np.abs(diff2) > 0)
    if np.sum(mask) > 0:
        same = np.sign(diff1[mask]) == np.sign(diff2[mask])
        in_phase_pct = np.sum(same) / len(same) * 100
    else:
        in_phase_pct = 0
    
    # Combined sync score (weighted average)
    sync_score = (
        sliding_corr * 0.4 +  # 40% weight to sliding correlation
        lag_compensated_corr * 0.4 +  # 40% weight to lag-compensated correlation
        in_phase_pct * 0.2  # 20% weight to direction matching
    )
    
    return {
        'sync_score': sync_score,
        'sliding_correlation': sliding_corr,
        'lag_compensated_correlation': lag_compensated_corr,
        'optimal_lag': optimal_lag,
        'in_phase_percentage': in_phase_pct,
        'window_correlations': window_corrs,
        'max_cross_correlation': max_corr
    }

# ============================
# Visualization Functions
# ============================

def create_plot(df, cycle_id, sync_metrics, avg_buffers):
    """Create and save a plot for the given cycle data."""
    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Initialize figure
    fig = plt.figure(figsize=(14, 11), facecolor='#1C1C1C')
    axs = fig.subplots(2, 2)
    fig.suptitle(f"Enhanced Sync Analysis - Cycle ID: {cycle_id}", fontsize=20, fontweight='bold', color='white')

    new_alerts = set()

    # Plot each comparison
    plot_enhanced_comparison(axs[0, 0], df, 'vertical_sealer_position', 
                            'horizontal_sealer_position', 'Vertical vs Horizontal Sealer', 
                            'vs_hs', 70, False, 1, new_alerts, sync_metrics, avg_buffers)

    plot_enhanced_comparison(axs[0, 1], df, 'piston_position_right', 
                            'rotary_valve_position', 'Piston vs Rotary Valve', 
                            'fp_rv', 40, True, 2, new_alerts, sync_metrics, avg_buffers)

    plot_enhanced_comparison(axs[1, 0], df, 'puller_tq', 
                            'horizontal_sealer_position', 'Puller Torque vs Horizontal Sealer', 
                            'ps_hs', 40, True, 3, new_alerts, sync_metrics, avg_buffers)

    plot_enhanced_comparison(axs[1, 1], df, 'rotary_valve_position', 
                            'vertical_sealer_position', 'Rotary Valve vs Vertical Sealer', 
                            'rv_vs', 70, False, 4, new_alerts, sync_metrics, avg_buffers)

    # Add alert message
    alert_message = "\n".join(new_alerts) if new_alerts else "All Axes Synchronized"
    fig.text(0.5, 0.02, alert_message, ha="center", va="bottom",
             bbox=dict(facecolor='#1C1C1C', edgecolor='#1C1C1C', boxstyle='round,pad=0.5'),
             fontweight='bold', fontsize=20, color='white')

    fig.tight_layout(rect=[0, 0.1, 1, 0.95])

    # Save the plot
    output_path = os.path.join(OUTPUT_DIR, f"sync_analysis_cycle_{cycle_id}.png")
    plt.savefig(output_path, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved plot for cycle {cycle_id} to {output_path}")

def plot_enhanced_comparison(ax, df, y1, y2, title, key, threshold, alert_above, idx, alerts, sync_metrics, avg_buffers):
    """Plot a single comparison subplot."""
    # Apply threshold filtering for display
    filtered1 = apply_threshold_filter(df[y1].values)
    filtered2 = apply_threshold_filter(df[y2].values)
    
    # Plot filtered signals
    ax.plot(df['timestamp'], filtered1, label=y1.replace('_', ' ').title() + ' (filtered)', 
            linewidth=2, alpha=0.8)
    ax.plot(df['timestamp'], filtered2, label=y2.replace('_', ' ').title() + ' (filtered)', 
            linewidth=2, alpha=0.8)
    
    # Plot original signals in background (lighter)
    ax.plot(df['timestamp'], df[y1], alpha=0.3, linewidth=1)
    ax.plot(df['timestamp'], df[y2], alpha=0.3, linewidth=1)

    # Get metrics
    metrics = sync_metrics[key]
    avg_sync = np.mean(avg_buffers[key])
    
    # Display enhanced metrics
    metrics_text = (
        f'Sync Score: {avg_sync:.1f}%\n'
        f'Sliding Corr: {metrics.get("sliding_correlation", 0):.1f}%\n'
        f'Lag Comp Corr: {metrics.get("lag_compensated_correlation", 0):.1f}%\n'
        f'Optimal Lag: {metrics.get("optimal_lag", 0)} samples'
    )
    
    ax.text(0.95, 0.95, metrics_text,
            transform=ax.transAxes, fontsize=10, verticalalignment='top', 
            horizontalalignment='right',
            bbox=dict(facecolor='#2C2C2C', alpha=0.9, edgecolor='white', boxstyle='round,pad=0.5'), 
            color='white', family='monospace')

    ax.set_title(title, color='white', fontweight='bold')
    ax.set_xlabel('PLC Timestamp', color='white')
    ax.set_ylabel('Position / Torque', color='white')
    ax.legend(loc='upper left', facecolor='#1C1C1C', edgecolor='#1C1C1C', 
              labelcolor='white', fontsize=9)

    ax.set_facecolor('#1C1C1C')
    ax.tick_params(axis='both', colors='white', labelsize=9)
    for spine in ax.spines.values():
        spine.set_color('white')

    # Alert based on sync score
    if (alert_above and avg_sync > threshold) or (not alert_above and avg_sync < threshold):
        ax.set_facecolor('#8B0000')
        lag_info = f" (lag: {metrics.get('optimal_lag', 0)} samples)" if abs(metrics.get('optimal_lag', 0)) > 5 else ""
        alerts.add(f"Plot {idx}: {title} out of sync (score: {avg_sync:.1f}%){lag_info}")

    # Add grid for better readability
    ax.grid(True, alpha=0.2, color='white', linestyle='--')

    for text in ax.get_legend().get_texts():
        text.set_color('white')

def display_machine_stopped(cycle_id):
    """Create a plot indicating the machine is stopped."""
    fig = plt.figure(figsize=(14, 11), facecolor='#1C1C1C')
    ax = fig.add_subplot(111)
    ax.set_facecolor('#1C1C1C')
    ax.set_xticks([])
    ax.set_yticks([])
    fig.suptitle("Machine Stopped", fontsize=25, fontweight='bold', color='white')
    
    output_path = os.path.join(OUTPUT_DIR, f"sync_analysis_cycle_{cycle_id}_stopped.png")
    plt.savefig(output_path, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved machine stopped plot for cycle {cycle_id} to {output_path}")

# ============================
# Main Analysis Loop
# ============================

def main():
    # Buffers for moving averages
    cycle_buffer = deque(maxlen=3)
    avg_buffers = {
        'vs_hs': deque(maxlen=100),
        'fp_rv': deque(maxlen=100),
        'ps_hs': deque(maxlen=100),
        'rv_vs': deque(maxlen=100)
    }
    
    # Store detailed sync metrics
    sync_metrics = {
        'vs_hs': {},
        'fp_rv': {},
        'ps_hs': {},
        'rv_vs': {}
    }

    while True:
        latest_cycle = get_latest_cycle_id()
        if not latest_cycle:
            print("No cycle data found. Waiting...")
            time.sleep(UPDATE_INTERVAL)
            continue

        if not cycle_buffer or cycle_buffer[-1] != latest_cycle:
            cycle_buffer.append(latest_cycle)

        if len(cycle_buffer) == 3:
            cycle_to_process = cycle_buffer.popleft()
            print(f"Processing cycle_id (delayed): {cycle_to_process}")
            df = fetch_data_by_cycle_id(cycle_to_process)
            if not df.empty:
                machine_status = df['status'].iloc[0]
                machine_running = bool(machine_status)

                if not machine_running:
                    display_machine_stopped(cycle_to_process)
                    time.sleep(UPDATE_INTERVAL)
                    continue

                # Perform comprehensive sync analysis for each pair
                vs_hs_metrics = comprehensive_sync_analysis(
                    df['vertical_sealer_position'].values, 
                    df['horizontal_sealer_position'].values,
                    "VS-HS"
                )
                
                fp_rv_metrics = comprehensive_sync_analysis(
                    df['piston_position_right'].values,
                    df['rotary_valve_position'].values,
                    "FP-RV"
                )
                
                ps_hs_metrics = comprehensive_sync_analysis(
                    df['puller_tq'].values,
                    df['horizontal_sealer_position'].values,
                    "PS-HS"
                )
                
                rv_vs_metrics = comprehensive_sync_analysis(
                    df['rotary_valve_position'].values,
                    df['vertical_sealer_position'].values,
                    "RV-VS"
                )

                # Store metrics
                sync_metrics['vs_hs'] = vs_hs_metrics
                sync_metrics['fp_rv'] = fp_rv_metrics
                sync_metrics['ps_hs'] = ps_hs_metrics
                sync_metrics['rv_vs'] = rv_vs_metrics

                # Update moving averages with sync scores
                avg_buffers['vs_hs'].append(vs_hs_metrics['sync_score'])
                avg_buffers['fp_rv'].append(fp_rv_metrics['sync_score'])
                avg_buffers['ps_hs'].append(ps_hs_metrics['sync_score'])
                avg_buffers['rv_vs'].append(rv_vs_metrics['sync_score'])

                # Create and save the plot
                create_plot(df, cycle_to_process, sync_metrics, avg_buffers)

        time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    main()
