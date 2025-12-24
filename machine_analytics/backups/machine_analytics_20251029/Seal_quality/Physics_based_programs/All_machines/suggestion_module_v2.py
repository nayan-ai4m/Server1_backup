#!/usr/bin/env python3
"""
Suggestion Module V2 - Handles both temperature and torque-based suggestions
For MC25 and MC26 machines
"""

import pandas as pd
import numpy as np
import json
import math
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
import os
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from scipy.optimize import minimize

# ===== Global storage for models and data per machine =====
_models = {}  # Format: {machine_id: {'torque_model': model, 'torque_poly': poly, 'pressure_model': model, ...}}

def setup_logger():
    """Setup daily rotating logger"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{today}_suggestion_module_v2.log")

    logger = logging.getLogger("SuggestionModuleV2")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8"
        )
        handler.suffix = "%Y-%m-%d"
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

# Initialize logger
logger = setup_logger()

# ===== Load config for specific machine =====
def load_config(machine_id):
    """Load config for specific machine"""
    config_file = f"config_{machine_id}.json"
    try:
        with open(config_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Config file {config_file} not found for {machine_id}")
        return None
    except Exception as e:
        logger.error(f"Error loading config for {machine_id}: {e}")
        return None

# ===== Initialize TORQUE model for specific machine =====
def initialize_torque_model(machine_id):
    """Initialize torque-stroke ML model for specific machine"""
    global _models
    
    try:
        # Load stroke-torque dataset for this machine
        csv_file = f"stroke_torque_log_{machine_id}.csv"
        
        if not os.path.exists(csv_file):
            logger.warning(f"Stroke torque file {csv_file} not found for {machine_id}")
            return False
        
        df = pd.read_csv(csv_file)
        
        # Validate required columns
        required_columns = ['stroke_1', 'stroke_2', 'avg_torque']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing columns {missing_columns} in {csv_file}")
            return False
        
        if df.empty:
            logger.warning(f"Empty dataset in {csv_file}")
            return False
        
        X = df[['stroke_1', 'stroke_2']].values
        y = df['avg_torque'].values

        # Fit polynomial regression
        degree = 4
        poly = PolynomialFeatures(degree=degree)
        X_poly = poly.fit_transform(X)
        model = LinearRegression()
        model.fit(X_poly, y)
        
        # Store model data for this machine
        if machine_id not in _models:
            _models[machine_id] = {}
        
        _models[machine_id].update({
            'torque_model': model,
            'torque_poly': poly,
            'torque_df': df,
            'torque_initialized': True
        })
        
        logger.info(f"Torque model initialized successfully for {machine_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing torque model for {machine_id}: {e}")
        return False

# ===== Initialize PRESSURE model for specific machine (legacy support) =====
def initialize_pressure_model(machine_id):
    """Initialize pressure-stroke ML model for specific machine (for machines that use pressure)"""
    global _models
    
    try:
        csv_file = f"stroke_pressure_log_{machine_id}.csv"
        
        if not os.path.exists(csv_file):
            # Not all machines may have pressure data, this is okay
            return False
        
        df = pd.read_csv(csv_file)
        
        required_columns = ['stroke_1', 'stroke_2', 'avg_pressure']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return False
        
        if df.empty:
            return False
        
        X = df[['stroke_1', 'stroke_2']].values
        y = df['avg_pressure'].values

        degree = 4
        poly = PolynomialFeatures(degree=degree)
        X_poly = poly.fit_transform(X)
        model = LinearRegression()
        model.fit(X_poly, y)
        
        if machine_id not in _models:
            _models[machine_id] = {}
        
        _models[machine_id].update({
            'pressure_model': model,
            'pressure_poly': poly,
            'pressure_df': df,
            'pressure_initialized': True
        })
        
        logger.info(f"Pressure model initialized successfully for {machine_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing pressure model for {machine_id}: {e}")
        return False

# ===== Initialize all models for a machine =====
def initialize_models(machine_id):
    """Initialize all available models for specific machine"""
    torque_success = initialize_torque_model(machine_id)
    pressure_success = initialize_pressure_model(machine_id)
    
    if not torque_success and not pressure_success:
        logger.warning(f"No models could be initialized for {machine_id}")
        return False
    
    return True

# ===== Predict torque for specific machine =====
def predict_torque(machine_id, stroke1, stroke2):
    """Predict torque for specific machine given stroke values"""
    if machine_id not in _models or not _models[machine_id].get('torque_initialized', False):
        if not initialize_torque_model(machine_id):
            logger.info(f"Cannot predict torque for {machine_id}: model not initialized")
            return None
    
    try:
        model_data = _models[machine_id]
        X_in = np.array([[stroke1, stroke2]])
        X_in_poly = model_data['torque_poly'].transform(X_in)
        return model_data['torque_model'].predict(X_in_poly)[0]
    except Exception as e:
        logger.error(f"Error predicting torque for {machine_id}: {e}")
        return None

# ===== Get required strokes for torque =====
def get_required_strokes_for_torque(machine_id, required_torque):
    """Get required strokes for specific machine to achieve target torque"""
    if machine_id not in _models or not _models[machine_id].get('torque_initialized', False):
        if not initialize_torque_model(machine_id):
            logger.info(f"Cannot get required strokes for {machine_id}: torque model not initialized")
            return None, None
    
    try:
        model_data = _models[machine_id]
        df = model_data['torque_df']
        
        def objective(vars):
            s1, s2 = vars
            pred = predict_torque(machine_id, s1, s2)
            if pred is None:
                return 1e6
            return (pred - required_torque) ** 2

        s1_min, s1_max = df['stroke_1'].min(), df['stroke_1'].max()
        s2_min, s2_max = df['stroke_2'].min(), df['stroke_2'].max()
        bounds = [(s1_min, s1_max), (s2_min, s2_max)]

        initial_guess = [np.mean([s1_min, s1_max]), np.mean([s2_min, s2_max])]
        
        result = minimize(objective, initial_guess, bounds=bounds)
        
        if result.success:
            return result.x[0], result.x[1]
        else:
            logger.info(f"Optimization failed for {machine_id}: {result.message}")
            return None, None
            
    except Exception as e:
        logger.error(f"Error getting required strokes for torque in {machine_id}: {e}")
        return None, None

# ===== Calculate BOPP surface temperature =====
def calculate_bopp_surface_temp(config):
    """Calculate BOPP surface temperature using thermal resistance"""
    try:
        Ti = config["ambient_temp"] + 30
        T_back = config.get("SIT", 97.0)
        T_eff = config.get("T_eff", 0.692)

        k_A = config["k_A"]
        k_B = config["k_B"] 
        k_C = config["k_C"]
        d1 = config["d1"]  # LDPE thickness
        d2 = config["d2"]  # metBOPP thickness  
        d3 = config["d3"]  # BOPP thickness

        # Use the thermal diffusivity values from config
        alpha_LDPE = config.get("alpha_LDPE", 1.92e-07)
        alpha_metBOPP = config.get("alpha_metBOPP", 1.7e-07)
        alpha_BOPP = config.get("alpha_BOPP", 1.46e-07)

        # Use d1, d2, d3 as the layer thicknesses
        L_total = d1 + d2 + d3
        t = config["t"]

        # Calculate thermal resistance
        thermal_resistance = (d1 / k_A) + (d2 / k_B) + (d3 / k_C)
        
        # Base effective diffusivity using d1, d2, d3 as weights
        alpha_eff = (
            alpha_LDPE * d1 +
            alpha_metBOPP * d2 +
            alpha_BOPP * d3
        ) / L_total
        
        # Apply thermal resistance factor
        alpha_eff_adjusted = alpha_eff / (1 + thermal_resistance)

        # Calculate with adjusted diffusivity
        eta = L_total / (2.0 * math.sqrt(alpha_eff_adjusted * t))
        erfc_eta = math.erfc(eta)
        Ts = Ti + (T_back - Ti) / erfc_eta
        
        # Apply thermal resistance correction
        temperature_correction_factor = 1 + (thermal_resistance * 0.1)
        
        return (Ts / T_eff) * temperature_correction_factor
        
    except Exception as e:
        logger.error(f"Error calculating BOPP surface temperature: {e}")
        return None

# ===== Main suggestion function =====
def get_suggestions(machine_id):
    """
    Get suggestions based on current config values for specific machine.
    Now handles both torque and pressure based systems.
    """
    try:
        # Load fresh config
        config = load_config(machine_id)
        if config is None:
            logger.info(f"Cannot get suggestions for {machine_id}: config not available")
            return {
                "suggested_temp": 0.0,
                "suggested_s1": 0.0,
                "suggested_s2": 0.0,
                "error": "Config not available"
            }
        
        # Initialize models if needed
        if machine_id not in _models:
            initialize_models(machine_id)
        
        # Determine if this machine uses torque or pressure
        use_torque = config.get("use_torque", True)  # Default to torque for MC25/26
        
        if use_torque:
            # Get required torque from config
            req_torque = config.get("target_torque", config.get("TORQUE_TARGET", 2.5))
            
            # Get strokes from required torque
            new_s1, new_s2 = get_required_strokes_for_torque(machine_id, req_torque)
            
            if new_s1 is None or new_s2 is None:
                error_msg = f"Could not calculate required strokes for torque in {machine_id}"
                logger.error(error_msg)
                print(f"ERROR: {error_msg}")
                return {
                    "suggested_temp": 0.0,
                    "suggested_s1": 0.0,
                    "suggested_s2": 0.0,
                    "error": "Stroke calculation failed - check torque model"
                }
        else:
            # Use pressure-based system (legacy)
            req_pressure = config.get("target_pressure", 2.1)
            
            # This would use the old pressure-based functions
            error_msg = f"Pressure model not available for {machine_id}"
            logger.error(error_msg)
            print(f"ERROR: {error_msg}")
            return {
                "suggested_temp": 0.0,
                "suggested_s1": 0.0,
                "suggested_s2": 0.0,
                "error": "Pressure model not configured"
            }

        # Calculate required temperature
        suggested_temp = calculate_bopp_surface_temp(config)
        
        if suggested_temp is None:
            error_msg = f"Could not calculate suggested temperature for {machine_id}"
            logger.error(error_msg)
            print(f"ERROR: {error_msg}")
            return {
                "suggested_temp": 0.0,
                "suggested_s1": 0.0,
                "suggested_s2": 0.0,
                "error": "Temperature calculation failed"
            }

        # Apply offsets from config
        temp_offset = config.get("temp_offset", 0)
        s1_offset = config.get("s1_offset", 0)
        s2_offset = config.get("s2_offset", 0)
        
        suggested_temp += temp_offset
        new_s1 += s1_offset
        new_s2 += s2_offset
        
        logger.info(f"Suggestion for {machine_id}: Temp:{suggested_temp:.2f} S1:{new_s1:.3f} S2:{new_s2:.3f}")
        
        return {
            "suggested_temp": suggested_temp,
            "suggested_s1": new_s1,
            "suggested_s2": new_s2
        }
        
    except Exception as e:
        logger.error(f"Error getting suggestions for {machine_id}: {e}")
        return {
            "suggested_temp": 0.0,
            "suggested_s1": 0.0,
            "suggested_s2": 0.0,
            "error": str(e)
        }

# ===== Additional utility functions =====
def get_current_config(machine_id):
    """Return current config for specific machine for debugging purposes"""
    return load_config(machine_id)

def get_model_info(machine_id):
    """Get model information for specific machine"""
    if machine_id not in _models:
        return f"Model for {machine_id} not initialized"
    
    model_data = _models[machine_id]
    info = {'machine_id': machine_id}
    
    # Check torque model
    if model_data.get('torque_initialized', False):
        df = model_data['torque_df']
        info['torque_model'] = {
            'initialized': True,
            'data_points': len(df),
            'stroke1_range': (df['stroke_1'].min(), df['stroke_1'].max()),
            'stroke2_range': (df['stroke_2'].min(), df['stroke_2'].max()),
            'torque_range': (df['avg_torque'].min(), df['avg_torque'].max())
        }
    
    # Check pressure model
    if model_data.get('pressure_initialized', False):
        df = model_data['pressure_df']
        info['pressure_model'] = {
            'initialized': True,
            'data_points': len(df),
            'stroke1_range': (df['stroke_1'].min(), df['stroke_1'].max()),
            'stroke2_range': (df['stroke_2'].min(), df['stroke_2'].max()),
            'pressure_range': (df['avg_pressure'].min(), df['avg_pressure'].max())
        }
    
    return info

def initialize_all_models():
    """Initialize models for MC25 and MC26"""
    machines = ['mc25', 'mc26']
    success_count = 0
    
    logger.info("Initializing models for MC25 and MC26...")
    for machine_id in machines:
        if initialize_models(machine_id):
            success_count += 1
            logger.info(f"✓ {machine_id} models initialized successfully")
        else:
            logger.warning(f"✗ {machine_id} model initialization incomplete")
    
    logger.info(f"Successfully initialized models for {success_count}/{len(machines)} machines")
    return success_count > 0

# ===== Module initialization =====
if __name__ == "__main__":
    # If run directly, initialize all models and show info
    initialize_all_models()
    
    # Test suggestions for both machines
    for machine_id in ['mc25', 'mc26']:
        print(f"\n{machine_id} Model Info:")
        print(get_model_info(machine_id))
        
        print(f"\n{machine_id} Suggestions:")
        suggestions = get_suggestions(machine_id)
        print(suggestions)
else:
    # If imported, try to initialize models but don't fail if some are missing
    try:
        machines = ['mc25', 'mc26']
        for machine_id in machines:
            initialize_models(machine_id)
    except Exception as e:
        logger.warning(f"Some models could not be initialized on import: {e}")
        logger.info("Models will be initialized when first suggestion is requested.")
