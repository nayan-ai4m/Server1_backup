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
_models = {}  # Format: {machine_id: {'model': model, 'poly': poly, 'df': df, 'initialized': bool}}
def setup_logger():
    """Setup daily rotating logger"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{today}_suggestion_module.log")

    logger = logging.getLogger("StrokePressureLogger")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if function is called multiple times
    if not logger.handlers:
        handler = TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",       # Rotate at midnight
            interval=1,            # Every day
            backupCount=7,         # Keep last 7 days
            encoding="utf-8"
        )
        handler.suffix = "%Y-%m-%d"  # Append date to rotated files
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Also log to console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


# ===== Initialize logger =====
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

# ===== Initialize ML model for specific machine =====
def initialize_model(machine_id):
    """Initialize ML model for specific machine"""
    global _models
    
    # Check if already initialized
    if machine_id in _models and _models[machine_id].get('initialized', False):
        return True
    
    try:
        # Load stroke-pressure dataset for this machine
        csv_file = f"stroke_pressure_log_{machine_id}.csv"
        
        if not os.path.exists(csv_file):
            logger.warning(f"Stroke pressure file {csv_file} not found for {machine_id}")
            return False
        
        df = pd.read_csv(csv_file)
        
        # Validate required columns
        required_columns = ['stroke_1', 'stroke_2', 'avg_pressure']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing columns {missing_columns} in {csv_file}")
            return False
        
        if df.empty:
            logger.warning(f"Empty dataset in {csv_file}")
            return False
        
        X = df[['stroke_1', 'stroke_2']].values
        y = df['avg_pressure'].values

        # Fit polynomial regression
        degree = 4
        poly = PolynomialFeatures(degree=degree)
        X_poly = poly.fit_transform(X)
        model = LinearRegression()
        model.fit(X_poly, y)
        
        # Store model data for this machine
        _models[machine_id] = {
            'model': model,
            'poly': poly,
            'df': df,
            'initialized': True
        }
        
        logger.info(f"Model initialized successfully for {machine_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing model for {machine_id}: {e}")
        return False

# ===== Force model reload for specific machine =====
def reload_model(machine_id):
    """Force model reload for specific machine"""
    global _models
    if machine_id in _models:
        _models[machine_id]['initialized'] = False
    return initialize_model(machine_id)

# ===== Predict pressure for specific machine =====
def predict_pressure(machine_id, stroke1, stroke2):
    """Predict pressure for specific machine given stroke values"""
    if not initialize_model(machine_id):
        logger.info(f"Cannot predict pressure for {machine_id}: model not initialized")
        return None
    
    try:
        model_data = _models[machine_id]
        X_in = np.array([[stroke1, stroke2]])
        X_in_poly = model_data['poly'].transform(X_in)
        return model_data['model'].predict(X_in_poly)[0]
    except Exception as e:
        logger.error(f"Error predicting pressure for {machine_id}: {e}")
        return None

# ===== Inverse: Get required strokes for pressure =====
def get_required_strokes(machine_id, required_pressure):
    """Get required strokes for specific machine to achieve target pressure"""
    if not initialize_model(machine_id):
        logger.info(f"Cannot get required strokes for {machine_id}: model not initialized")
        return None, None
    
    try:
        model_data = _models[machine_id]
        df = model_data['df']
        
        def objective(vars):
            s1, s2 = vars
            pred = predict_pressure(machine_id, s1, s2)
            if pred is None:
                return 1e6  # Large penalty for failed predictions
            return (pred - required_pressure) ** 2

        s1_min, s1_max = df['stroke_1'].min(), df['stroke_1'].max()
        s2_min, s2_max = df['stroke_2'].min(), df['stroke_2'].max()
        bounds = [(s1_min, s1_max), (s2_min, s2_max)]

        # Use dataset means as initial guess
        initial_guess = [np.mean([s1_min, s1_max]), np.mean([s2_min, s2_max])]
        
        result = minimize(objective, initial_guess, bounds=bounds)
        
        if result.success:
            return result.x[0], result.x[1]
        else:
            logger.info(f"Optimization failed for {machine_id}: {result.message}")
            return None, None
            
    except Exception as e:
        logger.error(f"Error getting required strokes for {machine_id}: {e}")
        return None, None

# ===== Calculate BOPP surface temperature (WITH THERMAL RESISTANCE) =====
def calculate_bopp_surface_temp(config):
    """Calculate BOPP surface temperature using thermal resistance from d1, d2, d3"""
    try:
        Ti = config["ambient_temp"] + 30
        T_back = config["SIT"]
        T_eff = config["T_eff"]

        # Get thermal conductivity and thickness values
        k_A = config["k_A"]
        k_B = config["k_B"] 
        k_C = config["k_C"]
        d1 = config["d1"]
        d2 = config["d2"]
        d3 = config["d3"]

        alpha_BOPP = config["alpha_BOPP"]
        alpha_metBOPP = config["alpha_metBOPP"]
        alpha_LDPE = config["alpha_LDPE"]

        L_BOPP = config["L_BOPP"]
        L_metBOPP = config["L_metBOPP"]
        L_LDPE = config["L_LDPE"]

        L_total = L_BOPP + L_metBOPP + L_LDPE
        t = config["t"]

        # Calculate thermal resistance using d1, d2, d3
        thermal_resistance = (d1 / k_A) + (d2 / k_B) + (d3 / k_C)
        
        # Base effective diffusivity
        alpha_eff = (
            alpha_BOPP * L_BOPP +
            alpha_metBOPP * L_metBOPP +
            alpha_LDPE * L_LDPE
        ) / L_total
        
        # Apply thermal resistance factor to effective diffusivity
        alpha_eff_adjusted = alpha_eff / (1 + thermal_resistance)

        # Calculate with adjusted diffusivity
        eta = L_total / (2.0 * math.sqrt(alpha_eff_adjusted * t))
        erfc_eta = math.erfc(eta)
        Ts = Ti + (T_back - Ti) / erfc_eta
        
        # Apply thermal resistance correction to final temperature
        temperature_correction_factor = 1 + (thermal_resistance * 0.1)
        
        return (Ts / T_eff) * temperature_correction_factor
        
    except Exception as e:
        logger.error(f"Error calculating BOPP surface temperature: {e}")
        return None

# ===== Main suggestion function for specific machine =====
def get_suggestions(machine_id):
    """
    Get suggestions based on current config values for specific machine.
    Reloads config every time to ensure responsiveness to changes.
    """
    try:
        # Load fresh config every time this function is called
        config = load_config(machine_id)
        if config is None:
            logger.info(f"Cannot get suggestions for {machine_id}: config not available")
            return {
                "suggested_temp": 0.0,
                "suggested_s1": 0.0,
                "suggested_s2": 0.0,
                "error": "Config not available"
            }
        
        # Get required pressure from current config
        req_pressure = config.get("target_pressure")
        if req_pressure is None:
            logger.info(f"Target pressure not found in config for {machine_id}")
            return {
                "suggested_temp": 0.0,
                "suggested_s1": 0.0,
                "suggested_s2": 0.0,
                "error": "Target pressure not configured"
            }

        # Strokes from required pressure
        new_s1, new_s2 = get_required_strokes(machine_id, req_pressure)
        
        if new_s1 is None or new_s2 is None:
            logger.info(f"Could not calculate required strokes for {machine_id}")
            return {
                "suggested_temp": 0.0,
                "suggested_s1": 0.0,
                "suggested_s2": 0.0,
                "error": "Stroke calculation failed"
            }

        # Required temperature from BOPP calculation using current config
        suggested_temp = calculate_bopp_surface_temp(config)
        
        if suggested_temp is None:
            logger.info(f"Could not calculate suggested temperature for {machine_id}")
            return {
                "suggested_temp": 0.0,
                "suggested_s1": new_s1,
                "suggested_s2": new_s2,
                "error": "Temperature calculation failed"
            }

        # Apply offsets from config (if they exist)
        temp_offset = config.get("temp_offset", 0)  # Default to 0 if not in config
        s1_offset = config.get("s1_offset", 0)      # Default to 0 if not in config
        s2_offset = config.get("s2_offset", 0)      # Default to 0 if not in config
        
        suggested_temp += temp_offset
        new_s1 += s1_offset
        new_s2 += s2_offset
        logger.info(f"Return suggestion for {machine_id} as Temp:{suggested_temp} S1:{new_s1} S2:{new_s2}")
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
    if not model_data.get('initialized', False):
        return f"Model for {machine_id} not properly initialized"
    
    df = model_data['df']
    return {
        'machine_id': machine_id,
        'initialized': True,
        'data_points': len(df),
        'stroke1_range': (df['stroke_1'].min(), df['stroke_1'].max()),
        'stroke2_range': (df['stroke_2'].min(), df['stroke_2'].max()),
        'pressure_range': (df['avg_pressure'].min(), df['avg_pressure'].max())
    }

def get_all_models_info():
    """Get information about all initialized models"""
    machines = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
    info = {}
    for machine_id in machines:
        info[machine_id] = get_model_info(machine_id)
    return info

def initialize_all_models():
    """Initialize models for all machines"""
    machines = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
    success_count = 0
    
    logger.info("Initializing models for all machines...")
    for machine_id in machines:
        if initialize_model(machine_id):
            success_count += 1
            logger.info(f"✓ {machine_id} model initialized successfully")
        else:
            logger.info(f"✗ {machine_id} model initialization failed")
    
    logger.info(f"Successfully initialized {success_count}/{len(machines)} models")
    return success_count == len(machines)

# ===== Initialize models on module import =====
if __name__ == "__main__":
    # If run directly, initialize all models and show info
    initialize_all_models()
    logger.info(f"\nModel Information:")
else:
    # If imported, try to initialize models but don't fail if some are missing
    try:
        machines = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
        for machine_id in machines:
            initialize_model(machine_id)  # Will print warnings for missing files
    except Exception as e:
        logger.error(f"Warning: Some models could not be initialized on import: {e}")
        logger.error("Models will be initialized when first suggestion is requested.")
