import pandas as pd
import numpy as np
import json
import math
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from scipy.optimize import minimize

# ===== Load config (moved to function for dynamic loading) =====
def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

# ===== Initialize model variables (will be loaded once) =====
_model = None
_poly = None
_df = None
_model_initialized = False

# ===== Initialize ML model (loads once, but can be reloaded if needed) =====
def initialize_model():
    global _model, _poly, _df, _model_initialized
    
    if _model_initialized:
        return
    
    # Load stroke-pressure dataset
    _df = pd.read_csv("stroke_pressure_log.csv")
    X = _df[['stroke_1', 'stroke_2']].values
    y = _df['avg_pressure'].values

    # Fit polynomial regression
    degree = 5
    _poly = PolynomialFeatures(degree=degree)
    X_poly = _poly.fit_transform(X)
    _model = LinearRegression()
    _model.fit(X_poly, y)
    
    _model_initialized = True

# ===== Force model reload (if CSV data changes) =====
def reload_model():
    global _model_initialized
    _model_initialized = False
    initialize_model()

# ===== Predict pressure =====
def predict_pressure(stroke1, stroke2):
    initialize_model()  # Ensure model is loaded
    X_in = np.array([[stroke1, stroke2]])
    X_in_poly = _poly.transform(X_in)
    return _model.predict(X_in_poly)[0]

# ===== Inverse: Get required strokes for pressure =====
def get_required_strokes(required_pressure):
    initialize_model()  # Ensure model is loaded
    
    def objective(vars):
        s1, s2 = vars
        pred = predict_pressure(s1, s2)
        return (pred - required_pressure) ** 2

    s1_min, s1_max = _df['stroke_1'].min(), _df['stroke_1'].max()
    s2_min, s2_max = _df['stroke_2'].min(), _df['stroke_2'].max()
    bounds = [(s1_min, s1_max), (s2_min, s2_max)]

    initial_guess = [np.mean([s1_min, s1_max]), np.mean([s2_min, s2_max])]
    result = minimize(objective, initial_guess, bounds=bounds)
    return result.x

# ===== Calculate BOPP surface temperature =====
def calculate_bopp_surface_temp(config):
    """Calculate BOPP surface temperature using current config values"""
    Ti = config["ambient_temp"] + 30
    T_back = config["SIT"]
    T_eff = config["T_eff"]

    # Get thermal conductivity values (same as in monitoring code)
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
    t = config["t"]  # heating time from config

    # Calculate thermal resistance (same formula as in heat energy calculation)
    thermal_resistance = (d1 / k_A) + (d2 / k_B) + (d3 / k_C)
    
    # Modify alpha_eff based on thermal resistance
    alpha_eff = (
        alpha_BOPP * L_BOPP +
        alpha_metBOPP * L_metBOPP +
        alpha_LDPE * L_LDPE
    ) / L_total
    
    # Apply thermal resistance factor to effective diffusivity
    alpha_eff_adjusted = alpha_eff / (1 + thermal_resistance)

    eta = L_total / (2.0 * math.sqrt(alpha_eff_adjusted * t))
    erfc_eta = math.erfc(eta)
    Ts = Ti + (T_back - Ti) / erfc_eta
    
    # Apply thermal resistance correction to final temperature
    temperature_correction_factor = 1 + (thermal_resistance * 0.1)  # Adjustable factor
    
    return (Ts / T_eff) * temperature_correction_factor

# ===== Main suggestion function (now fully responsive to config changes) =====
def get_suggestions():
    """
    Get suggestions based on current config values.
    Reloads config every time to ensure responsiveness to changes.
    """
    # Load fresh config every time this function is called
    config = load_config()
    
    # Get required pressure from current config
    req_pressure = config["target_pressure"]

    # Strokes from required pressure
    new_s1, new_s2 = get_required_strokes(req_pressure)

    # Required temperature from BOPP calculation using current config
    suggested_temp = calculate_bopp_surface_temp(config)

    # Apply offsets from config (if they exist)
    temp_offset = config.get("temp_offset", 0)  # Default to 0 if not in config
    s1_offset = config.get("s1_offset", 0)      # Default to 0 if not in config
    s2_offset = config.get("s2_offset", 0)      # Default to 0 if not in config
    
    suggested_temp += temp_offset
    new_s1 += s1_offset
    new_s2 += s2_offset

    return {
        "suggested_temp": suggested_temp,
        "suggested_s1": new_s1,
        "suggested_s2": new_s2
    }

# ===== Additional utility functions =====
def get_current_config():
    """Return current config for debugging purposes"""
    return load_config()

def validate_config():
    """Validate that all required config parameters are present"""
    config = load_config()
    required_params = [
        "ambient_temp", "SIT", "T_eff", "alpha_BOPP", "alpha_metBOPP", 
        "alpha_LDPE", "L_BOPP", "L_metBOPP", "L_LDPE", "t", "target_pressure"
    ]
    
    missing_params = []
    for param in required_params:
        if param not in config:
            missing_params.append(param)
    
    if missing_params:
        raise ValueError(f"Missing required config parameters: {missing_params}")
    
    return True

# ===== Initialize model on module import =====
try:
    initialize_model()
except Exception as e:
    print(f"Warning: Could not initialize model on import: {e}")
    print("Model will be initialized when first suggestion is requested.")