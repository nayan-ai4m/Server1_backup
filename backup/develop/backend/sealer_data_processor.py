def process_vertical_sealer_data(res_vertical_sealer_data):
    if not res_vertical_sealer_data:
        return None

    data = {
        "ver_sealer_pressure": {
            "value": res_vertical_sealer_data[0] if res_vertical_sealer_data[0] is not None else 0,
            "color": "#FF0000" if res_vertical_sealer_data[0] and res_vertical_sealer_data[0] > 200 else "#FFFFFF",
        }
    }

    for i in range(1, 14):
        temp_key = f"ver_sealer_front_{i}_temp"
        data[temp_key] = {
            "value": res_vertical_sealer_data[i] if res_vertical_sealer_data[i] is not None else 0,
            "color": "#FF0000" if res_vertical_sealer_data[i] and res_vertical_sealer_data[i] > 200 else "#FFFFFF",
        }

    for i in range(1, 14):
        temp_key = f"ver_sealer_rear_{i}_temp"
        data[temp_key] = {
            "value": res_vertical_sealer_data[i + 13] if res_vertical_sealer_data[i + 13] is not None else 0,
            "color": "#FF0000" if res_vertical_sealer_data[i + 13] and res_vertical_sealer_data[i + 13] > 200 else "#FFFFFF",
        }

    return data

