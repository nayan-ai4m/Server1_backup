import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import json
import os
from datetime import datetime

# Base path for JSON config files
BASE_PATH = "/home/ai4m/Desktop/configs"

# Initialize Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Custom index with JS for camera + QR scanning
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>QR Scanner Config Updater</title>
        {%favicon%}
        {%css%}
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jsQR/1.4.0/jsQR.min.js"></script>
        <style>
            body { background: #f8f9fa; }
            video { border-radius: 10px; border: 3px solid #0d6efd; }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>

        <script>
        document.addEventListener("DOMContentLoaded", function() {
            let video = document.getElementById("camera-video");
            let canvas = document.getElementById("camera-canvas");
            let context = canvas.getContext("2d");

            // Access camera
            if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
                navigator.mediaDevices.getUserMedia({ video: { facingMode: "environment" } })
                    .then(stream => {
                        video.srcObject = stream;
                        video.setAttribute("playsinline", true); // for iOS
                        video.play();
                        requestAnimationFrame(scanFrame);
                    })
                    .catch(err => {
                        console.error("Camera access error:", err);
                        alert("Camera access denied or not available!");
                    });
            } else {
                alert("Camera not supported in this browser.");
            }

            function scanFrame() {
                if (video.readyState === video.HAVE_ENOUGH_DATA) {
                    canvas.width = video.videoWidth;
                    canvas.height = video.videoHeight;
                    context.drawImage(video, 0, 0, canvas.width, canvas.height);
                    let imageData = context.getImageData(0, 0, canvas.width, canvas.height);
                    let qrCode = jsQR(imageData.data, imageData.width, imageData.height);

                    if (qrCode) {
                        let inputBox = document.getElementById("qr-data");
                        if (inputBox && inputBox.value !== qrCode.data) {
                            inputBox.value = qrCode.data;
                            inputBox.dispatchEvent(new Event("input", { bubbles: true }));
                        }
                    }
                }
                requestAnimationFrame(scanFrame);
            }
        });
        </script>
    </body>
</html>
"""

# Layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("QR Scanner Config Updater", className="text-center text-primary mb-4"))
    ]),

    dbc.Row([
        dbc.Col([
            dcc.Dropdown(
                id="machine-dropdown",
                options=[{"label": f"Machine MC{i}", "value": i} for i in range(17, 23)],
                placeholder="Select machine number (17-22)"
            ),
            html.Div(id="machine-selected", className="mt-2 text-secondary"),
        ], width=4),

        dbc.Col([
            html.Video(id="camera-video", autoplay=True, playsInline=True, muted=True,
                       style={"width": "100%", "maxWidth": "500px", "background": "#000"}),
            html.Canvas(id="camera-canvas", style={"display": "none"})
        ], width=8)
    ]),

    html.Hr(),

    dbc.Row([
        dbc.Col([
            html.H5("Scanned QR Data", className="text-secondary"),
            dcc.Textarea(id="qr-data", rows=3, style={"width": "100%"}),
            dbc.Button("Update Config", id="update-btn", color="success", className="mt-2"),
            html.Div(id="update-status", className="mt-3")
        ])
    ]),
], fluid=True, className="p-4")


# Save QR to JSON
@app.callback(
    Output("update-status", "children"),
    Input("update-btn", "n_clicks"),
    State("qr-data", "value"),
    State("machine-dropdown", "value")
)
def update_config(n_clicks, qr_value, machine_num):
    if not n_clicks:
        return ""
    if not machine_num:
        return dbc.Alert("Please select a machine", color="warning")
    if not qr_value:
        return dbc.Alert("No QR data found", color="danger")

    try:
        parts = qr_value.split(";")
        if len(parts) < 8:
            return dbc.Alert("Invalid QR format", color="danger")

        d3, d2, d1, sit = parts[4], parts[5], parts[6], parts[7]
        file_path = os.path.join(BASE_PATH, f"config_mc{machine_num}.json")

        if not os.path.exists(file_path):
            return dbc.Alert(f"Config file not found for MC{machine_num}", color="danger")

        with open(file_path, "r") as f:
            data = json.load(f)

        data["d1"] = float(d1) * 1e-6
        data["d2"] = float(d2) * 1e-6
        data["d3"] = float(d3) * 1e-6
        data["SIT"] = float(sit)

        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)

        return dbc.Alert(f"Config for MC{machine_num} updated successfully at {datetime.now()}", color="success")

    except Exception as e:
        return dbc.Alert(f"Error updating config: {str(e)}", color="danger")


# Run App
if __name__ == "__main__":
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)

    print("App running...")
    print(f"Local:   http://localhost:8050")
    print(f"Network: http://{local_ip}:8050")

    app.run_server(host="0.0.0.0", port=8050, debug=True)
