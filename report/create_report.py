import os
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet

# Ensure directory exists
os.makedirs("shiftwise_reports", exist_ok=True)

# Load data
state_df = pd.read_csv("state_df.csv")
reasonwise_stoppage = pd.read_csv("reasonwise_stoppage.csv")
shift_analysis = pd.read_csv("shift_analysis.csv")

# Report file path
report_path = "shiftwise_reports/current_shift_report.pdf"

# Set up PDF
doc = SimpleDocTemplate(report_path, pagesize=A4)
elements = []
styles = getSampleStyleSheet()

# Title
elements.append(Paragraph("Current Shift Report", styles['Title']))
elements.append(Spacer(1, 12))

# State DF
elements.append(Paragraph("1. State Data", styles['Heading2']))
state_table = Table([state_df.columns.to_list()] + state_df.values.tolist())
state_table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
    ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
]))
elements.append(state_table)
elements.append(Spacer(1, 12))

# Add image
elements.append(Paragraph("2. State Diagram", styles['Heading2']))
elements.append(Image("state_diagram.png", width=400, height=200))
elements.append(Spacer(1, 12))

# Shift Analysis
elements.append(Paragraph("3. Shift Analysis", styles['Heading2']))
shift_table = Table([shift_analysis.columns.to_list()] + shift_analysis.values.tolist())
shift_table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
    ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
]))
elements.append(shift_table)
elements.append(Spacer(1, 12))

# Reasonwise Stoppage
elements.append(Paragraph("4. Reasonwise Stoppage", styles['Heading2']))
reason_table = Table([reasonwise_stoppage.columns.to_list()] + reasonwise_stoppage.values.tolist())
reason_table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
    ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
]))
elements.append(reason_table)

# Build PDF
doc.build(elements)

print("Report saved at:", report_path)

