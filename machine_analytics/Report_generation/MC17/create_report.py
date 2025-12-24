import os
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                Paragraph, Spacer, Image, PageBreak)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from datetime import datetime, time, timedelta
import pytz

machine = '17'

def get_day_and_shift_ist():
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    current_time = now_ist.time()

    shift_a_start = time(12, 40)
    shift_a_end = time(15, 20)
    shift_b_start = time(22, 40)
    shift_b_end = time(23, 20)
    shift_c_start = time(6, 40)
    shift_c_end = time(7, 20)

    if shift_a_start <= current_time <= shift_a_end:
        shift = "A"
        date_for_shift = now_ist
    elif shift_b_start <= current_time <= shift_b_end:
        shift = "B"
        date_for_shift = now_ist
    elif shift_c_start <= current_time <= shift_c_end:
        shift = "C"
        date_for_shift = now_ist - timedelta(days=1)
    else:
        shift = None
        date_for_shift = now_ist

    date_str = date_for_shift.strftime("%d %B %Y")
    return date_str, shift

date, shift = get_day_and_shift_ist()
print("Date:", date)
print("Shift:", shift if shift else "No shift")

os.makedirs("shiftwise_reports", exist_ok=True)

state_df = pd.read_csv("state_df.csv")
reasonwise_stoppage = pd.read_csv("reasonwise_stoppage.csv")
shift_analysis = pd.read_csv("shift_analysis.csv")

def clean_state_df(df):
    df_clean = df.copy()
    if df_clean.shape[1] >= 2:
        df_clean.iloc[:, 0] = df_clean.iloc[:, 0].astype(str).str.replace(r'\+00:00', '', regex=True)
        df_clean.iloc[:, 1] = df_clean.iloc[:, 1].astype(str).str.replace(r'\+00:00', '', regex=True)
    for col in df_clean.columns:
        if df_clean[col].astype(str).str.contains("days").any():
            df_clean[col] = df_clean[col].astype(str).str.replace('0 days ', '', regex=False)
    return df_clean

state_df = clean_state_df(state_df)

report_path = f"shiftwise_reports/Machine_{machine}_{date}_shift_{shift}_report.pdf"

doc = SimpleDocTemplate(
    report_path,
    pagesize=A4,
    leftMargin=5*mm,
    rightMargin=5*mm,
    topMargin=5*mm,
    bottomMargin=5*mm
)

elements = []
styles = getSampleStyleSheet()

# ✨ Custom styles
centered_green_title = ParagraphStyle(
    'CenteredGreenTitle',
    parent=styles['Title'],
    alignment=1,
    fontSize=24,
    textColor=colors.green
)

centered_red_heading = ParagraphStyle(
    'CenteredRedHeading',
    parent=styles['Heading2'],
    alignment=1,
    textColor=colors.red
)

# 🟢 Add SHIFT REPORT title at the top of page 1
elements.append(Paragraph("SHIFT REPORT", centered_green_title))
elements.append(Spacer(1, 12))

# Section 1: Machine State Data
elements.append(Paragraph("1. Machine State Data", centered_red_heading))
elements.append(Spacer(1, 12))
state_table = Table([state_df.columns.to_list()] + state_df.values.tolist(), repeatRows=1)
state_table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.yellow),
    ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
]))
elements.append(state_table)
elements.append(PageBreak())

# Section 2: State Diagram
elements.append(Paragraph("2. State Diagram", centered_red_heading))
elements.append(Spacer(1, 12))
elements.append(Image("state_diagram.png", width=200*mm, height=60*mm))
elements.append(PageBreak())

# Section 3: Shift Analysis
elements.append(Paragraph("3. Shift Analysis", centered_red_heading))
elements.append(Spacer(1, 12))
shift_table = Table([shift_analysis.columns.to_list()] + shift_analysis.values.tolist(), repeatRows=1)
shift_table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.yellow),
    ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
]))
elements.append(shift_table)
elements.append(PageBreak())

# Section 4: Reasonwise Stoppage
elements.append(Paragraph("4. Reasonwise Stoppage", centered_red_heading))
elements.append(Spacer(1, 12))
reason_table = Table([reasonwise_stoppage.columns.to_list()] + reasonwise_stoppage.values.tolist(), repeatRows=1)
reason_table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.yellow),
    ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
]))
elements.append(reason_table)

# Draw border + machine info (top-right) only on first page
def draw_border(canvas, doc):
    canvas.saveState()
    width, height = A4
    margin = 0.5 * mm

    canvas.setLineWidth(1)
    canvas.setStrokeColor(colors.black)
    canvas.rect(margin, margin, width - 2 * margin, height - 2 * margin)

    canvas.setFont("Helvetica", 8)
    info_text = f"Machine: {machine}\nDate: {date}\nShift: {shift if shift else 'N/A'}"
    text_obj = canvas.beginText()
    text_obj.setTextOrigin(width - 55 * mm, height - 10 * mm)
    for line in info_text.split('\n'):
        text_obj.textLine(line)
    canvas.drawText(text_obj)
    canvas.restoreState()

# Build PDF
doc.build(elements, onFirstPage=draw_border)

print(f"✅ Report saved at: {report_path}")
