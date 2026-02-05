import re
import os
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch

def parse_markdown_table(table_str):
    """Parses a markdown table string into a list of lists."""
    lines = table_str.strip().split('\n')
    parsed_rows = []
    for line in lines:
        if set(line.strip()) <= {'|', '-', ':', ' '}:
            continue  # Skip separator row
        cols = [c.strip().replace('**', '') for c in line.split('|') if c.strip() or line.count('|') > 1]
        if cols:
            parsed_rows.append(cols)
    return parsed_rows

def generate_pdf(md_path, pdf_path):
    if not os.path.exists(md_path):
        print(f"Error: Markdown file not found at {md_path}")
        return

    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    doc = SimpleDocTemplate(pdf_path, pagesize=letter, leftMargin=0.5*inch, rightMargin=0.5*inch, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    
    # Custom Styles
    alert_style = ParagraphStyle(
        name='AlertBox', 
        parent=styles['Normal'], 
        spaceBefore=10, 
        spaceAfter=10, 
        leftIndent=20, 
        rightIndent=20, 
        backColor=colors.whitesmoke, 
        borderPadding=10, 
        borderVisible=True,
        borderColor=colors.lightgrey
    )
    styles.add(alert_style)
    
    story = []

    # Title
    title_match = re.search(r'^# (.+)$', content, re.MULTILINE)
    if title_match:
        story.append(Paragraph(title_match.group(1), styles['Title']))
        story.append(Spacer(1, 12))

    # Split into sections by horizontal rules
    sections = re.split(r'\n---+\n', content)
    
    for section in sections:
        lines = section.strip().split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if not line:
                i += 1
                continue
            
            # Headers
            if line.startswith('## '):
                story.append(Spacer(1, 12))
                story.append(Paragraph(line[3:], styles['Heading2']))
                story.append(Spacer(1, 6))
            elif line.startswith('### '):
                story.append(Spacer(1, 10))
                story.append(Paragraph(line[4:], styles['Heading3']))
                story.append(Spacer(1, 4))
            
            # Tables
            elif line.startswith('|'):
                table_lines = []
                while i < len(lines) and lines[i].strip().startswith('|'):
                    table_lines.append(lines[i])
                    i += 1
                table_data = parse_markdown_table('\n'.join(table_lines))
                if table_data:
                    cell_style = ParagraphStyle(name='TableCell', parent=styles['Normal'], fontSize=8, leading=10, alignment=1) # Center aligned
                    wrapped_data = []
                    for row_idx, r in enumerate(table_data):
                        wrapped_row = []
                        for c in r:
                            # Handle basic bolding in table cells
                            c_clean = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', c)
                            if row_idx == 0:
                                p_style = ParagraphStyle(name='TableHeader', parent=cell_style, fontName='Helvetica-Bold', textColor=colors.whitesmoke)
                                wrapped_row.append(Paragraph(f"<b>{c_clean}</b>", p_style))
                            else:
                                wrapped_row.append(Paragraph(c_clean, cell_style))
                        wrapped_data.append(wrapped_row)

                    col_count = len(table_data[0])
                    total_width = letter[0] - inch
                    
                    # Logic for column widths based on the TWEB report structure
                    if col_count == 2:
                        col_widths = [total_width * 0.6, total_width * 0.4]
                    elif col_count >= 5:
                        col_widths = [total_width * 0.25] + [(total_width * 0.75) / (col_count - 1)] * (col_count - 1)
                    else:
                        col_widths = [total_width / col_count] * col_count
                    
                    t = Table(wrapped_data, colWidths=col_widths)
                    t.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('LEFTPADDING', (0, 0), (-1, -1), 4),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                        ('TOPPADDING', (0, 0), (-1, -1), 4),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                    ]))
                    story.append(t)
                    story.append(Spacer(1, 10))
                continue

            # Alert boxes (> [!TIP], etc)
            elif line.startswith('> [!'):
                alert_match = re.search(r'\[!(.+)\]', line)
                alert_type = alert_match.group(1) if alert_match else "INFO"
                alert_content = []
                while i < len(lines) and lines[i].strip().startswith('>'):
                    cleaned_line = lines[i].strip().replace('>', '').strip()
                    if not cleaned_line.startswith('[!'):
                        alert_content.append(cleaned_line)
                    i += 1
                
                joined_content = f"<b>{alert_type}:</b> " + ' '.join(alert_content)
                # Handle bold in alert content
                joined_content = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', joined_content)
                story.append(Paragraph(joined_content, styles['AlertBox']))
                story.append(Spacer(1, 10))
                continue
            
            # Normal text
            elif not line.startswith('#'):
                # Handle basic bolding in text
                line = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', line)
                # Handle markdown links [text](url) -> text
                line = re.sub(r'\[(.+?)\]\(.+?\)', r'\1', line)
                story.append(Paragraph(line, styles['Normal']))
                story.append(Spacer(1, 6))
            
            i += 1

    doc.build(story)
    print(f"Successfully generated {pdf_path}")

if __name__ == "__main__":
    # Absolute paths for reliability
    md_file = r"C:\Users\admin\.gemini\antigravity\brain\1e9da628-3e52-49c4-9b88-02b4307bd99f\walkthrough.md"
    project_root = r"f:\Vibe Code Projects\190Group Analytics Dashboard"
    pdf_file = os.path.join(project_root, "TWEB_Sales_Analysis_Feb_June_2025.pdf")
    
    generate_pdf(md_file, pdf_file)
