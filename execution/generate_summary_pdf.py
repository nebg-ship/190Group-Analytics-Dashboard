import re
import os
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch

def parse_markdown_table(table_str):
    """Parses a markdown table string into a list of lists."""
    rows = table_str.strip().split('\n')
    parsed_rows = []
    for row in rows:
        if set(row.strip()) <= {'|', '-', ':', ' '}:
            continue  # Skip separator row
        cols = [c.strip().replace('**', '') for c in row.split('|') if c.strip() or row.count('|') > 1]
        if cols:
            parsed_rows.append(cols)
    return parsed_rows

def generate_pdf(md_path, pdf_path):
    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    doc = SimpleDocTemplate(pdf_path, pagesize=letter, leftMargin=0.5*inch, rightMargin=0.5*inch, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    
    # Custom Styles
    styles.add(ParagraphStyle(name='AlertBox', parent=styles['Normal'], spaceBefore=10, spaceAfter=10, leftIndent=20, rightIndent=20, backColor=colors.whitesmoke, borderPadding=10, borderVisible=True))
    
    story = []

    # Simple Regex-based Markdown Parsing
    # Title
    title_match = re.search(r'^# (.+)$', content, re.MULTILINE)
    if title_match:
        story.append(Paragraph(title_match.group(1), styles['Title']))
        story.append(Spacer(1, 12))

    # Split into sections
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
                    # Convert content to Paragraphs for wrapping
                    cell_style = ParagraphStyle(name='TableCell', parent=styles['Normal'], fontSize=8, leading=10, alignment=1) # Center aligned
                    wrapped_data = []
                    for row_idx, r in enumerate(table_data):
                        wrapped_row = []
                        for c in r:
                            # Headers get bold style
                            if row_idx == 0:
                                p_style = ParagraphStyle(name='TableHeader', parent=cell_style, fontName='Helvetica-Bold', textColor=colors.whitesmoke)
                                wrapped_row.append(Paragraph(f"<b>{c}</b>", p_style))
                            else:
                                wrapped_row.append(Paragraph(c, cell_style))
                        wrapped_data.append(wrapped_row)

                    col_count = len(table_data[0])
                    # Give the first column (usually SKU/Category) more relative width if it's a long string
                    total_width = letter[0] - inch
                    if col_count > 3:
                        # Distribution: First column gets 30%, others split remaining 70%
                        first_col = total_width * 0.35
                        others = (total_width * 0.65) / (col_count - 1)
                        col_widths = [first_col] + [others] * (col_count - 1)
                    else:
                        col_widths = [total_width / col_count] * col_count
                    
                    t = Table(wrapped_data, colWidths=col_widths)
                    t.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('LEFTPADDING', (0, 0), (-1, -1), 3),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                        ('TOPPADDING', (0, 0), (-1, -1), 3),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                    ]))
                    story.append(t)
                    story.append(Spacer(1, 10))
                continue

            # Alert boxes (> [!TIP], etc)
            elif line.startswith('> [!'):
                alert_type = re.search(r'\[!(.+)\]', line).group(1)
                alert_content = []
                while i < len(lines) and lines[i].strip().startswith('>'):
                    cleaned_line = lines[i].strip().replace('>', '').strip()
                    if not cleaned_line.startswith('[!'):
                        alert_content.append(cleaned_line)
                    i += 1
                
                joined_content = f"<b>{alert_type}:</b> " + ' '.join(alert_content)
                story.append(Paragraph(joined_content, styles['AlertBox']))
                story.append(Spacer(1, 10))
                continue

            # Standard Bold Text Key Takeaways
            elif line.startswith('**Key Takeaway:**'):
                 story.append(Paragraph(f"<b>Key Takeaway:</b> {line.replace('**Key Takeaway:**', '').strip()}", styles['Normal']))
                 story.append(Spacer(1, 10))
            
            # Normal text
            elif not line.startswith('#'):
                # Handle basic bolding in text
                line = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', line)
                story.append(Paragraph(line, styles['Normal']))
                story.append(Spacer(1, 6))
            
            i += 1

    doc.build(story)
    print(f"Successfully generated {pdf_path}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    # Correct path to the artifact walkthrough.md
    artifact_dir = os.path.join(os.environ['USERPROFILE'], '.gemini', 'antigravity', 'brain', 'b346c27b-3480-4b79-8d90-6d0cb8f7bc06')
    md_file = os.path.join(artifact_dir, 'walkthrough.md')
    pdf_file = os.path.join(project_root, 'January_Performance_Summary.pdf')
    
    generate_pdf(md_file, pdf_file)
