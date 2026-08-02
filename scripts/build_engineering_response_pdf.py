"""Build the recruiter-facing Engineering response PDF from Markdown.

The Markdown remains the editable source of truth. This builder intentionally uses
only local fonts and deterministic ReportLab layout so the PDF can be regenerated
without Word or LibreOffice.
"""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

INK = colors.HexColor("#172B4D")
BLUE = colors.HexColor("#2E74B5")
DARK_BLUE = colors.HexColor("#1F4D78")
MUTED = colors.HexColor("#5F6B7A")
LIGHT_GRAY = colors.HexColor("#F2F4F7")
BLUE_GRAY = colors.HexColor("#E8EEF5")
CALLOUT = colors.HexColor("#F4F6F9")
BORDER = colors.HexColor("#D5DAE2")
WHITE = colors.white
PAGE_WIDTH, PAGE_HEIGHT = letter
CONTENT_WIDTH = 6.5 * inch


def register_fonts() -> tuple[str, str, str, str]:
    font_dir = Path(r"C:\Windows\Fonts")
    candidates = {
        "Calibri": font_dir / "calibri.ttf",
        "Calibri-Bold": font_dir / "calibrib.ttf",
        "Calibri-Italic": font_dir / "calibrii.ttf",
        "Calibri-BoldItalic": font_dir / "calibriz.ttf",
    }
    if all(path.exists() for path in candidates.values()):
        for name, path in candidates.items():
            pdfmetrics.registerFont(TTFont(name, str(path)))
        pdfmetrics.registerFontFamily(
            "Calibri",
            normal="Calibri",
            bold="Calibri-Bold",
            italic="Calibri-Italic",
            boldItalic="Calibri-BoldItalic",
        )
        return "Calibri", "Calibri-Bold", "Calibri-Italic", "Calibri-BoldItalic"
    return "Helvetica", "Helvetica-Bold", "Helvetica-Oblique", "Helvetica-BoldOblique"


BASE_FONT, BOLD_FONT, ITALIC_FONT, BOLD_ITALIC_FONT = register_fonts()


def build_styles() -> dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    return {
        "body": ParagraphStyle(
            "Body",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=10.7,
            leading=12.8,
            textColor=colors.HexColor("#20262E"),
            spaceBefore=0,
            spaceAfter=6,
            allowWidows=0,
            allowOrphans=0,
        ),
        "body_keep": ParagraphStyle(
            "BodyKeepWithNext",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=10.7,
            leading=12.8,
            textColor=colors.HexColor("#20262E"),
            spaceBefore=0,
            spaceAfter=6,
            allowWidows=0,
            allowOrphans=0,
            keepWithNext=True,
        ),
        "h1": ParagraphStyle(
            "H1",
            parent=sample["Heading1"],
            fontName=BOLD_FONT,
            fontSize=16,
            leading=19,
            textColor=BLUE,
            spaceBefore=16,
            spaceAfter=8,
            keepWithNext=True,
        ),
        "h2": ParagraphStyle(
            "H2",
            parent=sample["Heading2"],
            fontName=BOLD_FONT,
            fontSize=13,
            leading=15.5,
            textColor=BLUE,
            spaceBefore=12,
            spaceAfter=6,
            keepWithNext=True,
        ),
        "h3": ParagraphStyle(
            "H3",
            parent=sample["Heading3"],
            fontName=BOLD_FONT,
            fontSize=12,
            leading=14,
            textColor=DARK_BLUE,
            spaceBefore=8,
            spaceAfter=4,
            keepWithNext=True,
        ),
        "bullet": ParagraphStyle(
            "Bullet",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=10.5,
            leading=12.5,
            leftIndent=18,
            firstLineIndent=-10,
            spaceAfter=5,
            textColor=colors.HexColor("#20262E"),
        ),
        "number": ParagraphStyle(
            "Number",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=10.5,
            leading=12.5,
            leftIndent=20,
            firstLineIndent=-18,
            spaceAfter=6,
            textColor=colors.HexColor("#20262E"),
        ),
        "callout": ParagraphStyle(
            "Callout",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=10.2,
            leading=12.5,
            textColor=INK,
            spaceAfter=0,
        ),
        "diagram": ParagraphStyle(
            "Diagram",
            parent=sample["Code"],
            fontName="Courier",
            fontSize=8.4,
            leading=11.2,
            textColor=INK,
            leftIndent=0,
            rightIndent=0,
            spaceAfter=0,
        ),
        "table_header": ParagraphStyle(
            "TableHeader",
            parent=sample["BodyText"],
            fontName=BOLD_FONT,
            fontSize=8.8,
            leading=10.5,
            textColor=INK,
            spaceAfter=0,
        ),
        "table_body": ParagraphStyle(
            "TableBody",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=8.6,
            leading=10.4,
            textColor=colors.HexColor("#20262E"),
            spaceAfter=0,
        ),
        "cover_kicker": ParagraphStyle(
            "CoverKicker",
            parent=sample["BodyText"],
            fontName=BOLD_FONT,
            fontSize=10,
            leading=12,
            tracking=1.2,
            textColor=BLUE,
            alignment=TA_LEFT,
            spaceAfter=8,
        ),
        "cover_title": ParagraphStyle(
            "CoverTitle",
            parent=sample["Title"],
            fontName=BOLD_FONT,
            fontSize=29,
            leading=33,
            textColor=INK,
            alignment=TA_LEFT,
            spaceBefore=0,
            spaceAfter=8,
        ),
        "cover_subtitle": ParagraphStyle(
            "CoverSubtitle",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=15,
            leading=18,
            textColor=MUTED,
            alignment=TA_LEFT,
            spaceAfter=24,
        ),
        "cover_meta_label": ParagraphStyle(
            "CoverMetaLabel",
            parent=sample["BodyText"],
            fontName=BOLD_FONT,
            fontSize=10,
            leading=12,
            textColor=INK,
        ),
        "cover_meta_value": ParagraphStyle(
            "CoverMetaValue",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=10,
            leading=12,
            textColor=colors.HexColor("#20262E"),
        ),
        "cover_note": ParagraphStyle(
            "CoverNote",
            parent=sample["BodyText"],
            fontName=BASE_FONT,
            fontSize=9.8,
            leading=12,
            textColor=INK,
            alignment=TA_LEFT,
        ),
    }


STYLES = build_styles()


def inline_markup(value: str) -> str:
    escaped = html.escape(value, quote=False)
    escaped = re.sub(
        r"`([^`]+)`",
        rf'<font name="{BOLD_FONT}" color="#1F4D78">\1</font>',
        escaped,
    )
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", escaped)
    return escaped


def parse_frontmatter(lines: list[str]) -> tuple[dict[str, str], list[str]]:
    metadata: dict[str, str] = {}
    if not lines or lines[0].strip() != "---":
        return metadata, lines
    end = next((idx for idx in range(1, len(lines)) if lines[idx].strip() == "---"), None)
    if end is None:
        return metadata, lines
    for line in lines[1:end]:
        if ":" in line:
            key, value = line.split(":", 1)
            metadata[key.strip()] = value.strip()
    return metadata, lines[end + 1 :]


def make_callout(text_lines: list[str]) -> Table:
    text = "<br/>".join(inline_markup(line.strip()) for line in text_lines if line.strip())
    table = Table([[Paragraph(text, STYLES["callout"])]], colWidths=[CONTENT_WIDTH])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), CALLOUT),
                ("BOX", (0, 0), (-1, -1), 0.7, BORDER),
                ("LINEBEFORE", (0, 0), (0, -1), 3, BLUE),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    table.spaceBefore = 5
    table.spaceAfter = 10
    return table


def make_diagram(text_lines: list[str]) -> Table:
    text = "\n".join(line.rstrip() for line in text_lines)
    diagram = Preformatted(text, STYLES["diagram"])
    table = Table([[diagram]], colWidths=[CONTENT_WIDTH])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), BLUE_GRAY),
                ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#B8C5D6")),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    table.spaceBefore = 4
    table.spaceAfter = 10
    return table


def table_widths(headers: list[str]) -> list[float]:
    lowered = [header.lower() for header in headers]
    if len(headers) == 2:
        return [1.55 * inch, 4.95 * inch]
    if len(headers) == 3 and "credential" in lowered:
        return [1.35 * inch, 1.55 * inch, 3.60 * inch]
    if len(headers) == 3 and "alternative" in lowered:
        return [1.75 * inch, 1.75 * inch, 3.00 * inch]
    if len(headers) == 3:
        return [1.50 * inch, 2.20 * inch, 2.80 * inch]
    return [CONTENT_WIDTH / len(headers)] * len(headers)


def make_markdown_table(raw_rows: list[list[str]]) -> Table:
    headers = raw_rows[0]
    widths = table_widths(headers)
    rows: list[list[Paragraph]] = []
    rows.append([Paragraph(inline_markup(cell), STYLES["table_header"]) for cell in headers])
    for raw in raw_rows[1:]:
        rows.append([Paragraph(inline_markup(cell), STYLES["table_body"]) for cell in raw])
    table = Table(rows, colWidths=widths, repeatRows=1, hAlign="LEFT")
    commands = [
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_GRAY),
        ("TEXTCOLOR", (0, 0), (-1, 0), INK),
        ("GRID", (0, 0), (-1, -1), 0.45, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]
    for row_idx in range(1, len(rows)):
        if row_idx % 2 == 0:
            commands.append(("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#FAFBFC")))
    table.setStyle(TableStyle(commands))
    table.spaceBefore = 4
    table.spaceAfter = 10
    return table


def parse_table(lines: list[str], start: int) -> tuple[Table, int]:
    rows: list[list[str]] = []
    idx = start
    while idx < len(lines) and lines[idx].strip().startswith("|"):
        cells = [cell.strip() for cell in lines[idx].strip().strip("|").split("|")]
        rows.append(cells)
        idx += 1
    if len(rows) > 1 and all(re.fullmatch(r":?-{3,}:?", cell) for cell in rows[1]):
        rows.pop(1)
    return make_markdown_table(rows), idx


def make_cover(metadata: dict[str, str]) -> list[object]:
    title = metadata.get("title", "Sushant's Answers to the Take-Home Questionnaire")
    subtitle = metadata.get("subtitle", "GreyOrange - Abacus Engineering Exercise")
    candidate = metadata.get("candidate", "Sushant")
    date = metadata.get("date", "August 1, 2026")
    status = metadata.get("status", "ENGINEERING EXERCISE ONLY")
    release_note = metadata.get(
        "release_note",
        "This document is generated from the tested Engineering response source.",
    )

    story: list[object] = [Spacer(1, 0.55 * inch)]
    story.append(Paragraph("ENGINEERING TAKE-HOME RESPONSE", STYLES["cover_kicker"]))
    story.append(Paragraph(inline_markup(title), STYLES["cover_title"]))
    story.append(Paragraph(inline_markup(subtitle), STYLES["cover_subtitle"]))
    story.append(HRFlowable(width="100%", thickness=1.3, color=BLUE, spaceBefore=0, spaceAfter=18))

    meta_rows = [
        [
            Paragraph("Candidate", STYLES["cover_meta_label"]),
            Paragraph(candidate, STYLES["cover_meta_value"]),
        ],
        [
            Paragraph("Submission date", STYLES["cover_meta_label"]),
            Paragraph(date, STYLES["cover_meta_value"]),
        ],
        [
            Paragraph("Scope", STYLES["cover_meta_label"]),
            Paragraph(
                "Engineering Exercise only; Product Manager Exercise excluded",
                STYLES["cover_meta_value"],
            ),
        ],
        [
            Paragraph("Status", STYLES["cover_meta_label"]),
            Paragraph(status, STYLES["cover_meta_value"]),
        ],
    ]
    meta = Table(meta_rows, colWidths=[1.25 * inch, 5.25 * inch], hAlign="LEFT")
    meta.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LINEBELOW", (0, 0), (-1, -2), 0.35, BORDER),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    story.extend([meta, Spacer(1, 0.48 * inch)])

    note = Table(
        [
            [
                Paragraph(
                    inline_markup(release_note),
                    STYLES["cover_note"],
                )
            ]
        ],
        colWidths=[CONTENT_WIDTH],
    )
    note.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), CALLOUT),
                ("BOX", (0, 0), (-1, -1), 0.7, BORDER),
                ("LINEBEFORE", (0, 0), (0, 0), 3, BLUE),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    story.extend([note, Spacer(1, 0.32 * inch)])

    coverage = [
        "1. Brand and Store Onboarding",
        "2. Product Master Ingestion",
        "3. Real-Time RFID Inventory Feed",
        "4. Identity, Access, and User Management",
        "5. Replenishment Policy Ingestion and Execution",
    ]
    story.append(Paragraph("Response coverage", STYLES["h2"]))
    for line in coverage:
        story.append(Paragraph(inline_markup(line), STYLES["number"]))
    story.append(PageBreak())
    return story


def parse_markdown(path: Path) -> tuple[dict[str, str], list[object]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    metadata, lines = parse_frontmatter(lines)
    story = make_cover(metadata)
    idx = 0
    paragraph_buffer: list[str] = []

    def flush_paragraph(*, keep_with_next: bool = False) -> None:
        if paragraph_buffer:
            text = " ".join(part.strip() for part in paragraph_buffer)
            style = STYLES["body_keep"] if keep_with_next else STYLES["body"]
            story.append(Paragraph(inline_markup(text), style))
            paragraph_buffer.clear()

    while idx < len(lines):
        raw = lines[idx]
        stripped = raw.strip()

        if not stripped:
            flush_paragraph()
            idx += 1
            continue

        if stripped == ":::pagebreak":
            flush_paragraph()
            story.append(PageBreak())
            idx += 1
            continue

        if stripped in {":::callout", ":::diagram"}:
            kind = stripped[3:]
            flush_paragraph(keep_with_next=kind == "diagram")
            idx += 1
            block: list[str] = []
            while idx < len(lines) and lines[idx].strip() != ":::":
                block.append(lines[idx])
                idx += 1
            idx += 1
            story.append(make_callout(block) if kind == "callout" else make_diagram(block))
            continue

        if stripped == "---":
            flush_paragraph()
            story.append(
                HRFlowable(width="100%", thickness=0.6, color=BORDER, spaceBefore=8, spaceAfter=8)
            )
            idx += 1
            continue

        if stripped.startswith("# "):
            flush_paragraph()
            story.append(Paragraph(inline_markup(stripped[2:]), STYLES["h1"]))
            idx += 1
            continue
        if stripped.startswith("## "):
            flush_paragraph()
            story.append(Paragraph(inline_markup(stripped[3:]), STYLES["h2"]))
            idx += 1
            continue
        if stripped.startswith("### "):
            flush_paragraph()
            story.append(Paragraph(inline_markup(stripped[4:]), STYLES["h3"]))
            idx += 1
            continue

        if (
            stripped.startswith("|")
            and idx + 1 < len(lines)
            and lines[idx + 1].strip().startswith("|")
        ):
            flush_paragraph()
            table, idx = parse_table(lines, idx)
            story.append(table)
            continue

        if stripped.startswith("- "):
            flush_paragraph()
            story.append(
                Paragraph(f"&#8226;&nbsp;&nbsp;{inline_markup(stripped[2:])}", STYLES["bullet"])
            )
            idx += 1
            continue

        number_match = re.match(r"^(\d+)\.\s+(.*)$", stripped)
        if number_match:
            flush_paragraph()
            number, text = number_match.groups()
            story.append(Paragraph(f"{number}.&nbsp;&nbsp;{inline_markup(text)}", STYLES["number"]))
            idx += 1
            continue

        paragraph_buffer.append(stripped)
        idx += 1

    flush_paragraph()
    return metadata, story


def draw_page(canvas, doc) -> None:  # type: ignore[no-untyped-def]
    canvas.saveState()
    canvas.setTitle("Sushant's Answers to the Take-Home Questionnaire")
    canvas.setAuthor("Sushant")
    canvas.setSubject("GreyOrange - Abacus Engineering Exercise")
    page = canvas.getPageNumber()
    if page == 1:
        canvas.setFont(BASE_FONT, 8.5)
        canvas.setFillColor(MUTED)
        status = getattr(doc, "submission_status", "Engineering Exercise only")
        canvas.drawCentredString(PAGE_WIDTH / 2, 0.42 * inch, status.title())
    else:
        canvas.setStrokeColor(BORDER)
        canvas.setLineWidth(0.5)
        canvas.line(inch, PAGE_HEIGHT - 0.55 * inch, PAGE_WIDTH - inch, PAGE_HEIGHT - 0.55 * inch)
        canvas.setFont(BASE_FONT, 8.3)
        canvas.setFillColor(MUTED)
        canvas.drawString(
            inch, PAGE_HEIGHT - 0.43 * inch, "Sushant | GreyOrange - Abacus Engineering Exercise"
        )
        canvas.drawString(inch, 0.42 * inch, "Engineering Exercise only")
        canvas.drawRightString(PAGE_WIDTH - inch, 0.42 * inch, f"Page {page}")
    canvas.restoreState()


class BrandedDocTemplate(SimpleDocTemplate):
    """Draw page furniture after flowables so content cannot paint over it."""

    submission_status: str

    def afterPage(self) -> None:
        draw_page(self.canv, self)


def build(source: Path, output: Path) -> None:
    metadata, story = parse_markdown(source)
    output.parent.mkdir(parents=True, exist_ok=True)
    doc = BrandedDocTemplate(
        str(output),
        pagesize=letter,
        leftMargin=inch,
        rightMargin=inch,
        topMargin=0.82 * inch,
        bottomMargin=0.82 * inch,
        title=metadata.get("title", "Sushant's Answers to the Take-Home Questionnaire"),
        author=metadata.get("candidate", "Sushant"),
        subject=metadata.get("subtitle", "GreyOrange - Abacus Engineering Exercise"),
    )
    doc.submission_status = metadata.get("status", "Engineering Exercise only")
    doc.build(story)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    build(args.source.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
