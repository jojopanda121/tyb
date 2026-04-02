from __future__ import annotations

import re
from pathlib import Path
from typing import Type

from bs4 import BeautifulSoup
from markdown import Markdown
from pydantic import BaseModel, Field
from weasyprint import HTML

from crewai.tools import BaseTool


"""Markdown 转 PDF 工具"""


class MarkdownToPdfInput(BaseModel):  # 设计：定义转 PDF 入参；功能：约束输入输出路径与排版开关；默认标题为 Research Report，原因：兼容通用报告场景。
    markdown_path: str = Field(..., description="Path to the source markdown file")
    pdf_path: str = Field(..., description="Path to the output PDF file")
    title: str = Field(default="Research Report", description="Document title shown in the PDF metadata")
    auto_landscape: bool = Field(
        default=True,
        description="Automatically switch to landscape layout when wide tables are detected",
    )
    force_landscape: bool = Field(
        default=False,
        description="Force all pages to use landscape layout",
    )


class MarkdownToPdfTool(BaseTool):  # 设计：统一报告导出工具；功能：把 Markdown 渲染成可读 PDF；默认自动横向宽表，原因：财务表常较宽。
    name: str = "markdown_to_pdf"
    description: str = (
        "Convert a markdown file into a polished PDF, optimized for financial reports "
        "with readable tables, repeated headers, wrapped notes, and wide-table support."
    )
    args_schema: Type[BaseModel] = MarkdownToPdfInput

    def _run(  # 设计：执行导出；功能：把 Markdown 渲染成 PDF；可调：title、auto_landscape、force_landscape；默认自动横向且不强制，原因：优先兼顾可读性与版面稳定。
        self,
        markdown_path: str,
        pdf_path: str,
        title: str = "Research Report",
        auto_landscape: bool = True,
        force_landscape: bool = False,
    ) -> str:
        md_file = Path(markdown_path).expanduser().resolve()
        out_file = Path(pdf_path).expanduser().resolve()

        if not md_file.exists():
            raise FileNotFoundError(f"Markdown file not found: {md_file}")

        out_file.parent.mkdir(parents=True, exist_ok=True)
        md_text = md_file.read_text(encoding="utf-8")
        md_text = self._normalize_markdown_tables(md_text)

        html_body = self._markdown_to_html(md_text)
        soup = BeautifulSoup(html_body, "html.parser")

        self._decorate_tables(soup)

        landscape = force_landscape or (
            auto_landscape and self._needs_landscape(soup)
        )

        final_html = self._build_full_html(
            body_html=str(soup),
            title=title,
            landscape=landscape,
        )

        HTML(
            string=final_html,
            base_url=str(md_file.parent),
        ).write_pdf(str(out_file))

        return f"PDF created successfully at: {out_file}"

    def _markdown_to_html(self, md_text: str) -> str:  # 设计：统一转 HTML；功能：开启常用扩展渲染 Markdown；默认启用表格等扩展，原因：报告表格较多。
        md = Markdown(
            extensions=[
                "extra",
                "tables",
                "toc",
                "sane_lists",
            ]
        )
        return md.convert(md_text)

    def _normalize_markdown_tables(self, md_text: str) -> str:
        """表格预处理"""
        lines = md_text.splitlines()
        normalized: list[str] = []
        idx = 0

        while idx < len(lines):
            if self._is_table_header(lines, idx):
                if normalized and normalized[-1].strip():
                    normalized.append("")

                while idx < len(lines) and self._is_table_row(lines[idx]):
                    normalized.append(lines[idx].rstrip())
                    idx += 1

                if idx < len(lines) and lines[idx].strip():
                    normalized.append("")
                continue

            normalized.append(lines[idx].rstrip())
            idx += 1

        normalized_text = "\n".join(normalized)
        if md_text.endswith("\n"):
            normalized_text += "\n"
        return normalized_text

    def _is_table_header(self, lines: list[str], idx: int) -> bool:
        if idx + 1 >= len(lines):
            return False
        return self._is_table_row(lines[idx]) and self._is_table_separator(lines[idx + 1])

    def _is_table_row(self, line: str) -> bool:
        stripped = line.strip()
        return stripped.startswith("|") and stripped.endswith("|")

    def _is_table_separator(self, line: str) -> bool:
        stripped = line.strip()
        return bool(
            re.fullmatch(r"\|\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|", stripped)
        )

    def _decorate_tables(self, soup: BeautifulSoup) -> None:  # 设计：表格增强；功能：按列数和字段特征打样式类；默认偏向财务表优化，原因：报告以财务表为主。
        for table in soup.find_all("table"):
            headers = []
            first_row = table.find("tr")
            if first_row:
                headers = [cell.get_text(" ", strip=True) for cell in first_row.find_all(["th", "td"])]

            num_cols = len(headers)
            header_text = " | ".join(headers).lower()

            has_year_columns = any(re.fullmatch(r"(19|20)\d{2}", h.strip()) for h in headers)
            has_finance_keywords = any(
                kw in header_text
                for kw in [
                    "metric",
                    "revenue",
                    "ebitda",
                    "margin",
                    "cash flow",
                    "assets",
                    "liabilities",
                    "source",
                    "formula",
                    "roe",
                    "fcf",
                    "capex",
                    "ratio",
                    "收入",
                    "利润",
                    "现金流",
                    "资产",
                    "负债",
                    "附注",
                    "指标",
                    "年份",
                ]
            )

            classes = table.get("class", [])
            classes.append("report-table")

            if has_year_columns or has_finance_keywords:
                classes.append("financial-table")

            if num_cols >= 6:
                classes.append("wide-table")

            table["class"] = classes

            thead = table.find("thead")
            if thead is None:
                first_tr = table.find("tr")
                if first_tr:
                    new_thead = soup.new_tag("thead")
                    first_tr.extract()
                    new_thead.append(first_tr)
                    table.insert(0, new_thead)

            tbody = table.find("tbody")
            if tbody is None:
                rows = table.find_all("tr")
                if rows:
                    body_rows = rows[1:] if table.find("thead") else rows
                    new_tbody = soup.new_tag("tbody")
                    for row in body_rows:
                        row.extract()
                        new_tbody.append(row)
                    table.append(new_tbody)

            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                for idx, cell in enumerate(cells):
                    text = cell.get_text(" ", strip=True)
                    if idx > 0 and self._looks_numeric(text):
                        existing = cell.get("class", [])
                        existing.append("num")
                        cell["class"] = existing

    def _looks_numeric(self, value: str) -> bool:  # 设计：数值识别；功能：判断单元格是否应右对齐；默认用宽松正则，原因：兼容常见财务写法。
        v = value.replace(",", "").replace(" ", "")
        patterns = [
            r"^\(?-?\d+(\.\d+)?\)?%?$",
            r"^\(?-?\d+(\.\d+)?\)?x$",
            r"^\(?-?\d+(\.\d+)?\)?$",
        ]
        return any(re.fullmatch(p, v, flags=re.IGNORECASE) for p in patterns)

    def _needs_landscape(self, soup: BeautifulSoup) -> bool:  # 设计：横向判定；功能：按表格宽度决定是否横版；默认启发式规则，原因：实现简单且足够实用。
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if not rows:
                continue

            first_row = rows[0]
            num_cols = len(first_row.find_all(["th", "td"]))
            max_cell_text = max(
                (
                    len(cell.get_text(" ", strip=True))
                    for row in rows[:6]
                    for cell in row.find_all(["th", "td"])
                ),
                default=0,
            )

            if num_cols >= 7:
                return True
            if num_cols >= 6 and max_cell_text >= 28:
                return True
            if "wide-table" in table.get("class", []):
                return True

        return False

    def _build_full_html(self, body_html: str, title: str, landscape: bool) -> str:  # 设计：组装最终 HTML；功能：注入页面模板和 CSS；默认 A4，原因：报告导出通用性最好。
        page_size = "A4 landscape" if landscape else "A4"

        css = f"""
        @page {{
            size: {page_size};
            margin: 15mm 12mm 15mm 12mm;

            @top-center {{
                content: \"{self._css_escape(title)}\";
                font-size: 9pt;
                color: #666;
            }}

            @bottom-right {{
                content: \"Page \" counter(page);
                font-size: 9pt;
                color: #666;
            }}
        }}

        html {{
            font-size: 11px;
        }}

        body {{
            font-family: Arial, \"Noto Sans CJK SC\", \"Noto Sans SC\", \"Microsoft YaHei\", sans-serif;
            line-height: 1.55;
            color: #111;
            word-break: break-word;
            overflow-wrap: anywhere;
        }}

        h1, h2, h3, h4, h5, h6 {{
            page-break-after: avoid;
            margin-top: 1.1em;
            margin-bottom: 0.45em;
            line-height: 1.25;
        }}

        h1 {{
            font-size: 20px;
            border-bottom: 2px solid #222;
            padding-bottom: 6px;
        }}

        h2 {{
            font-size: 16px;
            border-bottom: 1px solid #999;
            padding-bottom: 4px;
        }}

        h3 {{
            font-size: 13px;
        }}

        p, ul, ol {{
            margin-top: 0.45em;
            margin-bottom: 0.55em;
        }}

        li {{
            margin-bottom: 0.25em;
        }}

        code {{
            font-family: \"Courier New\", monospace;
            font-size: 0.92em;
            background: #f5f5f5;
            padding: 1px 3px;
            border-radius: 3px;
        }}

        pre {{
            background: #f6f6f6;
            border: 1px solid #ddd;
            padding: 10px;
            overflow-x: auto;
            white-space: pre-wrap;
        }}

        blockquote {{
            margin: 0.8em 0;
            padding-left: 10px;
            border-left: 3px solid #bbb;
            color: #444;
        }}

        .report-table {{
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
            margin: 10px 0 16px 0;
            font-size: 9.5px;
            line-height: 1.35;
        }}

        .report-table caption {{
            caption-side: top;
            text-align: left;
            font-weight: 700;
            margin-bottom: 6px;
        }}

        .report-table thead {{
            display: table-header-group;
        }}

        .report-table tfoot {{
            display: table-footer-group;
        }}

        .report-table tr {{
            page-break-inside: avoid;
            break-inside: avoid;
        }}

        .report-table th,
        .report-table td {{
            border: 1px solid #999;
            padding: 5px 6px;
            vertical-align: top;
            overflow-wrap: anywhere;
            word-wrap: break-word;
            background-clip: padding-box;
        }}

        .report-table th {{
            font-weight: 700;
            text-align: center;
            background: #efefef;
        }}

        .report-table tbody tr:nth-child(even) td {{
            background: #fafafa;
        }}

        .report-table td.num {{
            text-align: right;
            white-space: nowrap;
            font-variant-numeric: tabular-nums;
        }}

        .financial-table {{
            font-size: 9px;
        }}

        .financial-table th:first-child,
        .financial-table td:first-child {{
            width: 18%;
            font-weight: 700;
        }}

        .financial-table th:nth-child(2),
        .financial-table td:nth-child(2),
        .financial-table th:nth-child(3),
        .financial-table td:nth-child(3),
        .financial-table th:nth-child(4),
        .financial-table td:nth-child(4),
        .financial-table th:nth-child(5),
        .financial-table td:nth-child(5) {{
            width: 10%;
        }}

        .financial-table th:last-child,
        .financial-table td:last-child {{
            width: 22%;
        }}

        .financial-table th:nth-last-child(2),
        .financial-table td:nth-last-child(2) {{
            width: 20%;
        }}

        .wide-table {{
            font-size: 8.4px;
        }}

        .wide-table th,
        .wide-table td {{
            padding: 4px 5px;
        }}

        hr {{
            border: none;
            border-top: 1px solid #ccc;
            margin: 16px 0;
        }}
        """

        return f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{self._html_escape(title)}</title>
  <style>{css}</style>
</head>
<body>
{body_html}
</body>
</html>
"""

    def _html_escape(self, text: str) -> str:  # 设计：HTML 转义；功能：保护标题插入 HTML；默认只转关键字符，原因：已够覆盖当前场景。
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    def _css_escape(self, text: str) -> str:  # 设计：CSS 转义；功能：保护标题插入 content；默认只转必要字符，原因：保持实现简单。
        return text.replace("\\", "\\\\").replace('"', '\\"')
