from __future__ import annotations

import re
from pathlib import Path
from typing import Type

from bs4 import BeautifulSoup
from markdown import Markdown
from pydantic import BaseModel, Field
from weasyprint import HTML

from crewai.tools import BaseTool

# 设计目的：把最终 Markdown 报告稳定地转换成适合投资研究阅读的 PDF，尤其处理表格与分页问题。
# 模块功能：读取 Markdown、修正表格结构、判断横向排版，并生成最终 PDF。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：标题、`auto_landscape` 和 `force_landscape`。
# 默认参数及原因：默认自动横向排版但不强制，原因是宽表和普通正文需要兼顾。


class MarkdownToPdfInput(BaseModel):
    """
    设计目的：定义 Markdown 转 PDF 工具的输入格式。
    模块功能：约束输入输出路径、标题和横向排版选项。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：所有字段都可由调用方覆盖。
    默认参数及原因：`auto_landscape` 默认开启，原因是研究报告里的表格经常偏宽。
    """

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


class MarkdownToPdfTool(BaseTool):
    """
    设计目的：把 Markdown 转 PDF 的复杂细节收口成一个稳定工具。
    模块功能：处理表格、样式和横向排版，再调用 WeasyPrint 输出 PDF。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：输入输出路径、标题和横向排版开关。
    默认参数及原因：默认尽量自动判断横向，原因是能同时兼顾正文页和宽表页。
    """

    name: str = "markdown_to_pdf"
    description: str = (
        "Convert a markdown file into a polished PDF, optimized for financial reports "
        "with readable tables, repeated headers, wrapped notes, and wide-table support."
    )
    args_schema: Type[BaseModel] = MarkdownToPdfInput

    def _run(
        self,
        markdown_path: str,
        pdf_path: str,
        title: str = "Research Report",
        auto_landscape: bool = True,
        force_landscape: bool = False,
    ) -> str:
        """
        设计目的：为 writeup crew 提供最终文件导出能力。
        模块功能：读取 Markdown、修正表格、生成 HTML，并调用 WeasyPrint 输出 PDF。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：输入输出路径、标题和横向排版开关。
        默认参数及原因：优先自动判断是否横向，原因是这样既兼顾正文页，也兼顾宽表页。
        """

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

    def _markdown_to_html(self, md_text: str) -> str:
        """
        设计目的：把 Markdown 文本统一转成 HTML。
        模块功能：使用固定扩展把 Markdown 渲染成后续可加工的 HTML。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`md_text`。
        默认参数及原因：默认启用 `tables` 等扩展，原因是研究报告常包含表格和目录结构。
        """

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
        """
        设计目的：在转 HTML 前先把 Markdown 表格整理成更稳定的块结构。
        模块功能：补齐表格前后的空行，减少渲染时表格断裂。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`md_text`。
        默认参数及原因：默认只调整表格附近空行，原因是尽量少碰正文内容。
        """

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
        """
        设计目的：识别某一行是否是 Markdown 表格头。
        模块功能：判断当前行与下一行是否构成“表头 + 分隔线”组合。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`lines` 和 `idx`。
        默认参数及原因：要求下一行是分隔线，原因是这样最稳，不会误判普通列表。
        """

        if idx + 1 >= len(lines):
            return False
        return self._is_table_row(lines[idx]) and self._is_table_separator(lines[idx + 1])

    def _is_table_row(self, line: str) -> bool:
        """
        设计目的：识别普通 Markdown 表格行。
        模块功能：判断一行文本是否以 `|` 开头并以 `|` 结尾。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`line`。
        默认参数及原因：默认只识别标准表格语法，原因是当前报告模板也采用标准写法。
        """

        stripped = line.strip()
        return stripped.startswith("|") and stripped.endswith("|")

    def _is_table_separator(self, line: str) -> bool:
        """
        设计目的：识别 Markdown 表格的分隔线。
        模块功能：用正则判断一行是否符合 `|---|---|` 这类格式。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`line`。
        默认参数及原因：默认兼容左右对齐冒号，原因是常见 Markdown 表格会带对齐标记。
        """

        stripped = line.strip()
        return bool(
            re.fullmatch(r"\|\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|", stripped)
        )

    def _decorate_tables(self, soup: BeautifulSoup) -> None:
        """
        设计目的：给 HTML 表格补齐样式类和结构，提升 PDF 可读性。
        模块功能：识别财务表、宽表，补 `thead`、`tbody`，并给数字列加样式。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`soup`。
        默认参数及原因：默认按列数和关键词判断宽表与财务表，原因是规则简单且对当前报告足够稳。
        """

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

    def _looks_numeric(self, value: str) -> bool:
        """
        设计目的：区分数字单元格和文本单元格。
        模块功能：判断字符串是否像数字、百分比或倍数。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`value`。
        默认参数及原因：默认兼容括号负数和百分号，原因是财务表里经常出现这两类格式。
        """

        v = value.replace(",", "").replace(" ", "")
        patterns = [
            r"^\(?-?\d+(\.\d+)?\)?%?$",
            r"^\(?-?\d+(\.\d+)?\)?x$",
            r"^\(?-?\d+(\.\d+)?\)?$",
        ]
        return any(re.fullmatch(p, v, flags=re.IGNORECASE) for p in patterns)

    def _needs_landscape(self, soup: BeautifulSoup) -> bool:
        """
        设计目的：自动判断报告是否需要横向页面。
        模块功能：根据列数、单元格长度和宽表标记判断是否切换横向。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`soup`。
        默认参数及原因：默认看到宽表就切横向，原因是横向排版比挤压表格更可读。
        """

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

    def _build_full_html(self, body_html: str, title: str, landscape: bool) -> str:
        """
        设计目的：把正文 HTML 包装成完整的可打印页面。
        模块功能：拼接页面尺寸、CSS 样式、头尾信息和正文。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`body_html`、`title` 和 `landscape`。
        默认参数及原因：默认使用 A4，只有在 `landscape=True` 时切横向，原因是正文页仍以竖版更易读。
        """

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

    def _html_escape(self, text: str) -> str:
        """
        设计目的：避免标题文本破坏 HTML 结构。
        模块功能：转义常见 HTML 特殊字符。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`text`。
        默认参数及原因：只转义最常见字符，原因是标题场景不需要更复杂的 HTML 处理。
        """

        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    def _css_escape(self, text: str) -> str:
        """
        设计目的：避免标题文本破坏 CSS 字符串。
        模块功能：转义反斜杠和双引号。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`text`。
        默认参数及原因：只处理 CSS 字符串里最常见的两个风险字符，原因是当前标题用法很简单。
        """

        return text.replace("\\", "\\\\").replace('"', '\\"')
