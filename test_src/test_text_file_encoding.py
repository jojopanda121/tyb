from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEXT_EXTENSIONS = {".py", ".yaml", ".yml", ".md", ".toml", ".json", ".txt", ".ini", ".cfg"}
SCAN_ROOTS = (
    PROJECT_ROOT / "src",
    PROJECT_ROOT / "test_src",
    PROJECT_ROOT / "README.md",
    PROJECT_ROOT / "AGENTS.md",
    PROJECT_ROOT / "pyproject.toml",
)
MOJIBAKE_TOKENS = (
    "\ufffd",
    "".join(chr(code_point) for code_point in (0x951F, 0x65A4, 0x62F7)),
)


def _iter_text_files() -> list[Path]:
    """
    目的：收集仓库里需要长期保持 UTF-8 的核心文本文件。
    功能：遍历源码、测试和根目录关键文档，返回稳定的扫描列表。
    实现逻辑：目录用 `rglob` 递归展开，文件则直接纳入，再统一按路径排序。
    可调参数：`SCAN_ROOTS` 和 `TEXT_EXTENSIONS`。
    默认参数及原因：默认只扫核心开发文件，原因是 pdf、缓存和临时目录不属于源码编码治理范围。
    """

    collected: list[Path] = []
    for target in SCAN_ROOTS:
        if target.is_dir():
            collected.extend(
                path
                for path in target.rglob("*")
                if path.is_file() and path.suffix.lower() in TEXT_EXTENSIONS
            )
            continue
        if target.is_file():
            collected.append(target)
    return sorted(set(collected))


def test_project_text_files_are_utf8_without_bom_or_mojibake() -> None:
    """
    目的：防止源码和 YAML 再次出现中文乱码或 BOM。
    功能：检查核心文本文件是否是无 BOM 的 UTF-8，并拦截常见乱码残片。
    实现逻辑：逐个读原始字节，先验 BOM，再严格按 UTF-8 解码，最后检查典型乱码标记。
    可调参数：`TEXT_EXTENSIONS`、`SCAN_ROOTS` 和 `MOJIBAKE_TOKENS`。
    默认参数及原因：默认使用严格 UTF-8 解码，原因是仓库规范已经明确统一使用 UTF-8。
    """

    failures: list[str] = []
    for path in _iter_text_files():
        raw = path.read_bytes()
        relative_path = path.relative_to(PROJECT_ROOT).as_posix()
        if raw.startswith(b"\xef\xbb\xbf"):
            failures.append(f"{relative_path}: contains UTF-8 BOM")
            continue

        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            failures.append(f"{relative_path}: invalid UTF-8 ({exc})")
            continue

        bad_tokens = [token for token in MOJIBAKE_TOKENS if token in text]
        if bad_tokens:
            failures.append(f"{relative_path}: suspicious mojibake tokens {bad_tokens}")

    assert not failures, "Text encoding issues found:\n" + "\n".join(failures)
