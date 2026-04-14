from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 设计目的：集中放置 v0.2 Flow 运行时共用的目录、日志和时间辅助函数。
# 模块功能：统一管理 run 目录、日志落盘、路径标准化和调试清单输出。
# 实现逻辑：只保留最小但稳定的公共 helper，避免把简单规则分散到多个模块。
# 可调参数：`DEFAULT_PDF_PATH`、`CACHE_ROOT`、`CREWAI_MEMORY_DIR` 和各类文件名常量。
# 默认参数及原因：单次 run 产物默认都落到 `.cache/<run_slug>/`，原因是中间产物和日志需要按轮次集中查看。

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CACHE_ROOT = PROJECT_ROOT / ".cache"
DEFAULT_PDF_PATH = PROJECT_ROOT / "pdf" / "博瑞迪商业计划书v1.0.pdf"
CREWAI_MEMORY_DIR = PROJECT_ROOT / "crewai_memory"
RUN_ARTIFACT_DIR_NAME = "md"
RUN_LOG_DIR_NAME = "logs"
RUN_PREPROCESS_LOG_FILE_NAME = "preprocess.txt"
RUN_FLOW_LOG_FILE_NAME = "flow.txt"
RUN_CONSOLE_LOG_FILE_NAME = "console.txt"
RUN_DEBUG_MANIFEST_FILE_NAME = "run_manifest.json"

# 设计目的：统一项目内使用的北京时间时区对象。
# 模块功能：给目录命名、日志时间戳和调试清单输出提供固定时区。
# 实现逻辑：显式构造 `Asia/Shanghai` 固定偏移时区，避免依赖宿主机本地设置。
# 可调参数：当前无。
# 默认参数及原因：默认固定东八区，原因是当前项目主要按北京时间排查运行过程。
BEIJING_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")

CREW_LOG_NAMES = (
    "history_background_crew",
    "industry_crew",
    "business_crew",
    "peer_info_crew",
    "financial_crew",
    "operating_metrics_crew",
    "risk_crew",
    "valuation_crew",
    "investment_thesis_crew",
    "writeup_crew",
)
_ACTIVE_PREPROCESS_LOG_PATH: Path | None = None


def ensure_directory(path: Path) -> Path:
    """
    目的：统一创建目录，减少各处重复写 `mkdir`。
    功能：确保目标目录存在，并把同一个 `Path` 返回给调用方继续使用。
    实现逻辑：固定使用 `parents=True` 和 `exist_ok=True` 创建整条目录链。
    可调参数：`path`。
    默认参数及原因：默认自动补齐父目录，原因是 run 目录和日志目录通常需要一次性建好。
    """

    path.mkdir(parents=True, exist_ok=True)
    return path


def reset_runtime_logging_state() -> None:
    """
    目的：在新一轮运行开始前清空当前进程持有的日志上下文。
    功能：重置预处理日志的活动路径，避免上一轮 run 的日志路径串到下一轮。
    实现逻辑：把模块级 `_ACTIVE_PREPROCESS_LOG_PATH` 直接置空。
    可调参数：无。
    默认参数及原因：默认只清理内存里的上下文，不删除历史日志文件，原因是历史日志仍然要保留。
    """

    global _ACTIVE_PREPROCESS_LOG_PATH

    _ACTIVE_PREPROCESS_LOG_PATH = None


# 设计目的：在模块导入阶段就准备好 CrewAI 本地运行目录和必要环境变量。
# 模块功能：创建缓存目录、memory 目录，并关闭不需要的遥测相关开关。
# 实现逻辑：先确保目录存在，再用 `setdefault` 只在外部未配置时补默认值。
# 可调参数：相关环境变量都可以由外部环境覆盖。
# 默认参数及原因：统一使用 `setdefault`，原因是要优先尊重用户已有环境配置。
ensure_directory(CREWAI_MEMORY_DIR)
ensure_directory(CACHE_ROOT)
os.environ.setdefault("CREWAI_STORAGE_DIR", CREWAI_MEMORY_DIR.as_posix())
os.environ.setdefault("OTEL_SDK_DISABLED", "true")
os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")
os.environ.setdefault("CREWAI_DISABLE_TRACKING", "true")
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "true")


def utc_timestamp() -> str:
    """
    目的：统一项目内部使用的时间戳格式。
    功能：返回带时区信息、精确到秒的 ISO 时间戳。
    实现逻辑：基于 `BEIJING_TIMEZONE` 生成当前时间，再序列化成 ISO 字符串。
    可调参数：无。
    默认参数及原因：默认使用北京时间且去掉微秒，原因是日志对比和人工排查更直观。
    """

    return datetime.now(BEIJING_TIMEZONE).isoformat(timespec="seconds")


def build_run_slug(company_name: str) -> str:
    """
    目的：为单次运行生成稳定、可读、可排序的目录名。
    功能：把公司名清洗成 slug，再拼上北京时间时间戳。
    实现逻辑：先去空白和非法路径字符，再补上秒级时间戳。
    可调参数：`company_name`。
    默认参数及原因：空公司名时回退到 `unknown-company`，原因是 run 目录名不能为空。
    """

    slug = re.sub(r"\s+", "-", company_name.strip())
    slug = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "-", slug)
    slug = re.sub(r"-{2,}", "-", slug).strip(" .-")
    slug = slug or "unknown-company"
    timestamp = datetime.now(BEIJING_TIMEZONE).strftime("%Y%m%d_%H%M%S")
    return f"{timestamp}_{slug}"


def build_run_directories(company_name: str) -> dict[str, Path]:
    """
    目的：一次性准备单次 run 需要的目录集合。
    功能：生成 run slug，并返回根目录、产物目录和日志目录映射。
    实现逻辑：先创建 `.cache/<run_slug>/`，再固定拆出 `md/` 和 `logs/`。
    可调参数：`company_name`。
    默认参数及原因：目录结构固定为 `run_root_dir / md / logs`，原因是中间产物和日志需要分开放置。
    """

    run_slug = build_run_slug(company_name)
    run_root_dir = ensure_directory(CACHE_ROOT / run_slug)
    cache_dir = ensure_directory(run_root_dir / RUN_ARTIFACT_DIR_NAME)
    log_dir = ensure_directory(run_root_dir / RUN_LOG_DIR_NAME)
    return {
        "run_slug": Path(run_slug),
        "run_root_dir": run_root_dir,
        "cache_dir": cache_dir,
        "log_dir": log_dir,
    }


def normalize_path(path: Path | str) -> str:
    """
    目的：统一路径字符串的输出格式。
    功能：把任意路径转换成绝对路径，并统一输出为 POSIX 风格字符串。
    实现逻辑：展开用户目录、解析绝对路径，再调用 `as_posix()`。
    可调参数：`path`。
    默认参数及原因：默认输出 POSIX 风格，原因是 JSON、日志和跨平台文本里更稳定。
    """

    return Path(path).expanduser().resolve().as_posix()


def read_text_if_exists(path: Path | str) -> str:
    """
    目的：让 Flow 读取中间产物时不必重复写存在性判断。
    功能：文件存在则返回 UTF-8 文本；不存在或是目录则返回空串。
    实现逻辑：先判断空值和文件存在性，再按 UTF-8 读取。
    可调参数：`path`。
    默认参数及原因：缺文件时返回空串而不是抛错，原因是部分阶段产物在早期路由里可能还没生成。
    """

    if not path:
        return ""
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists() or resolved.is_dir():
        return ""
    return resolved.read_text(encoding="utf-8")


def append_text_log_line(path: Path | str, message: str) -> str:
    """
    目的：给 Flow 内部文本日志提供统一落盘入口。
    功能：按行追加 UTF-8 日志，并自动补上时间戳。
    实现逻辑：先确保父目录存在，再把格式化后的单行文本写入目标文件。
    可调参数：`path` 和 `message`。
    默认参数及原因：固定一行一条日志，原因是 grep 和人工定位都更直接。
    """

    resolved = Path(path).expanduser().resolve()
    ensure_directory(resolved.parent)
    line = f"{utc_timestamp()} {message}\n"
    with resolved.open("a", encoding="utf-8") as handle:
        handle.write(line)
    return normalize_path(resolved)


def append_text_log_chunk(path: Path | str, text: str) -> str:
    """
    目的：给终端 transcript 一类需要保留原始换行和格式的日志提供统一落盘入口。
    功能：按原样追加 UTF-8 文本块，不额外注入时间戳或包装格式。
    实现逻辑：先确保父目录存在，再把传入文本块直接写入目标文件。
    可调参数：`path` 和 `text`。
    默认参数及原因：默认保留原始文本，原因是 PowerShell 终端输出本身已经包含第三方库和异常栈的真实格式。
    """

    if not text:
        return normalize_path(path)
    resolved = Path(path).expanduser().resolve()
    ensure_directory(resolved.parent)
    with resolved.open("a", encoding="utf-8") as handle:
        handle.write(text)
    return normalize_path(resolved)


def run_artifact_dir(run_slug: str) -> Path:
    """
    目的：统一取得单次 run 的中间产物目录。
    功能：返回 `.cache/<run_slug>/md/` 对应目录，并确保目录存在。
    实现逻辑：基于 `CACHE_ROOT` 和 `RUN_ARTIFACT_DIR_NAME` 拼出固定路径。
    可调参数：`run_slug`。
    默认参数及原因：目录名固定为 `md`，原因是这里同时存 Markdown 和相关 JSON 中间产物。
    """

    return ensure_directory(CACHE_ROOT / run_slug / RUN_ARTIFACT_DIR_NAME)


def run_log_dir(run_slug: str) -> Path:
    """
    目的：统一取得单次 run 的日志目录。
    功能：返回 `.cache/<run_slug>/logs/` 对应目录，并确保目录存在。
    实现逻辑：基于 `CACHE_ROOT` 和 `RUN_LOG_DIR_NAME` 拼出固定路径。
    可调参数：`run_slug`。
    默认参数及原因：日志默认按 run 隔离，原因是单轮运行的排查应该集中完成。
    """

    return ensure_directory(CACHE_ROOT / run_slug / RUN_LOG_DIR_NAME)


def activate_run_preprocess_log(run_slug: str) -> str:
    """
    目的：把预处理日志绑定到当前 run。
    功能：记录当前活动的 `preprocess.txt` 路径，并返回标准化后的字符串路径。
    实现逻辑：先拿到 run 日志目录，再把固定文件名写入模块级上下文。
    可调参数：`run_slug`。
    默认参数及原因：默认按 run 维度单独建预处理日志，原因是页索引和元数据识别都属于单轮中间过程。
    """

    global _ACTIVE_PREPROCESS_LOG_PATH

    _ACTIVE_PREPROCESS_LOG_PATH = run_log_dir(run_slug) / RUN_PREPROCESS_LOG_FILE_NAME
    return normalize_path(_ACTIVE_PREPROCESS_LOG_PATH)


def append_preprocess_log_line(message: str) -> str:
    """
    目的：给 PDF 预处理阶段提供固定文本日志出口。
    功能：把日志追加到当前 run 已绑定的 `preprocess.txt`。
    实现逻辑：如果当前还没有激活日志路径则直接返回空串；否则转调统一写日志函数。
    可调参数：`message`。
    默认参数及原因：未绑定 run 时不报错，原因是预处理上下文可能晚于模块导入阶段才建立。
    """

    if _ACTIVE_PREPROCESS_LOG_PATH is None:
        return ""
    return append_text_log_line(_ACTIVE_PREPROCESS_LOG_PATH, message)


def run_flow_log_path(run_slug: str) -> str:
    """
    目的：给 Flow 主过程生成固定日志文件路径。
    功能：返回 `.cache/<run_slug>/logs/flow.txt` 的标准化字符串路径。
    实现逻辑：先拿到 run 级日志目录，再拼固定文件名。
    可调参数：`run_slug`。
    默认参数及原因：文件名固定为 `flow.txt`，原因是目录已经按 run 隔离，文件名应尽量直接。
    """

    return normalize_path(run_log_dir(run_slug) / RUN_FLOW_LOG_FILE_NAME)


def run_console_log_path(run_slug: str) -> str:
    """
    目的：给单次 run 的终端 transcript 生成固定日志路径。
    功能：返回 `.cache/<run_slug>/logs/console.txt` 的标准化字符串路径。
    实现逻辑：先拿到 run 日志目录，再拼接固定文件名 `console.txt`。
    可调参数：`run_slug`。
    默认参数及原因：文件名固定为 `console.txt`，原因是它表达的是整次 PowerShell 可见输出的原始转储。
    """

    return normalize_path(run_log_dir(run_slug) / RUN_CONSOLE_LOG_FILE_NAME)


def run_crew_log_path(run_slug: str, crew_name: str) -> str:
    """
    目的：给单个 crew 生成固定日志路径。
    功能：返回 `.cache/<run_slug>/logs/<crew_name>.txt` 的标准化字符串路径。
    实现逻辑：在 run 日志目录下直接按 crew 名组装文件名。
    可调参数：`run_slug` 和 `crew_name`。
    默认参数及原因：默认每个 crew 单独一份文件，原因是阶段排查比混在一份大日志里更清楚。
    """

    return normalize_path(run_log_dir(run_slug) / f"{crew_name}.txt")


def write_run_debug_manifest(
    *,
    run_slug: str,
    status: str,
    pdf_file_path: str,
    run_cache_dir: str,
    evidence_registry_path: str = "",
    registry_snapshot_markdown_path: str = "",
    page_index_file_path: str = "",
    document_metadata_file_path: str = "",
    final_report_markdown_path: str = "",
    final_report_pdf_path: str = "",
    failed_stage: str = "",
    failed_crew: str = "",
    error_message: str = "",
    blocked_packs: list[str] | None = None,
    block_reason: str = "",
) -> str:
    """
    目的：给每次 run 生成一份可追踪的调试索引文件。
    功能：把 PDF、缓存目录、日志目录、registry 和最终报告路径统一落盘到 `run_manifest.json`。
    实现逻辑：先组装 manifest 字典，再写入当前 run 的产物目录。
    可调参数：运行状态、输入 PDF 路径、缓存目录和各类可选产物路径。
    默认参数及原因：缺失路径默认写空串，原因是同一份 manifest 既要兼容运行中，也要兼容运行完成后。
    """

    artifact_dir = run_artifact_dir(run_slug)
    log_dir = run_log_dir(run_slug)
    flow_log_file_path = normalize_path(log_dir / RUN_FLOW_LOG_FILE_NAME)
    preprocess_log_file_path = normalize_path(log_dir / RUN_PREPROCESS_LOG_FILE_NAME)
    console_log_file_path = normalize_path(log_dir / RUN_CONSOLE_LOG_FILE_NAME)
    manifest_path = normalize_path(artifact_dir / RUN_DEBUG_MANIFEST_FILE_NAME)
    crew_log_paths = {crew_name: normalize_path(log_dir / f"{crew_name}.txt") for crew_name in CREW_LOG_NAMES}
    manifest = {
        "run_slug": run_slug,
        "status": status,
        "generated_at": utc_timestamp(),
        "pdf_file_path": normalize_path(pdf_file_path),
        "run_root_dir": normalize_path(CACHE_ROOT / run_slug),
        "run_cache_dir": normalize_path(run_cache_dir),
        "run_artifact_dir": normalize_path(artifact_dir),
        "run_log_dir": normalize_path(log_dir),
        "evidence_registry_path": normalize_path(evidence_registry_path) if evidence_registry_path else "",
        "registry_snapshot_markdown_path": (
            normalize_path(registry_snapshot_markdown_path) if registry_snapshot_markdown_path else ""
        ),
        "page_index_file_path": normalize_path(page_index_file_path) if page_index_file_path else "",
        "document_metadata_file_path": (
            normalize_path(document_metadata_file_path) if document_metadata_file_path else ""
        ),
        "final_report_markdown_path": (
            normalize_path(final_report_markdown_path) if final_report_markdown_path else ""
        ),
        "final_report_pdf_path": normalize_path(final_report_pdf_path) if final_report_pdf_path else "",
        "failed_stage": failed_stage,
        "failed_crew": failed_crew,
        "error_message": error_message,
        "blocked_packs": blocked_packs or [],
        "block_reason": block_reason,
        "run_debug_manifest_path": manifest_path,
        "preprocess_log_file_path": preprocess_log_file_path,
        "run_log_file_path": flow_log_file_path,
        "flow_log_file_path": flow_log_file_path,
        "console_log_file_path": console_log_file_path,
        "crew_log_paths": crew_log_paths,
    }

    serialized = json.dumps(manifest, ensure_ascii=False, indent=2)
    Path(manifest_path).write_text(serialized, encoding="utf-8")
    return manifest_path
