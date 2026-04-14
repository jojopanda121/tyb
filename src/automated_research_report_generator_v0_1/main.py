import subprocess
import sys
from datetime import datetime
from pathlib import Path
import re
import shutil
import yaml


"""运行入口与预处理"""


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_CACHE_DIR = PROJECT_ROOT / ".cache"
PROJECT_LOG_DIR = PROJECT_ROOT / "logs"
TASKS_CONFIG_PATH = (
    PROJECT_ROOT
    / "src"
    / "automated_research_report_generator_v0_1"
    / "config"
    / "tasks.yaml"
)
DEFAULT_PDF_FILE_PATH = r"pdf/sehk26022601053_c.pdf"
CACHE_ENTRIES_TO_KEEP = {".gitignore", ".lock", "CACHEDIR.TAG", "interpreter-v4", "sdists-v9"}
CACHE_ENTRY_PREFIXES_TO_KEEP = (".tmp",)


def reset_crewai_memories() -> None:  # 设计：运行前清理记忆；功能：减少历史结果串入本轮；默认每次 run 前执行，原因是当前流程更重视隔离性。
    try:
        subprocess.run(
            ["crewai", "reset-memories", "--all"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        print(f"[WARN] crewai reset-memories failed: {exc}")


def reset_project_cache_dir() -> None:  # 设计：运行前清理项目缓存；功能：强制预处理产物重建；默认保留少量 uv 条目，原因是避免环境缓存被误删。
    try:
        PROJECT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        for cache_entry in PROJECT_CACHE_DIR.iterdir():
            if (  # 默认保留白名单缓存；功能：避免误删解释器与下载缓存。
                cache_entry.name in CACHE_ENTRIES_TO_KEEP
                or cache_entry.name.startswith(CACHE_ENTRY_PREFIXES_TO_KEEP)
            ):
                continue

            if cache_entry.is_dir():
                shutil.rmtree(cache_entry)
            else:
                cache_entry.unlink()
    except Exception as exc:
        print(f"[WARN] project cache reset failed: {exc}")


def prepare_inputs(pdf_file_path: str | None = None) -> dict[str, str]:  # 设计：统一准备 kickoff 输入；功能：串联元数据识别与页索引；可调参数是 PDF 路径；默认使用内置 demo PDF，原因是兼容旧入口。
    from automated_research_report_generator_v0_1.document_metadata import (
        ensure_pdf_document_metadata,
    )
    from automated_research_report_generator_v0_1.pdf_indexing import (
        ensure_pdf_page_index,
        reset_pdf_preprocessing_runtime_state,
    )

    reset_project_cache_dir()  # 默认先清 .cache；原因是本轮预处理需要确定性重建。
    reset_pdf_preprocessing_runtime_state()  # 默认同时清理进程内 PDF 缓存；原因是避免同进程串数据。
    resolved_pdf_file_path = pdf_file_path or DEFAULT_PDF_FILE_PATH  # 可调：任意可读 PDF 路径；默认样例文件，原因是兼容原有单文件直跑。

    document_metadata = ensure_pdf_document_metadata(
        pdf_file_path=resolved_pdf_file_path,
    )

    # 默认补齐公司名和行业；功能：让 trace 与后续 prompt 使用稳定标签。
    resolved_company_name = (document_metadata.get("company_name") or Path(resolved_pdf_file_path).stem).strip()
    resolved_industry = (document_metadata.get("industry") or "未知行业").strip()
    prepared_inputs: dict[str, str] = {
        "pdf_file_path": resolved_pdf_file_path,
        "company_name": resolved_company_name,
        "industry": resolved_industry,
    }

    page_index_file_path = ensure_pdf_page_index(
        pdf_file_path=resolved_pdf_file_path,
        company_name=prepared_inputs["company_name"],
    )
    prepared_inputs["page_index_file_path"] = page_index_file_path  # 默认回填固定索引路径；功能：供全部 agent/tool 复用。
    prepared_inputs["document_metadata_file_path"] = document_metadata["document_metadata_file_path"]  # 默认保留元数据文件路径；功能：便于人工核对缓存。
    return prepared_inputs


def sanitize_filename_part(value: str) -> str:  # 设计：清洗日志文件名；功能：兼容 Windows 非法字符；默认保留可读性，原因是复盘时需要快速定位公司。
    sanitized_value = re.sub(r'[<>:"/\\|?*]+', "_", value).strip()
    sanitized_value = re.sub(r"\s+", "_", sanitized_value)
    sanitized_value = sanitized_value.rstrip(". ")
    return sanitized_value or "unknown_company"


def build_crew_log_path(company_name: str) -> str:  # 设计：为每次执行生成独立日志文件；功能：按时间戳与公司名归档到 logs 目录；默认 JSON，原因是更适合后续检索。
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_company_name = sanitize_filename_part(company_name)
    PROJECT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    return str(PROJECT_LOG_DIR / f"{timestamp}_{safe_company_name}.json")


def validate_tasks_config() -> None:  # 设计：启动前校验任务配置；功能：尽早发现 context 引用错误；默认每次运行都校验，原因是报错更直接。
    with TASKS_CONFIG_PATH.open("r", encoding="utf-8") as tasks_file:
        tasks_config = yaml.safe_load(tasks_file) or {}

    if not isinstance(tasks_config, dict):
        raise ValueError(f"Invalid tasks config format: {TASKS_CONFIG_PATH}")

    task_names = set(tasks_config.keys())
    for task_name, task_config in tasks_config.items():
        if not isinstance(task_config, dict):
            raise ValueError(f"Task '{task_name}' config must be a mapping in {TASKS_CONFIG_PATH}")

        context_list = task_config.get("context")
        if context_list is None:
            continue

        if not isinstance(context_list, list):
            raise ValueError(f"Task '{task_name}' context must be a list in {TASKS_CONFIG_PATH}")

        for position, context_task_name in enumerate(context_list, start=1):
            if not isinstance(context_task_name, str) or not context_task_name.strip():
                raise ValueError(
                    f"Task '{task_name}' has an empty context entry at position {position} in {TASKS_CONFIG_PATH}"
                )

            if context_task_name not in task_names:
                raise ValueError(
                    f"Task '{task_name}' references unknown context task '{context_task_name}' in {TASKS_CONFIG_PATH}"
                )


def extract_run_pdf_paths(argv: list[str] | None = None) -> list[str]:  # 设计：统一解析 run 命令参数；功能：同时兼容脚本入口和 `main.py run` 入口；可调参数是原始 argv；默认回退 demo PDF，原因是保留旧行为。
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if raw_args and raw_args[0] == "run":
        raw_args = raw_args[1:]

    resolved_pdf_file_paths = [arg for arg in raw_args if arg.strip()]
    return resolved_pdf_file_paths or [DEFAULT_PDF_FILE_PATH]


def kickoff_pdf_run(pdf_file_path: str):  # 设计：封装单个 PDF 的完整执行；功能：清记忆、预处理、构建 crew 并 kickoff；可调参数是 PDF 路径；默认每个文件单独隔离，原因是批量串行时避免串数据。
    from automated_research_report_generator_v0_1.crew import (
        AutomatedResearchReportGeneratorV01Crew,
    )

    reset_crewai_memories()
    inputs = prepare_inputs(pdf_file_path=pdf_file_path)
    crew_instance = AutomatedResearchReportGeneratorV01Crew()  # 默认显式保留实例；功能：便于断点和逐 agent 观察。
    crew_instance.output_log_file_path = build_crew_log_path(inputs["company_name"])
    return crew_instance.crew().kickoff(inputs=inputs)


def run(pdf_file_paths: list[str] | None = None):  # 设计：标准运行入口；功能：支持单文件和多文件串行执行；可调参数是 PDF 路径列表；默认串行逐个跑完，原因是最小改动即可满足批处理。
    resolved_pdf_file_paths = extract_run_pdf_paths(argv=pdf_file_paths)
    validate_tasks_config()

    if len(resolved_pdf_file_paths) == 1:
        return kickoff_pdf_run(resolved_pdf_file_paths[0])

    run_results: list[object] = []
    failed_pdf_file_paths: list[str] = []
    total_count = len(resolved_pdf_file_paths)

    for index, pdf_file_path in enumerate(resolved_pdf_file_paths, start=1):
        print(f"[RUN {index}/{total_count}] {pdf_file_path}")
        try:
            run_results.append(kickoff_pdf_run(pdf_file_path))
            print(f"[DONE {index}/{total_count}] {pdf_file_path}")
        except Exception as exc:
            failed_pdf_file_paths.append(pdf_file_path)
            print(f"[FAIL {index}/{total_count}] {pdf_file_path}: {exc}")

    if failed_pdf_file_paths:
        failed_pdf_file_path_text = ", ".join(failed_pdf_file_paths)
        raise RuntimeError(f"Batch run finished with failures: {failed_pdf_file_path_text}")

    return run_results


def run_with_trigger():  # 设计：外部触发入口；功能：复用标准 run；默认保持薄封装，原因是减少两套逻辑分叉。
    return run()


def train():  # 设计：训练入口；功能：复用预处理后执行 CrewAI train；默认沿用主输入准备，原因是训练与正式运行保持一致。
    from automated_research_report_generator_v0_1.crew import (
        AutomatedResearchReportGeneratorV01Crew,
    )

    inputs = prepare_inputs()
    validate_tasks_config()
    try:
        crew_instance = AutomatedResearchReportGeneratorV01Crew()
        crew_instance.output_log_file_path = build_crew_log_path(inputs["company_name"])
        crew_instance.crew().train(
            n_iterations=int(sys.argv[1]),
            filename=sys.argv[2],
            inputs=inputs,
        )
    except Exception as exc:
        raise Exception(f"An error occurred while training the crew: {exc}")


def replay():  # 设计：回放入口；功能：从指定 task_id 继续排查；默认只校验任务配置，原因是回放不需要重做预处理。
    from automated_research_report_generator_v0_1.crew import (
        AutomatedResearchReportGeneratorV01Crew,
    )

    validate_tasks_config()
    try:
        crew_instance = AutomatedResearchReportGeneratorV01Crew()
        crew_instance.output_log_file_path = build_crew_log_path(f"replay_{sys.argv[1]}")
        crew_instance.crew().replay(task_id=sys.argv[1])
    except Exception as exc:
        raise Exception(f"An error occurred while replaying the crew: {exc}")


def test():  # 设计：测试入口；功能：复用预处理后执行 CrewAI test；默认与 run 共用输入准备，原因是测试场景更贴近真实链路。
    from automated_research_report_generator_v0_1.crew import (
        AutomatedResearchReportGeneratorV01Crew,
    )

    inputs = prepare_inputs()
    validate_tasks_config()
    try:
        crew_instance = AutomatedResearchReportGeneratorV01Crew()
        crew_instance.output_log_file_path = build_crew_log_path(inputs["company_name"])
        crew_instance.crew().test(
            n_iterations=int(sys.argv[1]),
            openai_model_name=sys.argv[2],
            inputs=inputs,
        )
    except Exception as exc:
        raise Exception(f"An error occurred while testing the crew: {exc}")


if __name__ == "__main__":
    if len(sys.argv) < 2:  # 设计：命令分发守卫；功能：缺少子命令时直接退出；默认要求显式传 run/train/replay/test，原因是入口更清晰。
        print("Usage: main.py <command> [<args>]")
        sys.exit(1)

    command = sys.argv[1]  # 默认用直接分发；功能：保持入口简单；原因是当前命令集合很小。
    if command == "run":
        run()
    elif command == "train":
        train()
    elif command == "replay":
        replay()
    elif command == "test":
        test()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
