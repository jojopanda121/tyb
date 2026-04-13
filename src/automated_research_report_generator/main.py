from __future__ import annotations

import argparse
import sys
import threading
from pathlib import Path
from typing import Callable, TextIO

from automated_research_report_generator.flow.common import (
    CACHE_ROOT,
    DEFAULT_PDF_PATH,
    append_text_log_chunk,
    ensure_directory,
    normalize_path,
    reset_runtime_logging_state,
    run_console_log_path,
)
from automated_research_report_generator.flow.research_flow import ResearchReportFlow

# 设计目的：统一 v2 的命令行入口和脚本入口，避免不同入口各自拼装 flow 调用。
# 模块功能：提供默认输入、启动主流程、生成流程图，并解析命令行参数。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：`--pdf` 和 `--plot`。
# 默认参数及原因：默认读取 `DEFAULT_PDF_PATH` 并执行 `kickoff()`，原因是更贴近日常本地运行。


def default_inputs() -> dict[str, str]:
    """
    设计目的：集中定义命令行和脚本共用的默认输入。
    模块功能：返回主流程启动时使用的最小输入字典。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：当前只包含 `pdf_file_path`。
    默认参数及原因：默认使用 `DEFAULT_PDF_PATH`，原因是方便本地直接运行。
    """

    return {"pdf_file_path": DEFAULT_PDF_PATH.as_posix()}


class RunConsoleTranscript:
    """
    目的：把单次运行期间 PowerShell 终端里看到的原始输出统一转储到 run 目录日志中。
    功能：在 `run_slug` 尚未生成时先缓冲文本，等 run 目录确定后再一次性回灌到 `logs/console.txt`。
    实现逻辑：通过 `run_slug_getter` 动态感知 Flow 状态；有 `run_slug` 时直接落盘，没有时先保存在内存缓冲区。
    可调参数：`run_slug_getter` 和 `fallback_label`。
    默认参数及原因：默认优先等待真实 `run_slug`，原因是正常情况下终端 transcript 应该和本次 run 的其他产物严格放在同一目录下。
    """

    def __init__(self, run_slug_getter: Callable[[], str], fallback_label: str = "") -> None:
        """
        目的：初始化终端 transcript 管理器。
        功能：保存 run slug 解析函数、失败兜底标签和线程安全所需状态。
        实现逻辑：创建锁、缓冲区和当前活动日志路径占位。
        可调参数：`run_slug_getter` 用于动态读取 Flow 当前 run slug，`fallback_label` 用于早期异常时的兜底目录命名。
        默认参数及原因：`fallback_label` 默认允许为空，原因是大多数正常运行都能在 `prepare_evidence` 后拿到真实 run slug。
        """

        self._run_slug_getter = run_slug_getter
        self._fallback_label = fallback_label.strip() or "unknown"
        self._lock = threading.Lock()
        self._buffer: list[str] = []
        self._active_log_path = ""

    def _resolve_run_log_path(self) -> str:
        """
        目的：按当前 Flow 状态解析真正的 run 级 console log 路径。
        功能：有 `run_slug` 时返回 `.cache/<run_slug>/logs/console.txt`，没有时返回空串。
        实现逻辑：调用外部注入的 `run_slug_getter`，拿到值后委托 `run_console_log_path()` 组装路径。
        可调参数：当前无显式参数。
        默认参数及原因：未拿到 `run_slug` 时返回空串，原因是此时还不能安全绑定到具体 run 目录。
        """

        run_slug = str(self._run_slug_getter() or "").strip()
        if not run_slug:
            return ""
        return run_console_log_path(run_slug)

    def _fallback_log_path(self) -> str:
        """
        目的：在 run slug 迟迟拿不到时，为 transcript 提供最小可追踪的兜底落盘位置。
        功能：返回 `.cache/_console_bootstrap_<label>/logs/console.txt`。
        实现逻辑：先创建兜底目录，再返回标准化后的日志文件路径。
        可调参数：当前无显式参数。
        默认参数及原因：默认只在异常早退时使用兜底路径，原因是常规路径仍应严格落在实际 run 目录下。
        """

        fallback_dir = ensure_directory(CACHE_ROOT / f"_console_bootstrap_{self._fallback_label}" / "logs")
        return normalize_path(fallback_dir / "console.txt")

    def _flush_buffer_locked(self, log_path: str) -> None:
        """
        目的：把 run slug 确定前缓存的终端输出补写到最终 transcript 文件。
        功能：保持 PowerShell 终端输出的完整先后顺序。
        实现逻辑：把内存缓冲区拼接成完整文本块后一次性追加到目标文件，再清空缓冲。
        可调参数：`log_path`。
        默认参数及原因：默认整块写入，原因是这样更容易保持早期 stdout/stderr 交错顺序。
        """

        if not self._buffer:
            return
        append_text_log_chunk(log_path, "".join(self._buffer))
        self._buffer.clear()

    def append(self, text: str) -> None:
        """
        目的：接收一段新的终端输出并落盘或暂存。
        功能：在 run 目录可用时实时追加到 console log，不可用时先缓冲。
        实现逻辑：加锁后先尝试解析 run log 路径，再决定是立即写文件还是写入内存缓冲区。
        可调参数：`text`。
        默认参数及原因：空文本直接跳过，原因是许多终端包装层会重复触发空写入。
        """

        if not text:
            return
        with self._lock:
            log_path = self._resolve_run_log_path()
            if not log_path:
                self._buffer.append(text)
                return
            self._flush_buffer_locked(log_path)
            append_text_log_chunk(log_path, text)
            self._active_log_path = log_path

    def finalize(self) -> str:
        """
        目的：在主流程结束或异常退出时收尾 transcript 落盘。
        功能：确保缓冲区里的最后一段终端输出不会因为 run 结束而丢失。
        实现逻辑：优先写入真实 run log；如果 run slug 始终不可用，则写入兜底 bootstrap 路径。
        可调参数：当前无显式参数。
        默认参数及原因：默认优先真实 run 路径，原因是 run 目录才是单次执行的正式归档位置。
        """

        with self._lock:
            log_path = self._resolve_run_log_path() or self._active_log_path or self._fallback_log_path()
            self._flush_buffer_locked(log_path)
            self._active_log_path = log_path
            return log_path


class ConsoleStreamTee:
    """
    目的：把单个标准流同时输出到 PowerShell 终端和 run 级 console transcript。
    功能：代理 `stdout` 或 `stderr`，在不改变终端显示行为的前提下额外写入 transcript。
    实现逻辑：每次 `write()` 时先写原始流，再把同样文本交给 `RunConsoleTranscript` 处理。
    可调参数：`original_stream` 和 `transcript`。
    默认参数及原因：默认委托原始流处理编码、刷新和终端能力，原因是要尽量减少对第三方库输出行为的扰动。
    """

    def __init__(self, original_stream: TextIO, transcript: RunConsoleTranscript) -> None:
        """
        目的：初始化单个标准流的 tee 包装器。
        功能：保存原始流对象和共享 transcript。
        实现逻辑：仅保留最小状态，把其他能力交给原始流或 `__getattr__` 透传。
        可调参数：`original_stream` 和 `transcript`。
        默认参数及原因：默认和原始流共享编码配置，原因是避免终端中文输出被额外转换。
        """

        self._original_stream = original_stream
        self._transcript = transcript

    def write(self, text: str) -> int:
        """
        目的：把一段文本同时写到终端和 transcript。
        功能：保留 PowerShell 原始显示，同时为 run 目录生成完整 console 记录。
        实现逻辑：先调用原始流 `write()`，再把相同文本交给 transcript。
        可调参数：`text`。
        默认参数及原因：默认先写终端再写文件，原因是用户在交互时优先关心即时可见输出。
        """

        written = self._original_stream.write(text)
        self._transcript.append(text)
        return len(text) if written is None else written

    def flush(self) -> None:
        """
        目的：兼容标准流刷新语义。
        功能：把刷新动作透传给原始终端流。
        实现逻辑：直接调用原始流的 `flush()`。
        可调参数：当前无。
        默认参数及原因：默认不额外维护文件缓冲，原因是 transcript 已经按块立即写盘。
        """

        self._original_stream.flush()

    def writable(self) -> bool:
        """
        目的：向依赖标准流协议的调用方声明当前包装器可写。
        功能：兼容 `print()` 和部分第三方库的流能力检查。
        实现逻辑：固定返回 `True`。
        可调参数：当前无。
        默认参数及原因：默认可写，原因是该包装器仅用于写入型标准流。
        """

        return True

    def __getattr__(self, name: str):
        """
        目的：把未覆盖的标准流属性和方法继续代理给原始流。
        功能：兼容 `encoding`、`isatty()`、`fileno()` 等标准流接口。
        实现逻辑：对未知属性统一转发到原始流对象。
        可调参数：`name`。
        默认参数及原因：默认透传，原因是这样改动最小，兼容性最好。
        """

        return getattr(self._original_stream, name)


def _console_fallback_label(inputs: dict[str, str]) -> str:
    """
    目的：给极早期失败场景生成一个可读的 transcript 兜底标签。
    功能：优先使用输入 PDF 的 stem 作为标签，拿不到时回退到 `unknown`。
    实现逻辑：读取 `pdf_file_path`，提取文件名 stem 并做最小清洗。
    可调参数：`inputs`。
    默认参数及原因：默认优先用 PDF stem，原因是 run slug 尚未生成时它是最稳定的可识别上下文。
    """

    pdf_file_path = str(inputs.get("pdf_file_path", "")).strip()
    if not pdf_file_path:
        return "unknown"
    stem = Path(pdf_file_path).stem.strip()
    return stem or "unknown"


def kickoff(inputs: dict[str, str] | None = None):
    """
    设计目的：统一流程启动入口。
    模块功能：合并默认输入与外部参数，再执行 `ResearchReportFlow.kickoff()`。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`inputs`。
    默认参数及原因：先加载默认输入，原因是命令行、脚本和测试都能复用同一套入口。
    """

    merged_inputs = default_inputs()
    if inputs:
        merged_inputs.update(inputs)
    reset_runtime_logging_state()
    flow = ResearchReportFlow()
    transcript = RunConsoleTranscript(
        run_slug_getter=lambda: getattr(flow.state, "run_slug", ""),
        fallback_label=_console_fallback_label(merged_inputs),
    )
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = ConsoleStreamTee(original_stdout, transcript)
    sys.stderr = ConsoleStreamTee(original_stderr, transcript)
    try:
        return flow.kickoff(inputs=merged_inputs)
    finally:
        try:
            transcript.finalize()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr


def run():
    """
    设计目的：给脚本入口和 `pyproject` 命令提供最短调用路径。
    模块功能：直接转调 `kickoff()`。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：无。
    默认参数及原因：不额外包参数，原因是保持入口简单，避免出现多套默认值。
    """

    return kickoff()


def plot():
    """
    设计目的：提供流程图生成入口，方便检查 flow 结构。
    模块功能：实例化 `ResearchReportFlow` 并调用 `plot()`。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：当前固定输出名 `ResearchReportFlowV2`。
    默认参数及原因：固定图名，原因是便于持续覆盖最新版本而不是产生过多临时文件。
    """

    flow = ResearchReportFlow()
    return flow.plot("ResearchReportFlowV2")


def _parse_args() -> argparse.Namespace:
    """
    设计目的：集中管理命令行参数解析。
    模块功能：解析 PDF 路径和是否绘图两个参数。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`--pdf` 与 `--plot`。
    默认参数及原因：默认沿用 `DEFAULT_PDF_PATH`，原因是和 `default_inputs()` 保持一致。
    """

    parser = argparse.ArgumentParser(description="Run the CrewAI v2 research-report flow.")
    parser.add_argument("--pdf", default=DEFAULT_PDF_PATH.as_posix(), help="PDF path to analyze.")
    parser.add_argument("--plot", action="store_true", help="Generate the flow plot instead of running the flow.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.plot:
        plot()
    else:
        kickoff({"pdf_file_path": args.pdf})
