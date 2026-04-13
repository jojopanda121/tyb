from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import automated_research_report_generator.main as main_module
from automated_research_report_generator.flow import common as common_module


def test_kickoff_tees_stdout_and_stderr_to_run_console_log(tmp_path, monkeypatch, capsys):
    """
    目的：锁住主入口会把 PowerShell 可见的 stdout/stderr 同步写入本次 run 的 console transcript。
    功能：验证 run slug 生成前的早期输出会被缓冲，run slug 确定后会统一写进 `logs/console.txt`。
    实现逻辑：用假的 Flow 替换真实执行逻辑，在 kickoff 内先后输出 stdout/stderr，并断言终端与文件都能看到完整内容。
    可调参数：`tmp_path`、`monkeypatch` 和 `capsys`。
    默认参数及原因：默认只模拟最小 Flow 状态，原因是这里关注的是入口层 tee 行为，不是业务执行本身。
    """

    cache_root = tmp_path / ".cache"
    monkeypatch.setattr(common_module, "CACHE_ROOT", cache_root)
    monkeypatch.setattr(main_module, "CACHE_ROOT", cache_root)
    monkeypatch.setattr(main_module, "reset_runtime_logging_state", lambda: None)

    class FakeFlow:
        """
        目的：给入口层 console tee 测试提供一个最小可控的 Flow 替身。
        功能：模拟 kickoff 过程中先输出早期文本，再生成 run slug，最后继续输出 stdout/stderr。
        实现逻辑：通过 `SimpleNamespace` 暴露最小 state，并在 `kickoff()` 里按顺序写入终端。
        可调参数：当前无。
        默认参数及原因：默认 run slug 固定为测试值，原因是便于断言最终 console log 路径。
        """

        def __init__(self) -> None:
            """
            目的：初始化假 Flow 的最小状态。
            功能：提供 `run_slug` 字段供入口 tee 动态感知。
            实现逻辑：把 `state` 设为带 `run_slug` 的简单命名空间。
            可调参数：当前无。
            默认参数及原因：初始 `run_slug` 为空，原因是要模拟真实流程里 `prepare_evidence` 之前尚未建 run 目录的状态。
            """

            self.state = SimpleNamespace(run_slug="")

        def kickoff(self, inputs: dict[str, str]):
            """
            目的：按最小顺序模拟真实 Flow kickoff 的终端输出过程。
            功能：先输出 run slug 生成前文本，再写入 run slug，最后继续输出 stdout/stderr。
            实现逻辑：直接使用 `print()` 和 `sys.stderr.write()` 触发 tee 包装器。
            可调参数：`inputs`。
            默认参数及原因：默认只回传输入 PDF 路径，原因是测试只需要确认 kickoff 正常返回。
            """

            print("before-run-slug")
            self.state.run_slug = "20260413_测试公司"
            print("after-run-slug")
            sys.stderr.write("stderr-line\n")
            return {"pdf_file_path": inputs["pdf_file_path"]}

    monkeypatch.setattr(main_module, "ResearchReportFlow", FakeFlow)

    result = main_module.kickoff({"pdf_file_path": "sample.pdf"})

    console_log_path = Path(common_module.run_console_log_path("20260413_测试公司"))
    transcript = console_log_path.read_text(encoding="utf-8")
    captured = capsys.readouterr()

    assert result == {"pdf_file_path": "sample.pdf"}
    assert console_log_path == cache_root / "20260413_测试公司" / "logs" / "console.txt"
    assert "before-run-slug" in transcript
    assert "after-run-slug" in transcript
    assert "stderr-line" in transcript
    assert "before-run-slug" in captured.out
    assert "after-run-slug" in captured.out
    assert "stderr-line" in captured.err


def test_write_run_debug_manifest_includes_console_log_path(tmp_path, monkeypatch):
    """
    目的：锁住 run manifest 会记录 console transcript 的固定路径。
    功能：验证 manifest 除了 preprocess/flow/crew 日志外，还会写出 `console_log_file_path`。
    实现逻辑：把 `CACHE_ROOT` 指到临时目录，调用真实 manifest 写入函数后回读 JSON 断言。
    可调参数：`tmp_path` 和 `monkeypatch`。
    默认参数及原因：默认使用最小路径参数，原因是这里只验证 manifest 的索引行为。
    """

    cache_root = tmp_path / ".cache"
    monkeypatch.setattr(common_module, "CACHE_ROOT", cache_root)

    manifest_path = common_module.write_run_debug_manifest(
        run_slug="20260413_测试公司",
        status="prepared",
        pdf_file_path="sample.pdf",
        run_cache_dir=(cache_root / "20260413_测试公司" / "md").as_posix(),
    )

    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))

    assert manifest["console_log_file_path"] == (
        cache_root / "20260413_测试公司" / "logs" / "console.txt"
    ).resolve().as_posix()
