from __future__ import annotations

import pytest

from automated_research_report_generator_v0_1 import main


"""main 批量 PDF 串行运行测试"""


def test_extract_run_pdf_paths_supports_main_style_args() -> None:  # 设计：覆盖 `main.py run a.pdf b.pdf` 参数形态；功能：确认会跳过 `run` 子命令只保留 PDF；可调参数是模拟 argv；默认保留多文件顺序，原因是批量执行依赖稳定顺序。
    assert main.extract_run_pdf_paths(["run", "pdf/a.pdf", "pdf/b.pdf"]) == [
        "pdf/a.pdf",
        "pdf/b.pdf",
    ]


def test_extract_run_pdf_paths_falls_back_to_default_pdf() -> None:  # 设计：覆盖空参数回退；功能：确认旧的 demo 单文件入口仍然可用；可调参数是空 argv；默认回退到内置 PDF，原因是避免破坏现有使用方式。
    assert main.extract_run_pdf_paths([]) == [main.DEFAULT_PDF_FILE_PATH]


def test_run_processes_multiple_pdfs_in_order(monkeypatch: pytest.MonkeyPatch) -> None:  # 设计：覆盖多 PDF 串行主流程；功能：确认校验只做一次且文件按传入顺序依次运行；可调参数是 monkeypatch 替身；默认顺序执行，原因是这是本次需求的核心行为。
    call_order: list[str] = []

    monkeypatch.setattr(main, "validate_tasks_config", lambda: call_order.append("validate"))

    def fake_kickoff_pdf_run(pdf_file_path: str) -> str:  # 设计：模拟单文件运行结果；功能：记录执行顺序并返回可断言结果；可调参数是 PDF 路径；默认直接返回字符串，原因是测试只关心批处理编排。
        call_order.append(pdf_file_path)
        return f"done:{pdf_file_path}"

    monkeypatch.setattr(main, "kickoff_pdf_run", fake_kickoff_pdf_run)

    results = main.run(["first.pdf", "second.pdf", "third.pdf"])

    assert results == [
        "done:first.pdf",
        "done:second.pdf",
        "done:third.pdf",
    ]
    assert call_order == ["validate", "first.pdf", "second.pdf", "third.pdf"]


def test_run_continues_after_single_pdf_failure(monkeypatch: pytest.MonkeyPatch) -> None:  # 设计：覆盖批处理中途失败场景；功能：确认单个 PDF 失败后仍会继续跑后续文件并在最后汇总报错；可调参数是 monkeypatch 替身；默认最终抛错，原因是批任务需要显式暴露失败结果。
    call_order: list[str] = []

    monkeypatch.setattr(main, "validate_tasks_config", lambda: call_order.append("validate"))

    def fake_kickoff_pdf_run(pdf_file_path: str) -> str:  # 设计：模拟部分失败的单文件运行；功能：在指定文件上抛错并记录全部尝试顺序；可调参数是 PDF 路径；默认只让 `bad.pdf` 失败，原因是方便精确验证继续执行逻辑。
        call_order.append(pdf_file_path)
        if pdf_file_path == "bad.pdf":
            raise ValueError("boom")
        return f"done:{pdf_file_path}"

    monkeypatch.setattr(main, "kickoff_pdf_run", fake_kickoff_pdf_run)

    with pytest.raises(RuntimeError, match="bad\\.pdf"):
        main.run(["good-1.pdf", "bad.pdf", "good-2.pdf"])

    assert call_order == ["validate", "good-1.pdf", "bad.pdf", "good-2.pdf"]
