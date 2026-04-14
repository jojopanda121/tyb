from __future__ import annotations

from pathlib import Path
from typing import List

from crewai import Agent, Crew, Process, Task
from crewai.agents.agent_builder.base_agent import BaseAgent
from crewai.project import CrewBase, agent, crew, task

from automated_research_report_generator.flow.common import PROJECT_ROOT
from automated_research_report_generator.llm_config import get_heavy_llm
from automated_research_report_generator.tools.markdown_to_pdf_tool import MarkdownToPdfTool

# 设计目的：把最终 Markdown 的非破坏性确认和 PDF 导出拆开，避免 writeup 阶段再次改写正文。
# 模块功能：提供轻量 report editor，先确认 flow 预生成的 Markdown 已就绪，再执行 PDF 导出，并写稳定日志。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：editor 的 temperature、max_iter 和 `output_log_file_path`。
# 默认参数及原因：默认 `temperature=0.1`，原因是这一步只做确认和导出，不需要额外发散。

PROJECT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CREW_LOG_FILE = str(PROJECT_LOG_DIR / "writeup_crew.json")


@CrewBase
class WriteupCrew:
    """
    设计目的：把最终 Markdown 的确认与 PDF 导出固定为一个独立阶段，避免和上游研究判断混在一起。
    模块功能：集中声明最终报告确认任务、PDF 导出任务和写作阶段日志配置。
    实现逻辑：先确认 flow 已经生成最终 Markdown，再把同一份成文结果交给 PDF 导出工具。
    可调参数：YAML 配置、日志路径、模型温度、迭代次数和导出工具。
    默认参数及原因：默认顺序执行，原因是 PDF 必须建立在已经由 flow 定稿的 Markdown 之上。
    """

    agents: List[BaseAgent]
    tasks: List[Task]

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"
    output_log_file_path: str | bool | None = DEFAULT_CREW_LOG_FILE

    @agent
    def report_editor(self) -> Agent:
        """
        设计目的：集中定义最终报告编辑角色。
        模块功能：为最终确认与导出阶段提供稳定的语言模型角色。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：agent 配置、temperature 和迭代次数。
        默认参数及原因：默认 `temperature=0.1`，原因是这里只允许做非破坏性确认，不需要更高表达弹性。
        """
        return Agent(
            config=self.agents_config["report_editor"],  # type: ignore[index]
            tools=[],
            llm=get_heavy_llm(temperature=0.1),
            function_calling_llm=None,
            max_iter=25,
            max_rpm=None,
            max_execution_time=None,
            verbose=True,
            allow_delegation=False,
            step_callback=None,
            cache=True,
            allow_code_execution=False,
            max_retry_limit=2,
            respect_context_window=True,
            use_system_prompt=True,
            reasoning=False,
            max_reasoning_attempts=None,
            inject_date=True,
        )

    @task
    def compile_report(self) -> Task:
        """
        设计目的：定义最终 Markdown 的非破坏性确认任务。
        模块功能：创建对应 Task 实例。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：任务配置和输出格式。
        默认参数及原因：默认不写 Markdown 文件，原因是最终正文已经由 flow 预先生成，不允许这里再覆盖。
        """
        return Task(
            config=self.tasks_config["compile_report"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=False,
        )

    @task
    def export_final_report(self) -> Task:
        """
        设计目的：定义最终 PDF 导出任务。
        模块功能：在编写完成后调用 `MarkdownToPdfTool` 导出 PDF。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：任务配置、上下文依赖和工具列表。
        默认参数及原因：默认依赖 `compile_report()`，原因是先确认 flow 产物已就绪，再导出会更稳。
        """

        return Task(
            config=self.tasks_config["export_final_report"],  # type: ignore[index]
            context=[self.compile_report()],
            tools=[MarkdownToPdfTool()],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=False,
        )

    @crew
    def crew(self) -> Crew:
        """
        设计目的：统一返回写作阶段使用的 Crew 实例。
        模块功能：确保日志目录存在，并构造顺序执行的 writeup crew。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：日志路径、缓存、tracing 和 `chat_llm`。
        默认参数及原因：默认采用 `Process.sequential`，原因是先确认 Markdown 就绪、再导出 PDF。
        """
        if isinstance(self.output_log_file_path, str):
            Path(self.output_log_file_path).parent.mkdir(parents=True, exist_ok=True)
        return Crew(
            name="writeup_crew",
            agents=self.agents,
            tasks=self.tasks,
            process=Process.sequential,
            verbose=True,
            manager_llm=None,
            manager_agent=None,
            function_calling_llm=None,
            config=None,
            max_rpm=None,
            memory=False,
            cache=True,
            embedder=None,
            share_crew=False,
            step_callback=None,
            task_callback=None,
            planning=False,
            planning_llm=None,
            tracing=True,
            output_log_file=self.output_log_file_path,
            chat_llm=get_heavy_llm(),
        )
