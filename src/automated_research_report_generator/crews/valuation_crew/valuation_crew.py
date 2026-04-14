from __future__ import annotations

from pathlib import Path
from typing import List

from crewai import Agent, Crew, Process, Task
from crewai.agents.agent_builder.base_agent import BaseAgent
from crewai.project import CrewBase, agent, crew, task
from crewai_tools import SerperDevTool

from automated_research_report_generator.flow.common import PROJECT_ROOT
from automated_research_report_generator.llm_config import get_heavy_llm
from automated_research_report_generator.tools import (
    AddEntryTool,
    AddEvidenceTool,
    ComparableValuationTool,
    FootballFieldTool,
    IntrinsicValuationTool,
    ReadRegistryTool,
    RegistryReviewTool,
    StatusUpdateTool,
    TushareValuationDataTool,
    UpdateEntryTool,
)
from automated_research_report_generator.tools.pdf_page_tools import (
    ReadPdfPageIndexTool,
    ReadPdfPagesTool,
)

# 设计目的：把同行估值、内在价值估值和最终估值汇总拆开，方便分别复核每一种估值方法。
# 模块功能：提供 3 个估值 agent，顺序执行同行、内在价值和估值汇总任务，并写稳定日志。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：估值 agent 的 temperature、max_iter 和 `output_log_file_path`。
# 默认参数及原因：DCF 角色默认更低温度，原因是模型推导比文字判断更需要收敛。

PROJECT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CREW_LOG_FILE = str(PROJECT_LOG_DIR / "valuation_crew.json")

# 设计目的：让估值相关 agent 共用同一套搜索和 PDF 访问入口，保证估值引用口径一致。
# 模块功能：Serper 负责外部检索，PDF 工具负责页码定位和正文读取。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：后续如果搜索或 PDF 后端变化，可在这里统一替换工具类。
# 默认参数及原因：当前直接使用默认构造，因为项目已经把标准行为封装在工具类里。
shared_search_tool = SerperDevTool()
shared_pdf_page_index_tool = ReadPdfPageIndexTool()
shared_pdf_page_reader_tool = ReadPdfPagesTool()
shared_tushare_valuation_data_tool = TushareValuationDataTool()


@CrewBase
class ValuationCrew:
    """
    设计目的：把同行估值、内在价值和最终估值汇总拆开，便于分别复核每一层假设。
    模块功能：集中声明可比公司、现金流估值和估值汇总三类角色及任务。
    实现逻辑：先做 peer set，再做内在价值推导，最后把两类结果合并成估值结论。
    可调参数：YAML 配置、日志路径、工具列表、模型温度和迭代次数。
    默认参数及原因：默认顺序执行，原因是最终估值汇总必须建立在前两步已经产出的材料上。
    """

    agents: List[BaseAgent]
    tasks: List[Task]

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"
    output_log_file_path: str | bool | None = DEFAULT_CREW_LOG_FILE

    @agent
    def peer_analyst(self) -> Agent:
        """
        设计目的：集中定义可比公司分析角色。
        模块功能：为同行筛选和相对估值注入 PDF、registry、搜索和可比估值工具。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：agent 配置、工具列表、temperature 和迭代次数。
        默认参数及原因：默认 `temperature=0.5`，原因是可比池构建需要一定展开空间。
        """
        return Agent(
            config=self.agents_config["peer_analyst"],  # type: ignore[index]
            tools=[
                shared_pdf_page_index_tool,
                shared_pdf_page_reader_tool,
                AddEntryTool(),
                UpdateEntryTool(),
                AddEvidenceTool(),
                ReadRegistryTool(),
                StatusUpdateTool(),
                RegistryReviewTool(),
                shared_search_tool,
                shared_tushare_valuation_data_tool,
                ComparableValuationTool(),
            ],
            llm=get_heavy_llm(temperature=0.5),
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

    @agent
    def cashflow_analyst(self) -> Agent:
        """
        设计目的：集中定义内在价值分析角色。
        模块功能：为 DCF 类任务注入 PDF、registry 和内在价值工具。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：agent 配置、工具列表、temperature 和迭代次数。
        默认参数及原因：默认 `temperature=0.1`，原因是现金流建模需要严谨。
        """
        return Agent(
            config=self.agents_config["cashflow_analyst"],  # type: ignore[index]
            tools=[
                shared_pdf_page_index_tool,
                shared_pdf_page_reader_tool,
                AddEntryTool(),
                UpdateEntryTool(),
                AddEvidenceTool(),
                ReadRegistryTool(),
                StatusUpdateTool(),
                RegistryReviewTool(),
                shared_tushare_valuation_data_tool,
                IntrinsicValuationTool(),
            ],
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

    @agent
    def valuation_analyst(self) -> Agent:
        """
        设计目的：集中定义最终估值汇总角色。
        模块功能：让该角色同时能访问相对估值和内在价值两类工具。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：agent 配置、工具列表、temperature 和迭代次数。
        默认参数及原因：默认 `temperature=0.5`，原因是汇总阶段需要一定判断展开空间。
        """

        return Agent(
            config=self.agents_config["valuation_analyst"],  # type: ignore[index]
            tools=[
                shared_pdf_page_index_tool,
                shared_pdf_page_reader_tool,
                ReadRegistryTool(),
                shared_tushare_valuation_data_tool,
                ComparableValuationTool(),
                IntrinsicValuationTool(),
                FootballFieldTool(),
            ],
            llm=get_heavy_llm(temperature=0.5),
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
    def build_peer_set(self) -> Task:
        """
        设计目的：定义可比公司集合构建任务。
        模块功能：创建同行筛选和相对估值的起始任务。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：任务配置和输出格式。
        默认参数及原因：默认输出 markdown，原因是 peer list 需要人工复核。
        """

        return Task(
            config=self.tasks_config["build_peer_set"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def derive_intrinsic_valuation(self) -> Task:
        """
        设计目的：定义内在价值推导任务。
        模块功能：创建 DCF 或简化现金流估值任务。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：任务配置和输出格式。
        默认参数及原因：默认输出 markdown，原因是便于和同行估值结论并排复核。
        """

        return Task(
            config=self.tasks_config["derive_intrinsic_valuation"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def summarize_valuation(self) -> Task:
        """
        设计目的：把不同估值方法汇总成统一结论。
        模块功能：创建估值汇总任务，并接入前两步结果作为上下文。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：任务配置和上下文依赖。
        默认参数及原因：默认依赖同行和内在价值任务，原因是最终估值结论必须同时参考两边。
        """

        return Task(
            config=self.tasks_config["summarize_valuation"],  # type: ignore[index]
            context=[self.build_peer_set(), self.derive_intrinsic_valuation()],
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @crew
    def crew(self) -> Crew:
        """
        设计目的：统一返回估值阶段使用的 Crew 实例。
        模块功能：确保日志目录存在，并构造层级执行的 valuation crew。
        实现逻辑：通过 `Process.hierarchical` 让 manager 统一调度估值任务。
        可调参数：日志路径、缓存、tracing 和 `chat_llm`。
        默认参数及原因：默认采用 `Process.hierarchical`，原因是本次设计要求把估值 QA 收敛到 manager 内部。
        """
        if isinstance(self.output_log_file_path, str):
            Path(self.output_log_file_path).parent.mkdir(parents=True, exist_ok=True)
        return Crew(
            name="valuation_crew",
            agents=self.agents,
            tasks=self.tasks,
            process=Process.hierarchical,
            verbose=True,
            manager_llm=get_heavy_llm(temperature=0.1),
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
