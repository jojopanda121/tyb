from __future__ import annotations

from pathlib import Path
from typing import Callable, List

from crewai import Agent, Crew, Process, Task
from crewai.agents.agent_builder.base_agent import BaseAgent
from crewai.project import CrewBase, agent, crew, task
from crewai_tools import SerperDevTool

from automated_research_report_generator.crews.crew_profile_loader import (
    load_research_task_profile,
    strip_research_task_profile_fields,
)
from automated_research_report_generator.flow.common import PROJECT_ROOT
from automated_research_report_generator.flow.models import ResearchRegistryCheckResult
from automated_research_report_generator.llm_config import get_heavy_llm
from automated_research_report_generator.tools import (
    AddEntryTool,
    AddEvidenceTool,
    ReadRegistryTool,
    RegistryReviewTool,
    StatusUpdateTool,
    UpdateEntryTool,
)
from automated_research_report_generator.tools.pdf_page_tools import ReadPdfPageIndexTool, ReadPdfPagesTool

# 设计目的：把历史与背景专题的 research sub-crew 独立放回本文件，避免共享基类隐藏真实定义。
# 模块功能：提供历史专题所需的 4 个 agent、4 个 task 和层级执行 crew。
# 实现逻辑：保留 YAML 配置，但由本文件直接完成 tools 组装、agent/task 绑定和 crew 输出。
# 可调参数：历史专题 guidance、额外工具工厂、日志路径和模型温度。
# 默认参数及原因：默认更强调 PDF 原文，原因是历史与治理信息通常以公司披露为主。

PROJECT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CREW_LOG_FILE = str(PROJECT_LOG_DIR / "history_background_crew.json")
shared_pdf_page_index_tool = ReadPdfPageIndexTool()
shared_pdf_page_reader_tool = ReadPdfPagesTool()
CREW_PROFILE = load_research_task_profile(__file__)


@CrewBase
class HistoryBackgroundCrew:
    """
    目的：承接历史、背景与治理专题的 research 子 crew。
    功能：围绕公司发展历史、治理结构和关键里程碑产出历史与背景分析包。
    实现逻辑：本文件直接声明历史专题的 agent、task 和 crew，不再依赖共享基类。
    可调参数：YAML 配置、专题 guidance、额外工具工厂、日志路径和模型温度。
    默认参数及原因：默认更强调 PDF 原文，原因是历史与治理信息通常以公司披露为主。
    """

    agents: List[BaseAgent]
    tasks: List[Task]

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"
    output_log_file_path: str | bool | None = DEFAULT_CREW_LOG_FILE

    crew_name = CREW_PROFILE["crew_name"]
    pack_name = CREW_PROFILE["pack_name"]
    pack_title = CREW_PROFILE["pack_title"]
    pack_focus = CREW_PROFILE["pack_focus"]
    output_title = CREW_PROFILE["output_title"]
    search_guidance = CREW_PROFILE["search_guidance"]
    extract_guidance = CREW_PROFILE["extract_guidance"]
    qa_guidance = CREW_PROFILE["qa_guidance"]
    synthesize_guidance = CREW_PROFILE["synthesize_guidance"]
    output_skeleton = CREW_PROFILE["output_skeleton"]
    use_search_tool = True
    default_temperature = 0.2
    extra_tool_factories: tuple[Callable[[], object], ...] = ()

    def _extra_tools(self) -> list[object]:
        """
        目的：集中生成历史专题额外工具实例。
        功能：根据 `extra_tool_factories` 返回额外 tools 列表。
        实现逻辑：逐个调用工厂函数并收集返回值。
        可调参数：`extra_tool_factories`。
        默认参数及原因：默认返回空列表，原因是历史专题当前只依赖通用 research tools。
        """

        return [factory() for factory in self.extra_tool_factories]

    def _search_tools(self) -> list[object]:
        """
        目的：集中组装 `search_facts` 任务可用的工具集合。
        功能：只给外部搜索候选采集阶段开放搜索类工具和只读 registry 能力。
        实现逻辑：按需注入 `SerperDevTool`，再追加 `ReadRegistryTool` 和专题扩展工具。
        可调参数：`use_search_tool` 与 `extra_tool_factories`。
        默认参数及原因：默认启用搜索工具并保留专题扩展工具，原因是外部补证阶段仍需要公开资料与专题数据源。
        """

        tools: list[object] = [ReadRegistryTool()]
        if self.use_search_tool:
            tools.insert(0, SerperDevTool())
        tools.extend(self._extra_tools())
        return tools

    def _extract_tools(self) -> list[object]:
        """
        目的：集中组装 `extract_file_facts` 任务可用的工具集合。
        功能：只给 PDF 候选采集阶段开放页码索引、页内容读取和只读 registry 能力。
        实现逻辑：固定返回 PDF 页面索引工具、页面读取工具和 `ReadRegistryTool`。
        可调参数：当前无显式参数。
        默认参数及原因：默认不开放任何 registry 写入能力，原因是提取阶段只负责产出候选 patch 报告。
        """

        return [
            shared_pdf_page_index_tool,
            shared_pdf_page_reader_tool,
            ReadRegistryTool(),
        ]

    def _registry_recording_tools(self) -> list[object]:
        """
        目的：集中组装两个 `record_*` 任务可用的 registry 落账工具集合。
        功能：给登记与修订阶段开放条目读取、更新、新增、证据补充和状态留痕能力。
        实现逻辑：固定返回 registry 读写相关工具，不注入搜索或 PDF 工具。
        可调参数：当前无显式参数。
        默认参数及原因：默认把 amend 与新增都放在这个阶段，原因是要把“采集”和“落账”职责硬隔离。
        """

        return [
            ReadRegistryTool(),
            UpdateEntryTool(),
            AddEntryTool(),
            AddEvidenceTool(),
            StatusUpdateTool(),
            RegistryReviewTool(),
        ]

    def _qa_tools(self) -> list[object]:
        """
        目的：集中组装 `check_registry` 任务可用的最小 QA 工具集合。
        功能：给内部 QA 阶段提供 registry 检查、状态标记和 review 留痕能力。
        实现逻辑：固定返回 `ReadRegistryTool`、`StatusUpdateTool` 和 `RegistryReviewTool`。
        可调参数：当前无显式参数。
        默认参数及原因：默认不开放新增证据与新增条目能力，原因是 QA 只做检查和返工标记。
        """

        return [
            ReadRegistryTool(),
            StatusUpdateTool(),
            RegistryReviewTool(),
        ]

    def _synthesizing_tools(self) -> list[object]:
        """
        目的：集中组装 `synthesize_and_output` 任务可用的只读综合工具集合。
        功能：给综合阶段提供 registry 读取和审计留痕能力，不允许改写条目。
        实现逻辑：固定返回 `ReadRegistryTool` 和 `RegistryReviewTool`。
        可调参数：当前无显式参数。
        默认参数及原因：默认严格只读，原因是综合阶段只能消费已沉淀证据，不能再偷偷补账。
        """

        return [
            ReadRegistryTool(),
            RegistryReviewTool(),
        ]

    def _build_agent(
        self,
        *,
        config_name: str,
        tools: list[object],
        temperature: float | None = None,
        allow_delegation: bool = False,
    ) -> Agent:
        """
        目的：统一构建当前专题使用的各类 Agent。
        功能：把 YAML 配置、模型参数和通用运行约束组装成 `Agent` 实例。
        实现逻辑：读取对应 agent 配置后，按统一的 LLM 与运行参数返回 Agent。
        可调参数：`config_name`、`tools`、`temperature` 和 `allow_delegation`。
        默认参数及原因：默认关闭 delegation，原因是 research 子 crew 改为顺序执行后，各 worker 只负责自己的任务边界。
        """

        return Agent(
            config=self.agents_config[config_name],  # type: ignore[index]
            tools=tools,
            llm=get_heavy_llm(temperature=temperature if temperature is not None else self.default_temperature),
            function_calling_llm=None,
            max_iter=20,
            max_rpm=None,
            max_execution_time=None,
            verbose=True,
            allow_delegation=allow_delegation,
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
    def search_fact_agent(self) -> Agent:
        """
        目的：定义外部搜索候选采集与落账复用的 search agent。
        功能：同一 agent 同时承担搜索候选采集和 search 结果的 registry 落账任务。
        实现逻辑：agent 本身不挂载工具，实际能力完全由 task 级工具集控制。
        可调参数：YAML agent 配置和模型温度。
        默认参数及原因：默认 `temperature=0.15`，原因是搜索阶段需要兼顾稳定性和适度发散。
        """

        return self._build_agent(config_name="search_fact_agent", tools=[], temperature=0.15)

    @agent
    def extract_file_fact_agent(self) -> Agent:
        """
        目的：定义 PDF 候选采集与落账复用的 extract agent。
        功能：同一 agent 同时承担原文候选采集和 extract 结果的 registry 落账任务。
        实现逻辑：agent 本身不挂载工具，实际能力完全由 task 级工具集控制。
        可调参数：YAML agent 配置和模型温度。
        默认参数及原因：默认 `temperature=0.1`，原因是原文提取优先追求稳定取证。
        """

        return self._build_agent(config_name="extract_file_fact_agent", tools=[], temperature=0.1)

    @agent
    def qa_check_agent(self) -> Agent:
        """
        目的：定义专题内部 QA agent。
        功能：检查本 pack 的 registry 覆盖度并写入返工留痕。
        实现逻辑：agent 本身不挂载工具，实际能力完全由 `check_registry` 任务控制。
        可调参数：YAML agent 配置和模型温度。
        默认参数及原因：默认 `temperature=0.1`，原因是 QA 判断应更保守稳定。
        """

        return self._build_agent(config_name="qa_check_agent", tools=[], temperature=0.1)

    @agent
    def synthesizing_agent(self) -> Agent:
        """
        目的：定义专题最终综合输出 agent。
        功能：把已沉淀的事实、数据、判断和冲突整理成 Markdown 分析包。
        实现逻辑：agent 本身不挂载工具，实际能力完全由只读综合 task 控制。
        可调参数：YAML agent 配置和模型温度。
        默认参数及原因：默认 `temperature=0.2`，原因是综合输出需要收束但仍保留一定表达弹性。
        """

        return self._build_agent(config_name="synthesizing_agent", tools=[], temperature=0.2)

    @task
    def extract_file_facts(self) -> Task:
        """
        目的：定义专题的 PDF 候选采集任务。
        功能：驱动 extract agent 回到 PDF 取证，并产出候选 patch 报告。
        实现逻辑：读取 `tasks.yaml` 中的 `extract_file_facts` 配置，并挂载 PDF 读取类 task 工具。
        可调参数：YAML task 配置。
        默认参数及原因：默认不开结构化 JSON 输出，原因是任务主要通过候选 patch 文本交接给下游 record task。
        """

        return Task(
            config=strip_research_task_profile_fields(self.tasks_config["extract_file_facts"]),  # type: ignore[index]
            tools=self._extract_tools(),
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=False,
        )

    @task
    def record_extract_registry(self) -> Task:
        """
        目的：定义专题的 extract 结果登记与修订任务。
        功能：把 `extract_file_facts` 产出的候选 patch 报告优先回填到已有 registry 条目。
        实现逻辑：复用 extract agent，并把 `extract_file_facts` 作为唯一上游上下文输入。
        可调参数：YAML task 配置。
        默认参数及原因：默认不开 Markdown 输出，原因是该任务面向 registry 落账而不是正文成文。
        """

        return Task(
            config=strip_research_task_profile_fields(self.tasks_config["record_extract_registry"]),  # type: ignore[index]
            context=[self.extract_file_facts()],
            tools=self._registry_recording_tools(),
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=False,
        )

    @task
    def search_facts(self) -> Task:
        """
        目的：定义专题的外部搜索候选采集任务。
        功能：驱动 search agent 补足公开资料并产出候选 patch 报告。
        实现逻辑：读取 `tasks.yaml` 中的 `search_facts` 配置，并依赖 `record_extract_registry` 已完成的落账结果。
        可调参数：YAML task 配置。
        默认参数及原因：默认不开结构化 JSON 输出，原因是任务主要通过候选 patch 文本交接给下游 record task。
        """

        return Task(
            config=strip_research_task_profile_fields(self.tasks_config["search_facts"]),  # type: ignore[index]
            context=[self.record_extract_registry()],
            tools=self._search_tools(),
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=False,
        )

    @task
    def record_search_registry(self) -> Task:
        """
        目的：定义专题的 search 结果登记与修订任务。
        功能：把 `search_facts` 产出的候选 patch 报告优先回填到已有 registry 条目。
        实现逻辑：复用 search agent，并把 `search_facts` 作为唯一上游上下文输入。
        可调参数：YAML task 配置。
        默认参数及原因：默认不开 Markdown 输出，原因是该任务面向 registry 落账而不是正文成文。
        """

        return Task(
            config=strip_research_task_profile_fields(self.tasks_config["record_search_registry"]),  # type: ignore[index]
            context=[self.search_facts()],
            tools=self._registry_recording_tools(),
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=False,
        )

    @task
    def check_registry(self) -> Task:
        """
        目的：定义专题的内部 QA 任务。
        功能：检查两个 record 任务完成后的 registry 覆盖度和未关闭缺口。
        实现逻辑：复用 `tasks.yaml` 配置，并把两个落账任务作为上游上下文传入。
        可调参数：YAML task 配置和上下文依赖。
        默认参数及原因：默认依赖两个 record 任务，原因是 QA 必须基于已落账状态检查闭环。
        """

        return Task(
            config=strip_research_task_profile_fields(self.tasks_config["check_registry"]),  # type: ignore[index]
            context=[self.record_extract_registry(), self.record_search_registry()],
            tools=self._qa_tools(),
            async_execution=False,
            output_json=None,
            output_pydantic=ResearchRegistryCheckResult,
            human_input=False,
            cache=True,
            markdown=False,
        )

    @task
    def synthesize_and_output(self) -> Task:
        """
        目的：定义专题的最终只读综合输出任务。
        功能：驱动 synthesizing agent 输出最终 Markdown 分析包。
        实现逻辑：复用 `tasks.yaml` 配置，并把两个 record 结果和 QA 结果作为上游上下文传入。
        可调参数：YAML task 配置和上下文依赖。
        默认参数及原因：默认开启 Markdown 输出，原因是该任务直接产出下游复用的分析包文件。
        """

        return Task(
            config=strip_research_task_profile_fields(self.tasks_config["synthesize_and_output"]),  # type: ignore[index]
            context=[self.record_extract_registry(), self.record_search_registry(), self.check_registry()],
            tools=self._synthesizing_tools(),
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
        目的：输出专题最终使用的 research crew。
        功能：汇总 4 个 worker agent 和 6 个 task，并交给 CrewAI 以 sequential process 运行。
        实现逻辑：先确保日志目录存在，再返回按固定 task 顺序执行的 `Crew` 实例。
        可调参数：日志路径、缓存、tracing 和固定 task 顺序。
        默认参数及原因：默认采用 `Process.sequential`，原因是当前 research 子 crew 的顺序和上下文依赖已经显式写死。
        """

        if isinstance(self.output_log_file_path, str):
            Path(self.output_log_file_path).parent.mkdir(parents=True, exist_ok=True)
        return Crew(
            name=self.crew_name,
            agents=[
                self.extract_file_fact_agent(),
                self.search_fact_agent(),
                self.qa_check_agent(),
                self.synthesizing_agent(),
            ],
            tasks=[
                self.extract_file_facts(),
                self.record_extract_registry(),
                self.search_facts(),
                self.record_search_registry(),
                self.check_registry(),
                self.synthesize_and_output(),
            ],
            process=Process.sequential,
            verbose=True,
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
        )
