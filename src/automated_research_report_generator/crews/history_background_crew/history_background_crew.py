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
        目的：集中组装 search_fact_agent 的工具集。
        功能：把搜索、registry 写入和额外工具按固定顺序注入给外部搜索 agent。
        实现逻辑：先按需加搜索工具，再追加 registry 和专题扩展工具。
        可调参数：`use_search_tool` 和 `extra_tool_factories`。
        默认参数及原因：默认启用 `SerperDevTool`，原因是历史专题仍需要外部背景交叉验证。
        """

        tools: list[object] = []
        if self.use_search_tool:
            tools.append(SerperDevTool())
        tools.extend([ReadRegistryTool(), AddEntryTool(), UpdateEntryTool(), AddEvidenceTool(), StatusUpdateTool(), RegistryReviewTool()])
        tools.extend(self._extra_tools())
        return tools

    def _extract_tools(self) -> list[object]:
        """
        目的：集中组装 extract_file_fact_agent 的工具集。
        功能：把 PDF 读取、registry 写入和额外工具按固定顺序注入给原文提取 agent。
        实现逻辑：先放页索引和页内容工具，再追加 registry 和专题扩展工具。
        可调参数：`extra_tool_factories`。
        默认参数及原因：默认总是注入 PDF 工具，原因是历史专题必须回到原始披露材料取证。
        """

        tools: list[object] = [
            shared_pdf_page_index_tool,
            shared_pdf_page_reader_tool,
            ReadRegistryTool(),
            AddEntryTool(),
            UpdateEntryTool(),
            AddEvidenceTool(),
            StatusUpdateTool(),
            RegistryReviewTool(),
        ]
        tools.extend(self._extra_tools())
        return tools

    def _qa_tools(self) -> list[object]:
        """
        目的：集中组装 qa_check_agent 的工具集。
        功能：给内部 QA agent 提供最小可用的 registry 检查与状态回写能力。
        实现逻辑：固定返回账本读取、状态更新和 review 留痕工具。
        可调参数：当前无显式参数。
        默认参数及原因：默认不允许新增 evidence，原因是内部 QA 只负责查漏和留痕。
        """

        return [ReadRegistryTool(), StatusUpdateTool(), RegistryReviewTool()]

    def _synthesizing_tools(self) -> list[object]:
        """
        目的：集中组装 synthesizing_agent 的工具集。
        功能：给综合 agent 提供只读账本和收尾留痕能力。
        实现逻辑：固定返回账本读取、review 留痕和状态工具。
        可调参数：当前无显式参数。
        默认参数及原因：默认不提供新增证据能力，原因是综合阶段应只基于已沉淀账本输出。
        """

        return [ReadRegistryTool(), UpdateEntryTool(), RegistryReviewTool(), StatusUpdateTool()]

    def _build_agent(
        self,
        *,
        config_name: str,
        tools: list[object],
        temperature: float | None = None,
        allow_delegation: bool = False,
    ) -> Agent:
        """
        目的：统一构建历史专题各类 agent。
        功能：把 YAML 配置、工具、模型和通用运行参数组合成 `Agent` 实例。
        实现逻辑：读取对应 agent 配置后，套用当前专题共用的运行约束。
        可调参数：`config_name`、工具列表、可选 `temperature` 和 delegation 开关。
        默认参数及原因：默认关闭 delegation，原因是只有 custom manager 负责跨 agent 调度。
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
    def manager_agent(self) -> Agent:
        """
        目的：定义历史专题的层级调度 manager agent。
        功能：只通过委派和追问能力，按既定顺序调度提取、搜索、检查和综合任务。
        实现逻辑：使用独立 manager 配置，不挂载任何业务工具，并显式开启 delegation。
        可调参数：YAML agent 配置和 manager 温度。
        默认参数及原因：默认 `temperature=0.1`，原因是调度判断应稳定收敛，不应发散分析。
        """

        return self._build_agent(
            config_name="manager_agent",
            tools=[],
            temperature=0.1,
            allow_delegation=True,
        )

    @agent
    def search_fact_agent(self) -> Agent:
        """
        目的：定义历史专题的外部搜索 agent。
        功能：补充背景、治理和关键里程碑相关公开资料，并回写 registry。
        实现逻辑：使用历史专题的搜索工具集与通用 research 运行参数构建 Agent。
        可调参数：YAML agent 配置、搜索工具和模型温度。
        默认参数及原因：默认沿用专题基础温度，原因是历史搜索仍需兼顾稳定和补充性。
        """

        return self._build_agent(config_name="search_fact_agent", tools=self._search_tools(), temperature=0.15)

    @agent
    def extract_file_fact_agent(self) -> Agent:
        """
        目的：定义历史专题的原文提取 agent。
        功能：回到 PDF 原文提取治理结构、股东关系和关键时间线，并回写 registry。
        实现逻辑：使用 PDF + registry 工具集与通用 research 运行参数构建 Agent。
        可调参数：YAML agent 配置、PDF 工具和模型温度。
        默认参数及原因：默认沿用专题基础温度，原因是历史原文提取以稳定取证为先。
        """

        return self._build_agent(config_name="extract_file_fact_agent", tools=self._extract_tools(), temperature=0.1)

    @agent
    def qa_check_agent(self) -> Agent:
        """
        目的：定义历史专题的内部 QA agent。
        功能：检查当前 history pack 的 registry 覆盖度，并补 revision_detail 与 review 留痕。
        实现逻辑：使用最小账本工具集，并把温度压低到更保守的水平。
        可调参数：YAML agent 配置、内部 QA 工具和模型温度。
        默认参数及原因：默认 `temperature=0.1`，原因是内部 QA 应优先保证判断稳定。
        """

        return self._build_agent(config_name="qa_check_agent", tools=self._qa_tools(), temperature=0.1)

    @agent
    def synthesizing_agent(self) -> Agent:
        """
        目的：定义历史专题的综合输出 agent。
        功能：把已沉淀的历史事实、判断和冲突整理成 Markdown 分析包。
        实现逻辑：使用只读账本工具集，并以略低温度控制综合输出的收束程度。
        可调参数：YAML agent 配置、综合工具和模型温度。
        默认参数及原因：默认 `temperature=0.15`，原因是综合输出要收束但仍需一定表达弹性。
        """

        return self._build_agent(config_name="synthesizing_agent", tools=self._synthesizing_tools(), temperature=0.2)

    @task
    def search_facts(self) -> Task:
        """
        目的：定义历史专题的外部搜索任务。
        功能：驱动搜索 agent 补足背景、治理与关键时间线相关公开资料。
        实现逻辑：直接使用本专题 `tasks.yaml` 中的 `search_facts` 配置创建任务。
        可调参数：YAML task 配置。
        默认参数及原因：默认不开结构化 JSON 输出，原因是本任务主要依赖 registry 副作用。
        """

        return Task(config=strip_research_task_profile_fields(self.tasks_config["search_facts"]), tools=[], async_execution=False, output_json=None, output_pydantic=None, human_input=False, cache=True, markdown=False)  # type: ignore[index]

    @task
    def extract_file_facts(self) -> Task:
        """
        目的：定义历史专题的原文提取任务。
        功能：驱动原文提取 agent 回到 PDF 取证并回写 registry。
        实现逻辑：直接使用本专题 `tasks.yaml` 中的 `extract_file_facts` 配置创建任务。
        可调参数：YAML task 配置。
        默认参数及原因：默认不开结构化 JSON 输出，原因是本任务主要依赖 registry 副作用。
        """

        return Task(config=strip_research_task_profile_fields(self.tasks_config["extract_file_facts"]), tools=[], async_execution=False, output_json=None, output_pydantic=None, human_input=False, cache=True, markdown=False)  # type: ignore[index]

    @task
    def check_registry(self) -> Task:
        """
        目的：定义历史专题的内部 QA 任务。
        功能：驱动 QA agent 检查 history pack 的账本闭环情况。
        实现逻辑：复用 YAML 配置，并把搜索与提取任务作为上游上下文传入。
        可调参数：YAML task 配置和上下文依赖。
        默认参数及原因：默认依赖前两步任务，原因是内部 QA 需要基于本轮已完成的补证结果检查。
        """

        return Task(config=strip_research_task_profile_fields(self.tasks_config["check_registry"]), context=[self.extract_file_facts(), self.search_facts()], tools=[], async_execution=False, output_json=None, output_pydantic=None, human_input=False, cache=True, markdown=False)  # type: ignore[index]

    @task
    def synthesize_and_output(self) -> Task:
        """
        目的：定义历史专题的最终综合输出任务。
        功能：驱动综合 agent 输出最终 history pack Markdown。
        实现逻辑：复用 YAML 配置，并把搜索、提取和内部 QA 结果作为上下文传入。
        可调参数：YAML task 配置和上下文依赖。
        默认参数及原因：默认开启 Markdown 输出，原因是该任务直接产出下游复用的分析包文件。
        """

        return Task(config=strip_research_task_profile_fields(self.tasks_config["synthesize_and_output"]), context=[self.extract_file_facts(), self.search_facts(), self.check_registry()], tools=[], async_execution=False, output_json=None, output_pydantic=None, human_input=False, cache=True, markdown=True)  # type: ignore[index]

    @crew
    def crew(self) -> Crew:
        """
        目的：输出历史专题最终使用的层级 research crew。
        功能：汇总 4 个 agent 和 4 个 task，交给 CrewAI 以 hierarchical process 运行。
        实现逻辑：先确保日志目录存在，再返回带 custom manager_agent 的 `Crew` 实例。
        可调参数：日志路径、缓存、tracing、manager/chat llm 和固定 task 顺序。
        默认参数及原因：默认采用 `Process.hierarchical`，原因是历史专题仍按设计由 manager 统一调度。
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
                self.search_facts(),
                self.check_registry(),
                self.synthesize_and_output(),
            ],
            process=Process.hierarchical,
            verbose=True,
            manager_llm=None,
            manager_agent=self.manager_agent(),
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
