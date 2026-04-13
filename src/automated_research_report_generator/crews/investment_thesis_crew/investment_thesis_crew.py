from __future__ import annotations

from pathlib import Path
from typing import List

from crewai import Agent, Crew, Process, Task
from crewai.agents.agent_builder.base_agent import BaseAgent
from crewai.project import CrewBase, agent, crew, task

from automated_research_report_generator.flow.common import PROJECT_ROOT
from automated_research_report_generator.llm_config import get_heavy_llm
from automated_research_report_generator.tools import (
    ReadRegistryTool,
)
from automated_research_report_generator.tools.pdf_page_tools import (
    ReadPdfPageIndexTool,
    ReadPdfPagesTool,
)

# 设计目的：把 investment thesis crew 保留为整个项目里的特例，在这个 crew 里允许模型显式思考，因为综合投资判断和管理层尽调问题设计都需要更深一层的推理。
# 模块功能：提供 thesis 综合和尽调问题设计两个角色，先综合结论，再形成只读权限下的尽调问题。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：`INVESTMENT_THESIS_AGENT_TEMPERATURE`、`INVESTMENT_THESIS_AGENT_REASONING` 和 `output_log_file_path`。
# 默认参数及原因：默认 `temperature=0.5` 且 `reasoning=False`，原因是当前 CrewAI 1.14.1 在 thesis reasoning 上存在兼容噪音，先以稳定产出为主。

PROJECT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CREW_LOG_FILE = str(PROJECT_LOG_DIR / "investment_thesis_crew.json")
INVESTMENT_THESIS_AGENT_TEMPERATURE = 0.5
INVESTMENT_THESIS_AGENT_REASONING = False

# 设计目的：让 thesis 任务和 diligence 任务共用同一套 PDF 读取视图，避免引用页码口径不一致。
# 模块功能：提供页码索引与页面正文读取。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：后续如果 PDF 工具实现变化，可在这里统一替换。
# 默认参数及原因：当前直接使用默认构造，因为项目已经把常用行为封装在工具类里。
shared_pdf_page_index_tool = ReadPdfPageIndexTool()
shared_pdf_page_reader_tool = ReadPdfPagesTool()


@CrewBase
class InvestmentThesisCrew:
    """
    设计目的：把投资结论综合和尽调问题设计放在同一个 thesis 阶段，保持最终判断口径一致。
    模块功能：集中声明 thesis 综合角色、尽调问题角色、两步任务和该阶段特例参数。
    实现逻辑：先汇总前面各包形成投资主线，再在同一套结论基础上整理尽调问题清单。
    可调参数：YAML 配置、日志路径、temperature、reasoning 开关和工具权限边界。
    默认参数及原因：默认保留较高温度和 reasoning，原因是这一阶段需要把前面材料压成最终判断，但仍要由权限边界限制可写范围。
    """

    agents: List[BaseAgent]
    tasks: List[Task]

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"
    output_log_file_path: str | bool | None = DEFAULT_CREW_LOG_FILE

    # 设计目的：主 thesis agent 只消费前序阶段已经沉淀好的 registry 判断，不再反向改写账本。
    # 模块功能：构造 investment_synthesizer，并把温度和 reasoning 例外集中在常量里管理。
    # 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    # 可调参数：INVESTMENT_THESIS_AGENT_TEMPERATURE 和 INVESTMENT_THESIS_AGENT_REASONING。
    # 默认参数及原因：使用 0.5 和 False，原因是先绕开 CrewAI 1.14.1 的 thesis reasoning 兼容噪音。
    @agent
    def investment_synthesizer(self) -> Agent:
        """
        设计目的：集中定义主 thesis 综合角色。
        模块功能：让该角色只读 registry，并综合各研究包形成投资主线。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：agent 配置、工具列表、temperature 和 reasoning。
        默认参数及原因：默认使用 0.5 和 False，原因是先保证 thesis 阶段稳定执行，再观察是否需要重新打开 reasoning。
        """

        return Agent(
            config=self.agents_config["investment_synthesizer"],  # type: ignore[index]
            tools=[
                shared_pdf_page_index_tool,
                shared_pdf_page_reader_tool,
                ReadRegistryTool(),
            ],
            llm=get_heavy_llm(temperature=INVESTMENT_THESIS_AGENT_TEMPERATURE),
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
            reasoning=INVESTMENT_THESIS_AGENT_REASONING,
            max_reasoning_attempts=None,
            inject_date=True,
        )

    # 设计目的：把 diligence questions 绑定到只读 registry 权限，避免该任务改写证据账本。
    # 模块功能：构造 diligence_question_designer，并只注入 ReadRegistryTool。
    # 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    # 可调参数：INVESTMENT_THESIS_AGENT_TEMPERATURE 和 INVESTMENT_THESIS_AGENT_REASONING。
    # 默认参数及原因：仍然使用 0.5 和 False，原因是该 agent 与 thesis 主 agent 共用同一条稳定性策略，但权限边界仍保持只读。
    @agent
    def diligence_question_designer(self) -> Agent:
        """
        设计目的：集中定义尽调问题设计角色。
        模块功能：让该角色在只读 registry 权限下整理管理层提问清单。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：agent 配置、工具列表、temperature 和 reasoning。
        默认参数及原因：默认继续使用 thesis crew 的思考参数，但权限边界保持只读。
        """
        return Agent(
            config=self.agents_config["diligence_question_designer"],  # type: ignore[index]
            tools=[
                shared_pdf_page_index_tool,
                shared_pdf_page_reader_tool,
                ReadRegistryTool(),
            ],
            llm=get_heavy_llm(temperature=INVESTMENT_THESIS_AGENT_TEMPERATURE),
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
            reasoning=INVESTMENT_THESIS_AGENT_REASONING,
            max_reasoning_attempts=None,
            inject_date=True,
        )

    @task
    def synthesize_investment_case(self) -> Task:
        """
        设计目的：定义投资论述综合任务。
        模块功能：创建任务。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：任务配置和输出格式。
        默认参数及原因：默认输出 markdown，原因是投资主线需要直接进入最终报告。
        """
        return Task(
            config=self.tasks_config["synthesize_investment_case"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def draft_diligence_questions(self) -> Task:
        """
        设计目的：定义管理层尽调问题设计任务。
        模块功能：绑定只读 agent，并把 thesis 结果作为次级上下文。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：任务配置、agent 绑定和 context 列表。
        默认参数及原因：默认使用只读 agent，原因是该任务不能改写证据账本。
        """
        return Task(
            config=self.tasks_config["draft_diligence_questions"],  # type: ignore[index]
            agent=self.diligence_question_designer(),
            context=[self.synthesize_investment_case()],
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
        设计目的：统一返回 thesis 阶段使用的 Crew 实例。
        模块功能：确保日志目录存在，并构造顺序执行的 investment_thesis_crew。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：日志路径、缓存、tracing 和 `chat_llm`。
        默认参数及原因：默认采用 `Process.sequential`，原因是先形成 thesis，再整理尽调问题。
        """
        if isinstance(self.output_log_file_path, str):
            Path(self.output_log_file_path).parent.mkdir(parents=True, exist_ok=True)
        return Crew(
            name="investment_thesis_crew",
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
