import os
from pathlib import Path
from dotenv import load_dotenv
from crewai import Agent, Crew, LLM, Process, Task
from crewai.project import CrewBase, agent, crew, task
from crewai_tools import SerperDevTool
from automated_research_report_generator_v0_1.tools.MarkdownToPdfTool import (
    MarkdownToPdfTool,
)
from automated_research_report_generator_v0_1.tools.pdf_page_tools import (
    ReadPdfPageIndexTool,
    ReadPdfPagesTool,
)
load_dotenv()


"""LLM 与 Crew 定义"""
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CREW_LOG_FILE = str(PROJECT_LOG_DIR / "crew_default.json")


def get_llm(  # 设计：统一 LLM 入口；功能：按流程覆写采样与容错；可调：temperature 常用 0-1、timeout>0/None、max_retries>=0/None；默认 0.5/60/1 兼顾稳定与响应速度。
    temperature: float = 0.5,
    timeout: float | int | None = 60,
    max_retries: int | None = 1,
) -> LLM:
    return LLM(
        api_base="https://openrouter.ai/api/v1",
        model="openrouter/google/gemini-3-flash-preview",
        api_key=os.getenv("OPENROUTER_API_KEY"),
        temperature=temperature,
        timeout=timeout,
        max_retries=max_retries,
    )

shared_search_tool = SerperDevTool()  # 设计：复用搜索实例；功能：补公开信息核验；默认全局共享以减少重复初始化。
shared_pdf_page_index_tool = ReadPdfPageIndexTool()  # 设计：统一筛页入口；功能：先读页索引再选页；默认全员共用以固定调用顺序。
shared_pdf_page_reader_tool = ReadPdfPagesTool()  # 设计：统一读页入口；功能：直读指定页文本；默认共用以避免重复实现。

@CrewBase
class AutomatedResearchReportGeneratorV01Crew:  # 设计：集中装配研究报告主流程；功能：把 YAML 配置映射为 agents、tasks 与 crew；默认顺序执行以保证上下游稳定。

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"
    output_log_file_path: str | bool | None = DEFAULT_CREW_LOG_FILE

    @agent
    def industry_research_analyst(self) -> Agent:  # 设计：定义首个标准分析 agent；功能：行业研究并示范通用 agent 配置；默认低温度和禁委派以提高可控性。
        return Agent(
            config=self.agents_config["industry_research_analyst"],  # type: ignore[index]  # 默认读 YAML；可切换同结构配置；原因是角色文案与代码解耦。
            tools=[  # 可调：按任务增减工具；默认索引+读页+搜索，原因是行业研究同时需要文档定位与外部核验。
                shared_pdf_page_index_tool,
                shared_pdf_page_reader_tool,
                shared_search_tool,
            ],
            llm=get_llm(temperature=0.3),  # 可调：temperature 常用 0-1；默认 0.3，原因是分析要稳但仍保留少量归纳弹性。
            function_calling_llm=None,  # 可调：独立工具调用模型/None；默认 None，原因是沿用主模型便于维护。
            max_iter=25,  # 可调：建议 >=1；默认 25，原因是给复杂任务足够迭代空间。
            max_rpm=None,  # 可调：正整数/None；默认 None，原因是先不做人为限流。
            max_execution_time=None,  # 可调：秒数/None；默认 None，原因是交给上层超时控制。
            verbose=True,  # 可调：True/False；默认 True，原因是当前阶段更重视可观测性。
            allow_delegation=False,  # 可调：True/False；默认 False，原因是角色边界已明确。
            step_callback=None,  # 可调：回调函数/None；默认 None，原因是暂不插入额外钩子。
            cache=True,  # 可调：True/False；默认 True，原因是减少重复调用成本。
            allow_code_execution=False,  # 可调：True/False；默认 False，原因是当前任务以阅读分析为主。
            max_retry_limit=2,  # 可调：建议 >=0；默认 2，原因是兼顾重试收益与耗时。
            respect_context_window=True,  # 可调：True/False；默认 True，原因是避免长上下文溢出。
            use_system_prompt=True,  # 可调：True/False；默认 True，原因是保留系统约束。
            reasoning=False,  # 可调：True/False；默认 False，原因是此流程更依赖显式任务拆分。
            max_reasoning_attempts=5,  # 可调：建议 >=1/None；默认 5，原因是与 reasoning 兼容保留上限。
            inject_date=True,  # 可调：True/False；默认 True，原因是搜索与时效判断需要日期上下文。
        )

    @agent
    def business_model_analyst(self) -> Agent:  # 设计：聚焦商业模式；功能：只读 PDF 内部证据；默认不挂搜索以减少外部噪声。
        return Agent(
            config=self.agents_config["business_model_analyst"],  # type: ignore[index]
            tools=[shared_pdf_page_index_tool, shared_pdf_page_reader_tool],
            llm=get_llm(temperature=0.3),
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
    def financial_analysis_specialist(self) -> Agent:  # 设计：聚焦财务分析；功能：从原文抽取并归纳财务结论；默认低温度以压缩主观波动。
        return Agent(
            config=self.agents_config["financial_analysis_specialist"],  # type: ignore[index]
            tools=[shared_pdf_page_index_tool, shared_pdf_page_reader_tool],
            llm=get_llm(temperature=0.3),
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
    def operational_data_analyst(self) -> Agent:  # 设计：聚焦经营指标；功能：提炼运营数据与趋势；默认只用 PDF 工具以保持证据闭环。
        return Agent(
            config=self.agents_config["operational_data_analyst"],  # type: ignore[index]
            tools=[shared_pdf_page_index_tool, shared_pdf_page_reader_tool],
            llm=get_llm(temperature=0.3),
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
    def management_assessment_analyst(self) -> Agent:  # 设计：聚焦管理层评估；功能：结合文档与外部信息交叉判断；默认附带搜索以补公开背景。
        return Agent(
            config=self.agents_config["management_assessment_analyst"],  # type: ignore[index]
            tools=[
                shared_pdf_page_index_tool,
                shared_pdf_page_reader_tool,
                shared_search_tool,
            ],
            llm=get_llm(temperature=0.3),
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
    def risk_assessment_analyst(self) -> Agent:  # 设计：聚焦风险识别；功能：总结主要经营与投资风险；默认保守配置以降低误判。
        return Agent(
            config=self.agents_config["risk_assessment_analyst"],  # type: ignore[index]
            tools=[shared_pdf_page_index_tool, shared_pdf_page_reader_tool],
            llm=get_llm(temperature=0.3),
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
    def strategic_capital_analyst(self) -> Agent:  # 设计：聚焦战略与资本计划；功能：提炼募资、扩产与资本动作；默认只依赖招股书证据。
        return Agent(
            config=self.agents_config["strategic_capital_analyst"],  # type: ignore[index]
            tools=[shared_pdf_page_index_tool, shared_pdf_page_reader_tool],
            llm=get_llm(temperature=0.3),
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
    def comparable_company_identifier(self) -> Agent:  # 设计：筛选可比公司；功能：依赖外部搜索建立可比池；默认较高温度以放宽候选覆盖。
        return Agent(
            config=self.agents_config["comparable_company_identifier"],  # type: ignore[index]
            tools=[shared_search_tool],
            llm=get_llm(temperature=0.5),
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
    def comparable_company_data_extractor(self) -> Agent:  # 设计：抽取可比数据；功能：为估值环节整理公开指标；默认搜索驱动以覆盖多来源。
        return Agent(
            config=self.agents_config["comparable_company_data_extractor"],  # type: ignore[index]
            tools=[shared_search_tool],
            llm=get_llm(temperature=0.3),
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
    def valuation_analysis_specialist(self) -> Agent:  # 设计：聚焦估值分析；功能：基于可比数据形成估值判断；默认温度略高以支持方案比较。
        return Agent(
            config=self.agents_config["valuation_analysis_specialist"],  # type: ignore[index]
            tools=[shared_search_tool],
            llm=get_llm(temperature=0.7),
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
    def investment_highlights_and_risk_scorer(self) -> Agent:  # 设计：统一打分；功能：汇总亮点与风险形成投资结论；默认无工具以强制基于前序产物。
        return Agent(
            config=self.agents_config["investment_highlights_and_risk_scorer"],  # type: ignore[index]
            tools=[],
            llm=get_llm(temperature=0.8),
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
    def management_communication_specialist(self) -> Agent:  # 设计：生成管理层沟通问题；功能：把研究结果转成尽调问法；默认中高温度以扩展问题角度。
        return Agent(
            config=self.agents_config["management_communication_specialist"],  # type: ignore[index]
            tools=[],
            llm=get_llm(temperature=0.7),
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
    def research_report_writer(self) -> Agent:  # 设计：统一成文；功能：把多段分析汇编成报告；默认低温度以保持措辞稳定。
        return Agent(
            config=self.agents_config["research_report_writer"],  # type: ignore[index]
            tools=[],
            llm=get_llm(temperature=0.2),
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
    def qa_consistency_analyst(self) -> Agent:  # 设计：最终质检；功能：检查一致性与遗漏；默认中温度以保留纠错敏感度。
        return Agent(
            config=self.agents_config["qa_consistency_analyst"],  # type: ignore[index]
            tools=[],
            llm=get_llm(temperature=0.7),
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
    def conduct_industry_research_analysis(self) -> Task:  # 设计：定义首个标准任务；功能：示范 YAML 驱动的 task 运行选项；默认同步与 markdown 输出以便衔接后续任务。
        return Task(
            config=self.tasks_config["conduct_industry_research_analysis"],  # type: ignore[index]  # 默认读 YAML；可切换同结构任务配置；原因是任务文本与代码解耦。
            tools=[],  # 可调：按任务补充工具；默认空，原因是工具已挂在 agent 侧统一管理。
            async_execution=False,  # 可调：True/False；默认 False，原因是当前流程强依赖顺序上下文。
            output_json=None,  # 可调：Pydantic/None；默认 None，原因是该任务先输出自然语言 markdown。
            output_pydantic=None,  # 可调：Pydantic/None；默认 None，原因是暂不要求强结构化。
            human_input=False,  # 可调：True/False；默认 False，原因是主流程先保持全自动。
            cache=True,  # 可调：True/False；默认 True，原因是减少重复执行成本。
            markdown=True,  # 可调：True/False；默认 True，原因是后续汇编报告更方便。
        )

    @task
    def analyze_business_model(self) -> Task:  # 设计：商业模式任务；功能：输出商业模式分析；默认沿用通用任务配置以统一产物格式。
        return Task(
            config=self.tasks_config["analyze_business_model"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def analyze_management(self) -> Task:  # 设计：管理层任务；功能：输出管理层评估；默认同步执行以等待前序材料。
        return Task(
            config=self.tasks_config["analyze_management"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def conduct_financial_analysis(self) -> Task:  # 设计：财务任务；功能：输出财务分析；默认 markdown 便于后续引用。
        return Task(
            config=self.tasks_config["conduct_financial_analysis"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def analyze_operational_metrics(self) -> Task:  # 设计：经营指标任务；功能：输出运营分析；默认缓存开启以减少重复运行。
        return Task(
            config=self.tasks_config["analyze_operational_metrics"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def assess_investment_risks(self) -> Task:  # 设计：风险任务；功能：输出投资风险评估；默认同步保证依赖完整。
        return Task(
            config=self.tasks_config["assess_investment_risks"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def analyze_strategic_capital_plans(self) -> Task:  # 设计：战略资本任务；功能：输出资本计划分析；默认通用配置保持一致。
        return Task(
            config=self.tasks_config["analyze_strategic_capital_plans"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def identify_comparable_companies(self) -> Task:  # 设计：可比公司筛选任务；功能：建立可比池；默认 markdown 方便人工复核。
        return Task(
            config=self.tasks_config["identify_comparable_companies"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def extract_comparable_company_data(self) -> Task:  # 设计：可比数据任务；功能：抽取估值所需数据；默认顺序执行以承接可比池。
        return Task(
            config=self.tasks_config["extract_comparable_company_data"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def perform_comparable_valuation_analysis(self) -> Task:  # 设计：估值任务；功能：形成可比估值结果；默认缓存以便调试复跑。
        return Task(
            config=self.tasks_config["perform_comparable_valuation_analysis"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def investment_analysis_and_scoring(self) -> Task:  # 设计：打分任务；功能：汇总前序结论形成投资评分；默认 markdown 便于写入总报告。
        return Task(
            config=self.tasks_config["investment_analysis_and_scoring"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def identify_management_communication_questions(self) -> Task:  # 设计：沟通问题任务；功能：输出管理层问询清单；默认全自动生成初稿。
        return Task(
            config=self.tasks_config["identify_management_communication_questions"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def final_quality_review(self) -> Task:  # 设计：质检任务；功能：做最终一致性复核；默认同步执行以站在末端总览。
        return Task(
            config=self.tasks_config["final_quality_review"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def compile_research_report(self) -> Task:  # 设计：汇编任务；功能：整合各段分析为完整报告；默认 markdown 输出供 PDF 转换。
        return Task(
            config=self.tasks_config["compile_research_report"],  # type: ignore[index]
            tools=[],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=True,
        )

    @task
    def convert_research_report_md_to_pdf(self) -> Task:  # 设计：收口任务；功能：把 Markdown 报告转成 PDF；默认关闭 markdown 因为结果是文件产物。
        return Task(
            config=self.tasks_config["convert_research_report_md_to_pdf"],  # type: ignore[index]
            tools=[MarkdownToPdfTool(result_as_answer=True)],
            async_execution=False,
            output_json=None,
            output_pydantic=None,
            human_input=False,
            cache=True,
            markdown=False,
        )

    @crew
    def crew(self) -> Crew:  # 设计：组装主 crew；功能：串联全部 agents 与 tasks；默认顺序流程和开启 trace，原因是链路依赖强且当前重视可观测性。
        if isinstance(self.output_log_file_path, str):
            Path(self.output_log_file_path).parent.mkdir(parents=True, exist_ok=True)
        return Crew(
            name="automated_research_report_generator_v0.1",  # 可调：任意唯一名称；默认与项目同名，原因是日志更易定位。
            agents=self.agents,  # 默认收集全部 @agent；通常无需手改，原因是由 CrewBase 自动装配。
            tasks=self.tasks,  # 默认收集全部 @task；通常无需手改，原因是保持定义顺序即执行顺序。
            process=Process.sequential,  # 可调：sequential/hierarchical；默认 sequential，原因是任务依赖链明确。
            verbose=True,  # 可调：True/False；默认 True，原因是当前阶段更重视调试信息。
            manager_llm=None,  # 可调：层级流程时指定；默认 None，原因是当前不走 manager 模式。
            manager_agent=None,  # 可调：自定义 manager/None；默认 None，原因是顺序流程不需要。
            function_calling_llm=None,  # 可调：独立工具调用模型/None；默认 None，原因是复用主模型即可。
            config=None,  # 可调：额外 crew 配置/None；默认 None，原因是当前参数已显式写出。
            max_rpm=None,  # 可调：正整数/None；默认 None，原因是先不做全局限流。
            memory=False,  # 可调：True/False；默认 False，原因是避免旧运行污染当前报告。
            cache=True,  # 可调：True/False；默认 True，原因是降低重复执行成本。
            embedder=None,  # 可调：嵌入器配置/None；默认 None，原因是当前未启用 memory/knowledge。
            full_output=False,  # 可调：True/False；默认 False，原因是先收敛返回体体积。
            step_callback=None,  # 可调：回调函数/None；默认 None，原因是暂不加自定义钩子。
            task_callback=None,  # 可调：回调函数/None；默认 None，原因是日志已够用。
            share_crew=False,  # 可调：True/False；默认 False，原因是避免跨会话共享状态。
            output_log_file=self.output_log_file_path,  # 可调：True/路径/False；默认使用入口注入路径，原因是便于按次归档日志。
            planning=False,  # 可调：True/False；默认 False，原因是任务已人工拆分完毕。
            planning_llm=None,  # 可调：规划模型/None；默认 None，原因是未启用 planning。
            trace=True,  # 可调：True/False；默认 True，原因是需要执行链路追踪。
            chat_llm=get_llm(),  # 可调：任意 LLM；默认项目主模型，原因是交互场景保持一致。
        )
