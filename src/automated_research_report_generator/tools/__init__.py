# 设计目的：统一导出当前流程会直接引用的工具，同时避免包级循环导入。
# 模块功能：对外暴露 registry、财务建模、估值、Tushare 和 Markdown 转 PDF 等工具名。
# 实现逻辑：通过 `__getattr__` 懒加载实际工具模块，只有真正访问到名称时才执行对应导入。
# 可调参数：本模块本身没有运行时参数，实际参数由各工具类内部定义。
# 默认参数及原因：默认只导出当前流程已稳定使用的工具，原因是保持对外接口清晰并降低循环依赖风险。

from importlib import import_module
from typing import Any

_TOOL_IMPORTS = {
    "AddEntryTool": ("automated_research_report_generator.tools.registry_tools", "AddEntryTool"),
    "AddEvidenceTool": ("automated_research_report_generator.tools.registry_tools", "AddEvidenceTool"),
    "ReadRegistryTool": ("automated_research_report_generator.tools.registry_tools", "ReadRegistryTool"),
    "RegistryReviewTool": ("automated_research_report_generator.tools.registry_tools", "RegistryReviewTool"),
    "StatusUpdateTool": ("automated_research_report_generator.tools.registry_tools", "StatusUpdateTool"),
    "UpdateEntryTool": ("automated_research_report_generator.tools.registry_tools", "UpdateEntryTool"),
    "set_evidence_registry_context": (
        "automated_research_report_generator.tools.registry_tools",
        "set_evidence_registry_context",
    ),
    "FinancialModelTool": (
        "automated_research_report_generator.tools.financial_model_tool",
        "FinancialModelTool",
    ),
    "MarkdownToPdfTool": (
        "automated_research_report_generator.tools.markdown_to_pdf_tool",
        "MarkdownToPdfTool",
    ),
    "ComparableValuationTool": (
        "automated_research_report_generator.tools.valuation_tools",
        "ComparableValuationTool",
    ),
    "IntrinsicValuationTool": (
        "automated_research_report_generator.tools.valuation_tools",
        "IntrinsicValuationTool",
    ),
    "FootballFieldTool": (
        "automated_research_report_generator.tools.valuation_tools",
        "FootballFieldTool",
    ),
    "TushareValuationDataTool": (
        "automated_research_report_generator.tools.tushare_tools",
        "TushareValuationDataTool",
    ),
}

__all__ = list(_TOOL_IMPORTS.keys())


def __getattr__(name: str) -> Any:
    """
    设计目的：把工具导入延迟到真正访问时再执行。
    模块功能：按工具名动态导入对应模块并返回目标对象。
    实现逻辑：先查懒加载映射，再用 `import_module` 导入目标模块并取出属性。
    可调参数：`name`。
    默认参数及原因：未知名称抛出 `AttributeError`，原因是遵守 Python 模块属性访问约定。
    """

    if name not in _TOOL_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attribute_name = _TOOL_IMPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attribute_name)
    globals()[name] = value
    return value
