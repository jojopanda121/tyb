# 设计目的：对外暴露 flow 层稳定入口，同时避免包级循环导入。
# 模块功能：按需导出 `ResearchReportFlow`。
# 实现逻辑：通过 `__getattr__` 懒加载实际 flow 实现模块，减少导入时副作用。
# 可调参数：无。
# 默认参数及原因：默认只导出 `ResearchReportFlow`，原因是当前 flow 层对外公共入口只有这一项。

from importlib import import_module
from typing import Any

__all__ = ["ResearchReportFlow"]


def __getattr__(name: str) -> Any:
    """
    设计目的：把 flow 公共对象的导入延迟到真正访问时再执行。
    模块功能：按名称动态导入 `research_flow` 并返回目标对象。
    实现逻辑：仅当访问 `ResearchReportFlow` 时才导入实际模块。
    可调参数：`name`。
    默认参数及原因：未知名称抛出 `AttributeError`，原因是遵守 Python 模块属性访问约定。
    """

    if name != "ResearchReportFlow":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module("automated_research_report_generator.flow.research_flow")
    value = getattr(module, name)
    globals()[name] = value
    return value
