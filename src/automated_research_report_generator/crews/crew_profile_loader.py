from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# 设计目的：把 research sub-crew 的专题元数据统一从现有 tasks.yaml 里提取出来。
# 模块功能：提供“读取专题资料”和“剔除任务元数据字段”两个最小 helper。
# 实现逻辑：按固定任务块读取任务配置里的扩展字段，并在传给 CrewAI 前移除这些仅供项目内部使用的字段。
# 可调参数：任务块名称和元数据键集合。
# 默认参数及原因：固定使用 research sub-crew 当前四个任务块，原因是这些字段本来就服务于这四段 prompt。

_PROFILE_KEYS_BY_TASK: dict[str, tuple[str, ...]] = {
    "search_facts": (
        "crew_name",
        "pack_name",
        "pack_title",
        "pack_focus",
        "output_title",
        "search_guidance",
    ),
    "extract_file_facts": ("extract_guidance",),
    "check_registry": ("qa_guidance",),
    "synthesize_and_output": ("synthesize_guidance", "output_skeleton"),
}
_PROFILE_METADATA_KEYS = {
    key
    for keys in _PROFILE_KEYS_BY_TASK.values()
    for key in keys
}


def load_research_task_profile(module_file: str) -> dict[str, str]:
    """
    目的：从 research sub-crew 的 tasks.yaml 里读取专题元数据。
    功能：把 crew 名、pack 名、各段 guidance 和 output skeleton 组装成统一字典。
    实现逻辑：定位到与 crew.py 同目录的 `config/tasks.yaml`，再按固定任务块读取扩展字段。
    可调参数：`module_file`，用于定位当前 crew.py 所在目录。
    默认参数及原因：固定从现有四个任务块取值，原因是这样不用改 CrewAI 对顶层 task 结构的假设。
    """

    config_path = Path(module_file).resolve().parent / "config" / "tasks.yaml"
    task_config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    profile: dict[str, str] = {}
    missing_keys: list[str] = []

    for task_name, keys in _PROFILE_KEYS_BY_TASK.items():
        task_payload = task_config.get(task_name)
        if not isinstance(task_payload, dict):
            missing_keys.extend(keys)
            continue
        for key in keys:
            value = task_payload.get(key)
            if value is None:
                missing_keys.append(key)
                continue
            profile[key] = str(value)

    if missing_keys:
        missing_display = ", ".join(sorted(set(missing_keys)))
        raise ValueError(f"Missing crew profile keys in {config_path.as_posix()}: {missing_display}")

    return profile


def strip_research_task_profile_fields(task_config: dict[str, Any]) -> dict[str, Any]:
    """
    目的：在不丢失 tasks.yaml 维护体验的前提下兼容 CrewAI 的任务配置读取。
    功能：移除只给项目内部使用的专题元数据键，返回可直接传给 `Task(config=...)` 的字典。
    实现逻辑：先复制原始任务配置，再逐个弹出 profile 扩展字段。
    可调参数：`task_config`。
    默认参数及原因：默认只剔除项目自定义的元数据字段，原因是其他标准任务字段仍要完整保留给 CrewAI。
    """

    sanitized_config = dict(task_config)
    for key in _PROFILE_METADATA_KEYS:
        sanitized_config.pop(key, None)
    return sanitized_config
