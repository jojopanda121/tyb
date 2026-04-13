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

_COLLECT_TASK_DESCRIPTION_APPENDIX = """

登记内容细化要求：
- 你输出给 record task 的 `content` 草案，必须能直接写入 registry；不要只写标题复述、一个短语，或一句空泛判断。
- 对 `content_type="single"` 的条目，默认写成 2-4 句完整回答，或 3-6 条短点；至少写清直接答案、关键事实或数字、时间/口径，以及边界条件或不确定性。
- 对 `entry_type="judgment"` 的条目，先写结论，再写支撑依据、成立条件和主要例外。
- 对 `entry_type="data"` 的条目，优先写清数值、单位、期间、地域/口径和来源摘要。
- 如果当前只能确认部分信息，也要明确写出“已知什么、缺什么、下一步补什么”，不能只写“待补充”。
"""

_COLLECT_TASK_EXPECTED_OUTPUT_APPENDIX = """

补充约束：
- `existing_entry_updates` 里的 `content` 建议值，要写成可直接落账的完整正文；除非原始事实本身极短，否则不要只给一句话。
- `new_entry_candidates` 里的“核心内容”也要写成可直接写回 registry 的完整正文，并明确关键数字、时间/口径和适用边界。
- 若某条内容仍不足以形成完整正文，必须在 `unresolved_gaps` 中明确说明缺了什么，不能用空话占位。
"""

_RECORD_TASK_DESCRIPTION_APPENDIX = """

落账内容细化要求：
- 调用 `update_entry` 或 `add_entry` 时，`content` 不能只写关键词、标题改写或一句空泛结论；默认写成 2-4 句完整回答，或 3-6 条短点。
- 对 `entry_type="judgment"` 的条目，正文至少包含“结论 + 依据 + 条件/例外”三部分；不要只留一句判断。
- 对 `entry_type="data"` 的条目，正文或表格要尽量补齐数值、单位、期间、地域/口径和来源摘要。
- 如果证据不足以支持详细正文，就保持 `need_revision`，并在 `revision_detail` 里写清具体缺口；不要用很短的占位文本冒充完成。
- `add_evidence` 的 `summary` 要写成 1-3 句具体证据摘要，至少说明关键事实、时间或范围，以及它为什么支持或冲突对应条目。
- `registry_review` 的 `summary` 和 `next_action` 要具体写出：改了哪些条目、还有什么缺口、建议回到哪一步补。
"""

_QA_TASK_DESCRIPTION_APPENDIX = """

内容完整性检查要求：
- 把“content 过短、只有标题复述、只有一句结论没有依据、缺少关键数字/期间/口径、只写待补充但没写缺什么”视为未完成，不要因为有字就放过。
- 对 `judgment` 条目，如果没有“结论 + 依据 + 条件/例外”，优先判为 `missing_content` 或 `need_revision`。
- 对 `data` 条目，如果缺少数值、单位、期间、来源中的关键项，优先判为 `missing_content`。
- 对 `single` 条目，如果正文只有一个短句或一句标签式表述，应明确打回，而不是默认为可综合。
"""

_QA_TASK_EXPECTED_OUTPUT_APPENDIX = """

补充判定标准：
- 当条目内容过短、只有标题复述、只有单句结论、缺少关键数字/期间/口径，或判断没有依据时，`issues` 必须记录为 `missing_content` 或 `need_revision`。
- `summary` 需要点明“哪些条目已经够详细，哪些条目因为内容过短仍不能进入综合输出”。
"""

_TASK_PROMPT_APPENDICES: dict[str, dict[str, str]] = {
    "extract_file_facts": {
        "description": _COLLECT_TASK_DESCRIPTION_APPENDIX,
        "expected_output": _COLLECT_TASK_EXPECTED_OUTPUT_APPENDIX,
    },
    "search_facts": {
        "description": _COLLECT_TASK_DESCRIPTION_APPENDIX,
        "expected_output": _COLLECT_TASK_EXPECTED_OUTPUT_APPENDIX,
    },
    "record_extract_registry": {
        "description": _RECORD_TASK_DESCRIPTION_APPENDIX,
    },
    "record_search_registry": {
        "description": _RECORD_TASK_DESCRIPTION_APPENDIX,
    },
    "check_registry": {
        "description": _QA_TASK_DESCRIPTION_APPENDIX,
        "expected_output": _QA_TASK_EXPECTED_OUTPUT_APPENDIX,
    },
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


def _append_prompt_appendix(original_text: Any, appendix: str) -> str:
    """
    目的：安全地把运行时附加提示词拼到原始 prompt 后面。
    功能：避免重复追加同一段说明，同时保留 tasks.yaml 里的原始内容。
    实现逻辑：先转成字符串，再检查附加段是否已存在；不存在时追加到末尾。
    可调参数：`original_text` 和 `appendix`。
    默认参数及原因：空文本会被转成空字符串，原因是不同 task 字段有时可能缺失。
    """

    normalized_text = str(original_text or "")
    normalized_appendix = appendix.strip()
    if not normalized_appendix:
        return normalized_text
    if normalized_appendix in normalized_text:
        return normalized_text
    if not normalized_text.strip():
        return normalized_appendix
    return f"{normalized_text.rstrip()}\n\n{normalized_appendix}"


def _infer_research_task_name(task_config: dict[str, Any]) -> str:
    """
    目的：在 `strip_research_task_profile_fields()` 只收到单个 task 配置时，恢复它对应的任务名。
    功能：根据 profile 字段、agent 名和描述文本推断当前是 collect、record、qa 还是 synth 任务。
    实现逻辑：优先用显式 profile 字段判断；record task 再回退到 agent 名和描述里的上游任务标记。
    可调参数：`task_config`。
    默认参数及原因：无法识别时返回空字符串，原因是未知任务不应该被强行追加不相关提示词。
    """

    if "search_guidance" in task_config:
        return "search_facts"
    if "extract_guidance" in task_config:
        return "extract_file_facts"
    if "qa_guidance" in task_config:
        return "check_registry"
    if "synthesize_guidance" in task_config or "output_skeleton" in task_config:
        return "synthesize_and_output"

    description = str(task_config.get("description", ""))
    agent_name = str(task_config.get("agent", ""))
    if agent_name == "extract_file_fact_agent" and "`extract_file_facts`" in description:
        return "record_extract_registry"
    if agent_name == "search_fact_agent" and "`search_facts`" in description:
        return "record_search_registry"
    return ""


def _enrich_registry_detail_requirements(
    task_name: str,
    sanitized_config: dict[str, Any],
) -> dict[str, Any]:
    """
    目的：统一给 registry 相关 research task 追加“写详细”的运行时要求。
    功能：按任务类型补充 description / expected_output，让 collect、record、qa 三段都围绕完整正文工作。
    实现逻辑：读取任务名对应的附加提示词，只对存在的字段做追加，避免改动其他配置结构。
    可调参数：`task_name` 和已清理过 metadata 的 `sanitized_config`。
    默认参数及原因：未知任务不做处理，原因是保持最小改动边界，不影响非 registry 相关任务。
    """

    appendices = _TASK_PROMPT_APPENDICES.get(task_name)
    if not appendices:
        return sanitized_config

    enriched_config = dict(sanitized_config)
    for field_name, appendix in appendices.items():
        if field_name not in enriched_config:
            continue
        enriched_config[field_name] = _append_prompt_appendix(enriched_config[field_name], appendix)
    return enriched_config


def strip_research_task_profile_fields(task_config: dict[str, Any]) -> dict[str, Any]:
    """
    目的：在不丢失 tasks.yaml 维护体验的前提下兼容 CrewAI 的任务配置读取。
    功能：移除只给项目内部使用的专题元数据键，返回可直接传给 `Task(config=...)` 的字典。
    实现逻辑：先复制原始任务配置，再逐个弹出 profile 扩展字段。
    可调参数：`task_config`。
    默认参数及原因：默认只剔除项目自定义的元数据字段，原因是其他标准任务字段仍要完整保留给 CrewAI。
    """

    task_name = _infer_research_task_name(task_config)
    sanitized_config = dict(task_config)
    for key in _PROFILE_METADATA_KEYS:
        sanitized_config.pop(key, None)
    return _enrich_registry_detail_requirements(task_name, sanitized_config)
