from __future__ import annotations

import os

from crewai import LLM

# 设计目的：把项目里的大模型配置集中到一个入口，避免各个 crew 自己拼接 OpenRouter 参数。
# 模块功能：提供统一的 `LLM` 构造函数，让业务代码只关心温度、超时和重试次数。
# 实现逻辑：把模型名、API Base 和 API Key 的读取放在这里，所有调用方统一走同一套配置。
# 可调参数：`HEAVY_MODEL`、`temperature`、`timeout`、`max_retries`。
# 默认参数及原因：当前只保留一套稳定可用的模型入口，原因是仓库里实际只在使用这一套配置。

def get_heavy_llm(
    temperature: float = 0.5,
    timeout: float | int | None = 45,
    max_retries: int | None = 5,
) -> LLM:
    """
    设计目的：给项目里的核心分析 agent 提供统一的 LLM 入口。
    模块功能：按当前项目固定的 OpenRouter 配置返回一个 `LLM` 实例。
    实现逻辑：直接把模型、API Base、API Key 和采样参数写入 `LLM(...)`。
    可调参数：`temperature` 常用 0-1；`timeout` 可为正数或 `None`；`max_retries` 可为非负整数或 `None`。
    默认参数及原因：默认 `0.5 / 45 / 5`，原因是当前 research 与 thesis 阶段经常需要跨工具、跨上下文长响应，10 秒超时过短。
    """
    return LLM(
        api_base="https://openrouter.ai/api/v1",
        model="openrouter/google/gemini-3-flash-preview",
        api_key=os.getenv("OPENROUTER_API_KEY"),
        temperature=temperature,
        timeout=timeout,
        max_retries=max_retries,
    )
