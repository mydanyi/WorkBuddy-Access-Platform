"""Automated-action whitelist for the growth task runner.

首版（first cut）只把「服务端可以用真实动作链复现」的任务纳入白名单；其余一律
``manual``/``unknown``，绝不发 ``auth_confirm``/``automation_create_suc``/
``skill success`` 之类事件假装执行。

动作种类（action kind）::

    chat       普通动态模型对话 + billing ``chat_request_send`` 上报
    model_chat 指定模型对话（必须真实存在 ``glm-5.2``）
    black_cat  夜间窗口（Asia/Shanghai 23:00-08:00）的 ``glm-5.2`` 对话
    richmeow   桌面端成功对话链（需 ``allow_telemetry``）
    library    资料库阅读点击上报（需 ``allow_telemetry``）

凡是需要真实外部身份授权、真实工具技能结果、小程序 / 订阅捐赠 / 账号偏好
（主题、协议签署）的动作，首版都不会自动执行。

``allowed`` / ``action_for`` 有意做成纯静态：它们描述「不受实例开关影响」的
基础白名单。实例级开关（``allow_telemetry`` / ``allow_adoption`` /
``allow_theme_change``）由 :class:`core.growth.GrowthService` 在动作选择时叠加。
"""

from __future__ import annotations

# 基础白名单：任务码 -> 动作种类。
_ACTIONS = {
    "chat_5": "chat",
    "Model_chat_GLM5.2": "model_chat",
    "black_cat": "black_cat",
    "RichMeow_Chat": "richmeow",
    "Library_read": "library",
}

# 需要 ``allow_telemetry`` 的动作（客户端行为指纹上报）。
_TELEMETRY_ACTIONS = frozenset({"richmeow", "library"})

# 首版明确 manual 的任务与原因（仅用于可读的 pending 原因，不影响执行）。
MANUAL_REASONS = {
    # 外部身份 / 授权
    "Buddy_App": "external_authorization",
    "Buddy_App_QQ": "external_authorization",
    # 没有可复现的真实客户端动作链
    "automation_1": "no_client_action",
    "template_5": "no_client_action",
    "playbook_prompt": "no_client_action",
    "create_canvas": "no_client_action",
    "expert_5": "no_client_action",
    "Expert_team_use_3": "no_client_action",
    "Expert_lighthouse": "no_client_action",
    # 真实工具技能结果不可伪造
    "skill_1": "skill_result_not_forgeable",
    # 小程序
    "school_season": "miniprogram",
    "Sequential_Tasks_1": "miniprogram",
    "Sequential_Tasks_2": "miniprogram",
    # 账号偏好 / 协议变更，需用户显式开启
    "first_buddy": "adoption_requires_consent",
    "Hp_Appearance": "theme_change_disabled",
}

_UNKNOWN_REASON = "unknown"


class TaskActions:
    """静态自动化白名单。"""

    @staticmethod
    def allowed(task_code) -> bool:
        """该任务是否可被自动执行（基础白名单，不含实例开关）。"""
        return task_code in _ACTIONS

    @staticmethod
    def action_for(task_code):
        """返回动作种类；manual / unknown 返回 ``None``。"""
        return _ACTIONS.get(task_code)

    @staticmethod
    def requires_telemetry(task_code) -> bool:
        """该任务的动作是否依赖客户端行为指纹上报。"""
        return _ACTIONS.get(task_code) in _TELEMETRY_ACTIONS

    @staticmethod
    def manual_reason(task_code) -> str:
        """manual / unknown 任务的可读原因。"""
        return MANUAL_REASONS.get(task_code, _UNKNOWN_REASON)
