"""粗粒度断言验证器."""
from dataclasses import dataclass
from typing import Any, Optional

import httpx
from jsonpath_ng import parse

from tdc.config.models import AssertionConfig


@dataclass
class AssertionResult:
    """断言结果"""
    success: bool
    message: str = ""


class AssertionValidator:
    """HTTP 响应断言验证器"""

    @staticmethod
    def validate(response: httpx.Response, config: Optional[AssertionConfig]) -> AssertionResult:
        """
        验证 HTTP 响应是否符合断言配置

        检查顺序:
        1. status_code - HTTP 状态码
        2. json_path + json_expected - JSON 字段值
        3. json_success_path - 布尔成功标识
        """
        if config is None:
            return AssertionResult(success=True)

        try:
            # 1. 验证状态码
            if config.status_code is not None:
                result = AssertionValidator._check_status_code(
                    response.status_code, config.status_code
                )
                if not result.success:
                    return result

            # 需要解析 JSON 的断言
            if config.json_path or config.json_success_path:
                try:
                    response_data = response.json()
                except Exception as e:
                    return AssertionResult(
                        success=False,
                        message=f"Failed to parse response as JSON: {e}"
                    )

                # 2. 验证 JSON 字段值
                if config.json_path is not None:
                    result = AssertionValidator._check_json_path(
                        response_data, config.json_path, config.json_expected
                    )
                    if not result.success:
                        return result

                # 3. 验证布尔成功标识
                if config.json_success_path is not None:
                    result = AssertionValidator._check_json_success(
                        response_data, config.json_success_path, config.json_success_value
                    )
                    if not result.success:
                        return result

            return AssertionResult(success=True)

        except Exception as e:
            return AssertionResult(
                success=False,
                message=f"Assertion validation error: {e}"
            )

    @staticmethod
    def _check_status_code(actual: int, expected: Any) -> AssertionResult:
        """检查状态码"""
        # 支持单个值或列表
        if isinstance(expected, (list, tuple)):
            if actual not in expected:
                return AssertionResult(
                    success=False,
                    message=f"Status code {actual} not in expected {expected}"
                )
        else:
            if actual != expected:
                return AssertionResult(
                    success=False,
                    message=f"Status code {actual} != expected {expected}"
                )
        return AssertionResult(success=True)

    @staticmethod
    def _check_json_path(data: dict, path: str, expected: Any) -> AssertionResult:
        """检查 JSON 字段值"""
        try:
            jsonpath_expr = parse(path)
            matches = jsonpath_expr.find(data)

            if not matches:
                return AssertionResult(
                    success=False,
                    message=f"JSON path '{path}' not found in response"
                )

            actual_value = matches[0].value

            if actual_value != expected:
                return AssertionResult(
                    success=False,
                    message=f"JSON path '{path}' value '{actual_value}' != expected '{expected}'"
                )

            return AssertionResult(success=True)

        except Exception as e:
            return AssertionResult(
                success=False,
                message=f"Failed to check JSON path '{path}': {e}"
            )

    @staticmethod
    def _check_json_success(data: dict, path: str, expected: bool) -> AssertionResult:
        """检查布尔成功标识"""
        try:
            jsonpath_expr = parse(path)
            matches = jsonpath_expr.find(data)

            if not matches:
                return AssertionResult(
                    success=False,
                    message=f"Success indicator path '{path}' not found"
                )

            actual_value = matches[0].value

            if actual_value != expected:
                return AssertionResult(
                    success=False,
                    message=f"Success indicator '{path}' is '{actual_value}', expected '{expected}'"
                )

            return AssertionResult(success=True)

        except Exception as e:
            return AssertionResult(
                success=False,
                message=f"Failed to check success indicator '{path}': {e}"
            )
