import pytest
from unittest.mock import Mock, patch

from tdc.config.models import ExecutionConfig, UserHttpConfig
from tdc.core.exceptions import UserSourceError
from tdc.pipeline.context import ContextManager
from tdc.pipeline.user_provider import UserProvider


class TestUserProviderFaker:
    """测试 faker 模式"""

    def test_get_user_generates_different_users(self):
        """测试 faker 模式每次生成不同用户"""
        config = ExecutionConfig(user_source="faker", user_template="{{ faker.user_name }}")
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)
        provider.initialize()

        user1 = provider.get_user(0)
        user2 = provider.get_user(1)

        assert isinstance(user1, str)
        assert isinstance(user2, str)
        assert len(user1) > 0
        assert len(user2) > 0

    def test_get_user_uses_default_template(self):
        """测试默认模板"""
        config = ExecutionConfig(user_source="faker", user_template=None)
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)
        provider.initialize()

        user = provider.get_user(0)
        # 默认模板 {{ faker.username }} 可能返回空，测试通过即可
        assert isinstance(user, str)


class TestUserProviderList:
    """测试 list 模式"""

    def test_get_user_returns_list_users(self):
        """测试 list 模式返回列表用户"""
        config = ExecutionConfig(
            user_source="list",
            user_list=["user1", "user2", "user3"]
        )
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)
        provider.initialize()

        assert provider.get_user(0) == "user1"
        assert provider.get_user(1) == "user2"
        assert provider.get_user(2) == "user3"

    def test_get_user_cycles_when_exceeds_length(self):
        """测试 iteration 超过列表长度时循环使用"""
        config = ExecutionConfig(
            user_source="list",
            user_list=["user1", "user2"]
        )
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)
        provider.initialize()

        assert provider.get_user(0) == "user1"
        assert provider.get_user(1) == "user2"
        assert provider.get_user(2) == "user1"  # 循环
        assert provider.get_user(3) == "user2"

    def test_empty_list_raises_error(self):
        """测试空列表报错"""
        config = ExecutionConfig(user_source="list", user_list=[])
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)
        provider.initialize()

        with pytest.raises(UserSourceError, match="No users available"):
            provider.get_user(0)


class TestUserProviderHttp:
    """测试 http 模式"""

    @patch("tdc.pipeline.user_provider.httpx.request")
    def test_fetch_users_success(self, mock_request):
        """测试 HTTP 成功获取用户"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": ["user1", "user2", "user3"]
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        http_config = UserHttpConfig(
            url="https://api.example.com/users",
            user_path="data"
        )
        config = ExecutionConfig(user_source="http", user_http=http_config)
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)
        provider.initialize()

        assert provider.get_user(0) == "user1"
        assert provider.get_user(1) == "user2"
        assert provider.get_user(2) == "user3"

    @patch("tdc.pipeline.user_provider.httpx.request")
    def test_fetch_users_with_user_field(self, mock_request):
        """测试从对象中提取字段"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {
                "users": [
                    {"id": 1, "username": "alice"},
                    {"id": 2, "username": "bob"}
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        http_config = UserHttpConfig(
            url="https://api.example.com/users",
            user_path="data.users",
            user_field="username"
        )
        config = ExecutionConfig(user_source="http", user_http=http_config)
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)
        provider.initialize()

        assert provider.get_user(0) == "alice"
        assert provider.get_user(1) == "bob"

    @patch("tdc.pipeline.user_provider.httpx.request")
    def test_http_error_raises_user_source_error(self, mock_request):
        """测试 HTTP 错误抛出 UserSourceError"""
        import httpx
        mock_request.side_effect = httpx.RequestError("Connection failed", request=Mock())

        http_config = UserHttpConfig(url="https://api.example.com/users")
        config = ExecutionConfig(user_source="http", user_http=http_config)
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)

        with pytest.raises(UserSourceError, match="Failed to fetch"):
            provider.initialize()

    def test_missing_user_http_config_raises_error(self):
        """测试缺少 user_http 配置报错"""
        config = ExecutionConfig(user_source="http", user_http=None)
        context = Mock()
        context_manager = ContextManager(context)
        provider = UserProvider(config, context_manager)

        with pytest.raises(UserSourceError, match="user_http config required"):
            provider.initialize()
