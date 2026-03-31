import json
from typing import Optional
import httpx

from tdc.config.models import HTTPConfig
from tdc.core.exceptions import HTTPError


class HTTPClient:
    """HTTP客户端封装"""

    def __init__(self):
        self.client = httpx.AsyncClient(http2=True, timeout=30)

    async def request(self, config: HTTPConfig, rendered_body: Optional[str] = None):
        """执行HTTP请求"""
        try:
            response = await self.client.request(
                method=config.method,
                url=config.url,
                headers=config.headers,
                content=rendered_body.encode() if rendered_body else None,
                timeout=config.timeout
            )
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            raise HTTPError(f"HTTP {e.response.status_code}", status_code=e.response.status_code)

    async def close(self):
        await self.client.aclose()
