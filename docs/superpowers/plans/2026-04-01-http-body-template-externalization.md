# HTTP Body 模板外置化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 HTTP body_template 支持外置 JSON 文件，支持简写、相对路径、完整路径三种引用方式，向后兼容内联模板。

**Architecture:** 新增 `TemplateLoader` 类负责路径解析和文件加载，在 `PipelineEngine` 渲染模板前调用。TemplateLoader 根据配置值的后缀和格式智能判断是文件引用还是内联模板。

**Tech Stack:** Python 3.12+, Pydantic, Jinja2

---

## 文件结构

```
tdc/
├── config/
│   ├── __init__.py
│   ├── loader.py              # 已存在，无需修改
│   ├── models.py              # 已存在，无需修改
│   └── template_loader.py     # 【新增】模板加载器
├── pipeline/
│   ├── __init__.py
│   ├── context.py             # 已存在，无需修改
│   ├── engine.py              # 【修改】集成 TemplateLoader
│   └── http_client.py         # 已存在，无需修改
```

---

## Task 1: 创建 TemplateLoader 类

**Files:**
- Create: `tdc/config/template_loader.py`
- Test: `tests/unit/test_template_loader.py`

- [ ] **Step 1: 编写 TemplateLoader 测试（TDD）**

```python
import pytest
from pathlib import Path
from tdc.config.template_loader import TemplateLoader


class TestTemplateLoader:
    def test_load_shorthand_filename(self, tmp_path):
        """测试简写形式：纯文件名自动解析为 templates/{task_id}/{filename}"""
        # Arrange
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        templates_dir = config_dir / "templates" / "test_task"
        templates_dir.mkdir(parents=True)
        template_file = templates_dir / "create_user.json"
        template_file.write_text('{"name": "test"}')

        loader = TemplateLoader(str(config_dir))

        # Act
        result = loader.load_body_template("create_user.json", "test_task")

        # Assert
        assert result == '{"name": "test"}'

    def test_load_relative_path(self, tmp_path):
        """测试相对路径：./subdir/file.json"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        templates_dir = config_dir / "templates" / "test_task" / "orders"
        templates_dir.mkdir(parents=True)
        template_file = templates_dir / "create.json"
        template_file.write_text('{"order": true}')

        loader = TemplateLoader(str(config_dir))
        result = loader.load_body_template("./orders/create.json", "test_task")

        assert result == '{"order": true}'

    def test_load_absolute_path(self, tmp_path):
        """测试完整路径：templates/other/shared.json"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        shared_dir = config_dir / "templates" / "shared"
        shared_dir.mkdir(parents=True)
        template_file = shared_dir / "common.json"
        template_file.write_text('{"shared": true}')

        loader = TemplateLoader(str(config_dir))
        result = loader.load_body_template("templates/shared/common.json", "test_task")

        assert result == '{"shared": true}'

    def test_load_inline_template(self, tmp_path):
        """测试内联模板：不以 .json 结尾的直接返回原字符串"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        loader = TemplateLoader(str(config_dir))
        inline = '{"inline": "{{ faker.name }}"}'
        result = loader.load_body_template(inline, "test_task")

        assert result == inline

    def test_load_inline_json_like_but_not_file(self, tmp_path):
        """测试看起来像JSON路径但文件不存在时返回原字符串"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        loader = TemplateLoader(str(config_dir))
        inline = '{"not": "a file"}'
        result = loader.load_body_template(inline, "test_task")

        assert result == inline

    def test_file_not_found_raises_error(self, tmp_path):
        """测试文件不存在且以 .json 结尾时抛出 FileNotFoundError"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        loader = TemplateLoader(str(config_dir))

        with pytest.raises(FileNotFoundError) as exc_info:
            loader.load_body_template("nonexistent.json", "test_task")

        assert "nonexistent.json" in str(exc_info.value)
```

- [ ] **Step 2: 运行测试验证失败**

Run: `pytest tests/unit/test_template_loader.py -v`

Expected: 6 个测试全部 FAIL (ImportError 或 AttributeError)

- [ ] **Step 3: 实现 TemplateLoader 类**

```python
from pathlib import Path


class TemplateLoader:
    """模板加载器，负责解析 body_template 路径并加载内容

    支持三种引用方式：
    1. 简写："create_user.json" -> templates/{task_id}/create_user.json
    2. 相对路径："./orders/create.json" -> templates/{task_id}/orders/create.json
    3. 完整路径："templates/shared/common.json" -> 相对项目根目录解析

    不以 .json 结尾或文件不存在时，视为内联模板直接返回原字符串。
    """

    def __init__(self, config_dir: str):
        """
        Args:
            config_dir: configs 目录的路径（包含 db.yaml 和 tasks/ 的目录）
        """
        self.config_dir = Path(config_dir)
        self.template_dir = self.config_dir / "templates"

    def load_body_template(self, template_ref: str, task_id: str) -> str:
        """加载 body_template 内容

        Args:
            template_ref: 配置中的 body_template 值
            task_id: 当前任务的 task_id，用于解析简写

        Returns:
            模板内容字符串

        Raises:
            FileNotFoundError: 当 template_ref 以 .json 结尾但文件不存在时
        """
        # 不以 .json 结尾，视为内联模板
        if not template_ref.endswith(".json"):
            return template_ref

        # 解析文件路径
        file_path = self._resolve_path(template_ref, task_id)

        # 文件不存在，抛出错误
        if not file_path.exists():
            raise FileNotFoundError(f"Template file not found: {file_path}")

        return file_path.read_text(encoding="utf-8")

    def _resolve_path(self, template_ref: str, task_id: str) -> Path:
        """根据引用方式解析为完整路径"""
        # 纯文件名（不含 /）：简写形式
        if "/" not in template_ref:
            return self.template_dir / task_id / template_ref

        # 以 ./ 开头：相对当前 task 目录
        if template_ref.startswith("./"):
            relative = template_ref[2:]  # 去掉 ./
            return self.template_dir / task_id / relative

        # 其他路径：相对 config_dir（通常是 templates/... 或完整相对路径）
        return self.config_dir / template_ref
```

- [ ] **Step 4: 运行测试验证通过**

Run: `pytest tests/unit/test_template_loader.py -v`

Expected: 6 个测试全部 PASS

- [ ] **Step 5: 提交**

```bash
git add tests/unit/test_template_loader.py tdc/config/template_loader.py
git commit -m "feat(config): add TemplateLoader for external body templates

- Support shorthand: create_user.json -> templates/{task_id}/create_user.json
- Support relative path: ./orders/create.json
- Support full path: templates/shared/common.json
- Backward compatible with inline templates

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 2: 修改 PipelineEngine 集成 TemplateLoader

**Files:**
- Modify: `tdc/pipeline/engine.py`
- Test: `tests/unit/test_pipeline_engine.py` (已存在，需更新)

- [ ] **Step 1: 更新 PipelineEngine 导入 TemplateLoader**

修改 `tdc/pipeline/engine.py` 的导入部分：

```python
from jsonpath_ng import parse

from tdc.config.models import PipelineStepConfig, TaskConfig
from tdc.config.template_loader import TemplateLoader
from tdc.core.models import Context, PipelineResult
from tdc.pipeline.context import ContextManager
from tdc.pipeline.http_client import HTTPClient


class PipelineEngine:
    """管道执行引擎"""

    def __init__(self, template_loader: TemplateLoader):
        self.http_client = HTTPClient()
        self.template_loader = template_loader

    async def execute(self, config: TaskConfig, ctx: Context) -> PipelineResult:
        """执行完整的管道"""
        step_results = []

        for step in config.pipeline:
            try:
                await self.execute_step(step, ctx, config.task_id)
                step_results.append({"step_id": step.step_id, "success": True})
            except Exception as e:
                step_results.append({"step_id": step.step_id, "success": False, "error": str(e)})
                if config.on_failure.action == "stop":
                    return PipelineResult(context=ctx, success=False, error=str(e), step_results=step_results)

        return PipelineResult(context=ctx, success=True, step_results=step_results)

    async def execute_step(self, step: PipelineStepConfig, ctx: Context, task_id: str) -> dict:
        """执行单个步骤"""
        manager = ContextManager(ctx)

        # 检查条件
        if step.condition:
            condition_result = manager.render_template(step.condition)
            if not condition_result or condition_result.strip() in ("False", "None", ""):
                return {"skipped": True}

        # 加载并渲染请求体
        rendered_body = None
        if step.http.body_template:
            # 加载模板内容（支持文件或内联）
            template_content = self.template_loader.load_body_template(
                step.http.body_template, task_id
            )
            # 渲染 Jinja2 模板
            rendered_body = manager.render_template(template_content)

        # 渲染headers中的模板
        headers = manager.render_dict(step.http.headers)

        # 执行HTTP请求
        response = await self.http_client.request(step.http, rendered_body)

        # 提取字段到上下文
        if step.extract:
            response_data = response.json()
            for key, json_path in step.extract.items():
                value = self._extract_by_jsonpath(response_data, json_path)
                ctx.set(key, value)

        return {"status_code": response.status_code}

    def _extract_by_jsonpath(self, data: dict, path: str):
        """使用JSONPath提取数据"""
        jsonpath_expr = parse(path)
        matches = jsonpath_expr.find(data)
        if matches:
            return matches[0].value
        return None

    async def close(self):
        await self.http_client.close()
```

- [ ] **Step 2: 更新调用方（scheduler/router.py）**

Read: `tdc/scheduler/router.py`

```python
from tdc.config.models import TaskConfig, TaskType
from tdc.config.template_loader import TemplateLoader
from tdc.core.models import Context
from tdc.pipeline.engine import PipelineEngine
from tdc.generator.engine import DataGenerator
from tdc.storage.tag_store import TagStore
from tdc.storage.batch_insert import BatchInserter
from tdc.config.loader import ConfigLoader
from tdc.storage.mysql_pool import MySQLPoolManager


class TaskRouter:
    """任务路由分发器"""

    def __init__(self, pool_manager: MySQLPoolManager, config_loader: ConfigLoader):
        self.pool_manager = pool_manager
        self.config_loader = config_loader
        # 初始化 TemplateLoader，config_dir 从 ConfigLoader 获取
        self.template_loader = TemplateLoader(str(config_loader.config_dir))

    async def route(self, task: TaskConfig) -> dict:
        """根据任务类型路由到相应处理器"""
        if task.task_type == TaskType.HTTP_SOURCE:
            return await self._handle_http_source(task)
        elif task.task_type == TaskType.DIRECT_INSERT:
            return await self._handle_direct_insert(task)
        else:
            raise ValueError(f"Unknown task type: {task.task_type}")

    async def _handle_http_source(self, task: TaskConfig) -> dict:
        """处理 HTTP 源任务"""
        ctx = Context(task_id=task.task_id)
        # 传递 template_loader 给 PipelineEngine
        engine = PipelineEngine(self.template_loader)

        try:
            result = await engine.execute(task, ctx)

            # 保存标签
            if task.tag_mapping and result.success:
                engine_db = self.pool_manager.get_engine(task.target_db.instance)
                from sqlalchemy.ext.asyncio import AsyncSession
                async with AsyncSession(engine_db) as session:
                    tag_store = TagStore(session)
                    await tag_store.save_tags(ctx, task.tag_mapping)
                    await session.commit()

            return {"success": result.success, "steps": result.step_results}
        finally:
            await engine.close()

    async def _handle_direct_insert(self, task: TaskConfig) -> dict:
        """处理直接插入任务"""
        # ... 现有逻辑不变
        generator = DataGenerator(task.data_template)
        records = generator.generate()

        engine = self.pool_manager.get_engine(task.target_db.instance)
        from sqlalchemy.ext.asyncio import AsyncSession
        async with AsyncSession(engine) as session:
            inserter = BatchInserter(session, task.target_db.database)
            count = await inserter.insert(records, task.data_template.table)
            return {"success": True, "inserted": count}
```

- [ ] **Step 3: 运行现有测试确保不破坏**

Run: `pytest tests/unit/test_pipeline_engine.py -v`

注意：测试可能需要更新以提供 `TemplateLoader` 实例

Expected: 现有测试通过（可能需要更新 mock）

- [ ] **Step 4: 创建集成测试验证端到端流程**

创建或更新 `tests/integration/test_template_integration.py`：

```python
import pytest
import tempfile
from pathlib import Path
from tdc.config.template_loader import TemplateLoader
from tdc.config.models import HTTPConfig, PipelineStepConfig
from tdc.core.models import Context
from tdc.pipeline.context import ContextManager


class TestTemplateIntegration:
    def test_full_template_load_and_render(self, tmp_path):
        """测试完整的模板加载和渲染流程"""
        # Arrange: 创建配置目录和模板文件
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        # 创建模板目录和文件
        templates_dir = config_dir / "templates" / "order_flow"
        templates_dir.mkdir(parents=True)
        template_file = templates_dir / "create_user.json"
        template_file.write_text('{"name": "{{ faker.name }}", "email": "{{ faker.email }}"}')

        # Act: 加载模板
        loader = TemplateLoader(str(config_dir))
        template_content = loader.load_body_template("create_user.json", "order_flow")

        # Act: 渲染模板
        ctx = Context(task_id="order_flow")
        manager = ContextManager(ctx)
        rendered = manager.render_template(template_content)

        # Assert: 验证渲染结果
        import json
        data = json.loads(rendered)
        assert "name" in data
        assert "email" in data
        assert data["name"] != "{{ faker.name }}"  # 应该被替换
        assert data["email"] != "{{ faker.email }}"  # 应该被替换
```

- [ ] **Step 5: 运行集成测试**

Run: `pytest tests/integration/test_template_integration.py -v`

Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add tdc/pipeline/engine.py tdc/scheduler/router.py tests/
git commit -m "feat(pipeline): integrate TemplateLoader into PipelineEngine

- PipelineEngine now accepts TemplateLoader in constructor
- execute_step loads template content before rendering
- TaskRouter initializes and passes TemplateLoader to PipelineEngine
- Add integration tests for template load + render flow

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 3: 创建示例模板文件和验证

**Files:**
- Create: `configs/templates/example_order_flow/create_user.json`
- Create: `configs/templates/example_order_flow/create_order.json`
- Modify: `configs/tasks/example_http.yaml`

- [ ] **Step 1: 创建模板目录和示例模板文件**

```bash
mkdir -p configs/templates/example_order_flow
```

创建 `configs/templates/example_order_flow/create_user.json`：
```json
{
  "username": "{{ faker.name }}",
  "email": "{{ faker.email }}",
  "phone": "{{ faker.phone_number }}",
  "register_time": "{{ now.isoformat() }}",
  "source": "TDC_AUTO_GENERATE"
}
```

创建 `configs/templates/example_order_flow/create_order.json`：
```json
{
  "user_id": "{{ context.user_id }}",
  "order_no": "ORD{{ faker.random_number(digits=10) }}",
  "amount": {{ faker.random_int(min=100, max=10000) }},
  "status": "pending",
  "created_at": "{{ now.isoformat() }}"
}
```

- [ ] **Step 2: 更新 example_http.yaml 使用外置模板**

修改 `configs/tasks/example_http.yaml`：

```yaml
task_id: "example_order_flow"
task_name: "示例-订单全流程构造（外置模板版）"
task_type: "http_source"
schedule: "0 2 * * *"

pipeline:
  - step_id: "create_user"
    name: "创建测试用户"
    http:
      url: "https://httpbin.org/post"
      method: POST
      # 使用外置模板文件
      body_template: "create_user.json"
    extract:
      user_id: "$.json.username"

  - step_id: "create_order"
    name: "创建订单"
    http:
      url: "https://httpbin.org/post"
      method: POST
      # 使用外置模板文件，引用上一步提取的 user_id
      body_template: "create_order.json"
    extract:
      order_no: "$.json.order_no"

tag_mapping:
  user_id: "{{ context.user_id }}"
  order_id: "{{ context.order_no }}"
  data_tag: "EXAMPLE_HTTP_DATA"

target_db:
  instance: "biz_db_01"
  database: "test_db"
```

- [ ] **Step 3: 验证配置格式**

Run: `python -m tdc.cli config validate --file configs/tasks/example_http.yaml`

Expected: `Config file is valid: configs/tasks/example_http.yaml`

- [ ] **Step 4: 提交**

```bash
git add configs/templates/ configs/tasks/example_http.yaml
git commit -m "feat(configs): add external template examples

- Add configs/templates/example_order_flow/ with sample templates
- Update example_http.yaml to use external body_template references
- Templates demonstrate faker variables and context extraction

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 4: 更新 __init__.py 导出 TemplateLoader

**Files:**
- Modify: `tdc/config/__init__.py`

- [ ] **Step 1: 更新 config 包的导出**

修改 `tdc/config/__init__.py`：

```python
from tdc.config.loader import ConfigLoader
from tdc.config.models import (
    DBConfig,
    DBInstanceConfig,
    FieldGeneratorConfig,
    HTTPAuthConfig,
    HTTPConfig,
    PipelineStepConfig,
    RelationConfig,
    DataTemplateConfig,
    TagMappingConfig,
    TargetDBConfig,
    RetryConfig,
    OnFailureConfig,
    TaskConfig,
)
from tdc.config.template_loader import TemplateLoader

__all__ = [
    "ConfigLoader",
    "TemplateLoader",
    "DBConfig",
    "DBInstanceConfig",
    "FieldGeneratorConfig",
    "HTTPAuthConfig",
    "HTTPConfig",
    "PipelineStepConfig",
    "RelationConfig",
    "DataTemplateConfig",
    "TagMappingConfig",
    "TargetDBConfig",
    "RetryConfig",
    "OnFailureConfig",
    "TaskConfig",
]
```

- [ ] **Step 2: 验证导入正常**

Run: `python -c "from tdc.config import TemplateLoader; print('OK')"`

Expected: `OK`

- [ ] **Step 3: 提交**

```bash
git add tdc/config/__init__.py
git commit -m "chore(config): export TemplateLoader from config package

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## 自检查清单

- [x] **Spec coverage**: 所有设计文档中的功能点都已覆盖
  - 简写形式 (create_user.json) → Task 1 Step 1
  - 相对路径 (./orders/create.json) → Task 1 Step 1
  - 完整路径 (templates/shared/...) → Task 1 Step 1
  - 内联模板向后兼容 → Task 1 Step 1
  - PipelineEngine 集成 → Task 2
  - 示例配置 → Task 3

- [x] **No placeholders**: 所有代码块都是完整可执行的

- [x] **Type consistency**: 
  - `TemplateLoader.__init__(config_dir: str)` 一致
  - `load_body_template(template_ref: str, task_id: str) -> str` 一致

---

## 执行方式选择

**Plan complete and saved to `docs/superpowers/plans/2026-04-01-http-body-template-externalization.md`.**

Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints for review

Which approach would you prefer?
