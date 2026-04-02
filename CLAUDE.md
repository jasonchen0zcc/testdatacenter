# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

- **Name**: testdatacenter / TDC (Test Data Center)
- **Type**: Python project (Python 3.12+)
- **IDE**: PyCharm/IntelliJ IDEA
- **Status**: Active development - TDC core modules implemented

## Common Commands

### Setup

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"
```

### Configuration (.env file)

```bash
# Copy template and edit
cp .env.example .env

# Edit .env with your database credentials
TDC_DB_HOST=8.137.20.173
TDC_DB_PORT=3306
TDC_DB_USER=root
TDC_DB_PASSWORD=your_password
TDC_DB_POOL_SIZE=10
```

Or use environment variables directly:
```bash
export TDC_DB_PASSWORD="your_password"
tdc scheduler start

# Or inline
TDC_DB_PASSWORD="your_password" tdc task run --task-id example
```

### Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_engine.py

# Run specific test class
pytest tests/unit/test_engine.py::TestPipelineEngine

# Run specific test method
pytest tests/unit/test_engine.py::TestPipelineEngine::test_execute_step_success

# Run with coverage
pytest --cov=tdc --cov-report=term-missing
```

### Code Quality

```bash
# Format code
black tdc/ tests/

# Lint
ruff check tdc/ tests/

# Type check
mypy tdc/
```

### CLI

```bash
# Start scheduler
tdc scheduler start --config-dir ./configs

# List tasks
tdc task list

# Run task immediately
tdc task run --task-id example_http

# Validate config
tdc config validate --file configs/tasks/example_http.yaml
```

## Skills Workflow

### Core Skills

| Scenario | Skill | Trigger |
|----------|-------|---------|
| Design/brainstorming | superpowers:brainstorming | Before any feature/modification |
| Plan writing | superpowers:writing-plans | After design approval, before coding |
| Plan execution (single session) | superpowers:subagnet-driven-development | When there's an independent task list |
| Plan execution (parallel sessions) | superpowers:executing-plans | When batch review is needed |
| Debugging | superpowers:systematic-debugging | Any bug or test failure |
| Verification | superpowers:verification-before-completion | Before claiming completion/pass |
| Git workflow | superpowers:using-git-worktrees | At start of feature development |
| Finish branch | superpowers:finishing-a-development-branch | When implementation is complete |

### Code Quality Skills

| Scenario | Skill | Trigger |
|----------|-------|---------|
| TDD test-driven | superpowers:test-driven-development | New features/fixes/refactoring |
| Code review | superpowers:requesting-code-review | After each task completion |
| Security audit | everything-claude-code:security-review | Auth/input/secrets related |

### ECC Domain Skills

| Scenario | Skill | Trigger |
|----------|-------|---------|
| Build fix | everything-claude-code:build-fix | Compilation failures |
| E2E testing | everything-claude-code:e2e | Critical user flows |
| Java review | everything-claude-code:java-review | Java code changes |
| Python review | everything-claude-code:python-review | Python code changes |

## Architecture Overview

### Core Modules

```
tdc/
├── core/           # 领域模型、常量、异常定义
├── config/         # 配置加载与验证（Pydantic models, TemplateLoader）
├── scheduler/      # APScheduler 任务调度
├── pipeline/       # HTTP 管道执行（含模板渲染）
├── generator/      # Faker 数据生成
└── storage/        # MySQL 连接池与批量写入
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| `TemplateLoader` | `tdc/config/template_loader.py` | 解析 body_template 路径，加载外部模板文件 |
| `PipelineEngine` | `tdc/pipeline/engine.py` | 执行 HTTP 管道，支持多轮迭代和网关认证 |
| `TaskRouter` | `tdc/scheduler/router.py` | 初始化 TemplateLoader 并传递给 PipelineEngine |
| `ContextManager` | `tdc/pipeline/context.py` | 渲染 Jinja2 模板（faker, context, now, execution 变量）|
| `GatewayAuth` | `tdc/pipeline/gateway_auth.py` | 网关认证管理，获取并注入 token |
| `UserProvider` | `tdc/pipeline/user_provider.py` | 提供用户数据（faker/http/list 三种来源）|

### Configuration Structure

```
configs/
├── db.yaml              # 数据库连接配置
├── tasks/               # 任务定义（YAML）
│   ├── example_http.yaml
│   └── example_insert.yaml
└── templates/           # HTTP body 模板（JSON + Jinja2）
    └── {task_id}/
        ├── {step_id}.json
        └── ...
```

**Task Config Key Fields**:
- `task_type`: `http_source` | `direct_insert`
- `pipeline`: HTTP 步骤列表（http_source 专用）
- `data_template`: 数据生成模板（direct_insert 专用）
- `execution`: 批量执行配置（iterations, user_source, delay_ms）
- `gateway`: 网关认证配置（auth_url, body_template, token_path）
- `schedule`: Cron 表达式（如 `0 2 * * *` 每天2点）
- `timeout`: 任务执行超时（秒，默认300）
- `enabled`: 是否启用（true/false）

### Multi-Task Support

**Capabilities**:
- Unlimited tasks in `configs/tasks/` directory
- Independent cron schedule per task
- Isolated execution (one task failure doesn't affect others)
- Automatic duplicate task_id detection on startup
- Mixed task types support (http_source + direct_insert)

**Concurrency Control**:
- `max_instances=1`: Prevents same task from concurrent execution
- `coalesce=True`: Merges missed executions into single run
- Database-level lock: Checks `running` status in `tdc_task_log`
- Task timeout: Configurable per task (default 300s)

**Cron Schedule Examples**:
```yaml
schedule: "*/5 * * * *"    # Every 5 minutes
schedule: "0 */2 * * *"    # Every 2 hours
schedule: "0 2 * * *"      # Daily at 2:00 AM
schedule: "0 9 * * 1-5"    # Weekdays at 9:00 AM
```

**模板支持三种引用方式**（详见设计文档 `docs/superpowers/specs/2026-04-01-http-body-template-externalization-design.md`）：
1. **简写**: `body_template: "create_user.json"` → 自动解析为 `templates/{task_id}/create_user.json`
2. **相对路径**: `body_template: "./orders/create.json"` → 基于当前 task 目录
3. **内联**: `body_template: "{{...}}"` → 直接作为模板字符串（向后兼容）

**模板变量**（由 `ContextManager` 提供）：
- `faker` - Faker 实例（`faker.name`, `faker.email`, `faker.username` 等）
- `context` - 管道执行上下文（通过 `extract` 设置的变量）
- `now` - 当前时间 datetime 对象
- `execution` - 单次迭代执行上下文
  - `execution.user` - 当前用户
  - `execution.iteration` - 当前迭代序号（0-based）
  - `execution.total` - 总迭代次数

### Template Loading Flow

```
TaskConfig (YAML)
    │
    ▼
PipelineEngine.execute_step(step, ctx, task_id, execution, gateway_auth)
    │
    ├─► TemplateLoader.load_body_template(template_ref, task_id)
    │       │
    │       ├─► 以 .json 结尾? ──No──► 返回原字符串（内联模板）
    │       │
    │       └─► Yes ──► _resolve_path() ──► 读取文件内容
    │                   │
    │                   ├── 纯文件名 ───► templates/{task_id}/{filename}
    │                   ├── ./ 开头 ───► templates/{task_id}/{relative_path}
    │                   └── 其他路径 ───► 相对 config_dir 解析
    │
    ├─► ContextManager.render_template_with_execution(template, execution)
    │       │
    │       └─► Jinja2 渲染（提供 faker, context, now, execution 变量）
    │               └── execution.user, execution.iteration, execution.total
    │
    └─► gateway_auth.apply_to_request(headers) ──► 注入认证 token
```

### Pipeline Execution Flow (with Iterations)

```
PipelineEngine.execute(config, ctx)
    │
    ├─► UserProvider.initialize()
    │       ├── user_source=faker ──► 延迟生成（无需初始化）
    │       ├── user_source=http ──► 从 HTTP 接口预取用户列表
    │       └── user_source=list ──► 使用配置的静态列表
    │
    └─► For iteration in range(execution.iterations):
            │
            ├─► user = UserProvider.get_user(iteration)
            │       └── faker 模式每次渲染模板；http/list 模式循环使用列表
            │
            ├─► Create ExecutionContext(iteration, user, total)
            │
            ├─► GatewayAuth.authenticate(execution) ──► 获取 token
            │       ├── 渲染 auth body_template（可访问 execution.user）
            │       ├── 发送认证请求
            │       └── JSONPath 提取 token
            │
            └─► Execute pipeline steps
                    ├── 渲染 step body_template（含 execution 变量）
                    ├── 注入 gateway token 到 headers
                    └── 执行 HTTP 请求，extract 字段到 context
```

## Development Workflow

### Feature Development Flow

1. **brainstorming** - Explore requirements, output design document
2. **writing-plans** - Create implementation plan
3. **using-git-worktrees** - Create isolated worktree environment
4. **subagnet-driven-development** - Execute plan tasks
   - Per task: Implement → Self-review → Spec review → Quality review
5. **verification-before-completion** - Verify tests pass
6. **finishing-a-development-branch** - Merge/PR/Cleanup

### Parallel Task Processing

When 2+ independent tasks:
- Use **dispatching-parallel-agents** to distribute parallel investigation
- Aggregate results → Review conflicts → Full verification

### Debugging Flow

1. **systematic-debugging** - Root cause investigation
   - Phase 1: Gather evidence (error/repro/logs)
   - Phase 2: Pattern analysis (find reference/compare differences)
   - Phase 3: Hypothesis validation (single variable testing)
   - Phase 4: Implement fix (test → fix → verify)
2. **test-driven-development** - Write regression tests
3. **verification-before-completion** - Verify fix
