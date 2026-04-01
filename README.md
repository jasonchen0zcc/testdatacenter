# TDC (Test Data Center) - 测试数据生成中心

一个支持定时调度、HTTP接口链调用、数据模板生成、MySQL批量写入的测试数据生成中心。

## 特性

- **定时调度**: 基于Cron表达式的任务调度
- **HTTP管道**: 支持多接口链式调用，上下文传递
- **外置模板**: HTTP body 支持 JSON 文件外置，简化复杂请求体维护
- **数据生成**: 基于Faker的数据模板生成
- **批量写入**: MySQL批量插入优化
- **数据标记**: 独立标记表，支持测试数据追溯
- **多实例支持**: 支持多MySQL实例连接

## 快速开始

### 1. 安装依赖

```bash
pip install -e ".[dev]"
```

### 2. 初始化数据库

```bash
mysql -u root -p < scripts/init_db.sql
```

### 3. 配置数据库

编辑 `configs/db.yaml`，配置MySQL实例连接信息。

### 4. 创建 HTTP 任务

**方式A：外置模板文件（推荐用于复杂 body）**

创建 `configs/templates/order_flow/create_user.json`：
```json
{
  "username": "{{ faker.name }}",
  "email": "{{ faker.email }}",
  "phone": "{{ faker.phone_number }}"
}
```

在 `configs/tasks/order_flow.yaml` 中引用：
```yaml
task_id: "order_flow"
task_type: "http_source"
schedule: "0 2 * * *"
pipeline:
  - step_id: "create_user"
    http:
      url: "https://api.example.com/users"
      method: POST
      body_template: "create_user.json"  # 简写，自动解析路径
```

**方式B：内联模板（适合简单 body）**
```yaml
pipeline:
  - step_id: "simple_step"
    http:
      url: "https://api.example.com/simple"
      method: POST
      body_template: |
        {"message": "{{ faker.word }}"}
```

### 5. 启动调度器

```bash
tdc scheduler start --config-dir ./configs
```

## CLI命令

```bash
# 启动调度器
tdc scheduler start

# 列出任务
tdc task list

# 立即执行任务
tdc task run --task-id example_http

# 验证配置
tdc config validate --file configs/tasks/example_http.yaml
```

## Docker部署

```bash
docker-compose up -d
```

## 设计文档

| 文档 | 说明 |
|------|------|
| [HTTP Body 模板外置化设计](docs/superpowers/specs/2026-04-01-http-body-template-externalization-design.md) | body_template 文件化方案（简写/相对路径/内联） |
| [CLAUDE.md](CLAUDE.md) | 项目架构、开发流程、技能工作流 |

## 项目结构

### 代码结构

```
tdc/
├── core/              # 领域模型和常量
├── config/            # 配置管理（含 TemplateLoader）
├── scheduler/         # 调度器
├── pipeline/          # HTTP管道执行（模板渲染）
├── generator/         # 数据生成
├── storage/           # 存储层
└── cli.py             # 命令行入口
```

### 配置结构

```
configs/
├── db.yaml                    # 数据库连接配置
├── tasks/                     # 任务定义（YAML）
│   ├── example_http.yaml
│   └── example_insert.yaml
└── templates/                 # HTTP body 模板（JSON + Jinja2）
    └── {task_id}/
        ├── {step_id}.json
        └── ...
```
