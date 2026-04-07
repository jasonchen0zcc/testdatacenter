# TDC 配置系统

## 概述

TDC 配置系统支持配置继承、步骤模板复用、密钥管理和热加载功能，帮助您更高效地管理大量任务配置。

## 目录结构

```
configs/
├── base/              # 基础配置层
│   ├── default.yaml   # 默认基础配置
│   ├── order_db.yaml  # 订单库配置
│   └── user_db.yaml   # 用户库配置
├── common/            # 公共组件层
│   └── steps.yaml     # 公共步骤模板
└── tasks/             # 任务定义层
    ├── _index.yaml    # 任务目录索引
    ├── order/         # 订单相关任务
    └── user/          # 用户相关任务
```

## 配置继承

### 基础配置

创建 `configs/base/mybase.yaml`:

```yaml
base_id: "mybase"
target_db:
  instance: "db01"
  database: "test"
execution:
  iterations: 100
  delay_ms: 50
```

### 任务继承

```yaml
extends: "base/mybase"

task_id: "mytask"
task_name: "My Task"
# target_db 和 execution 从基础配置继承
# 可以覆盖特定字段
execution:
  iterations: 50  # 覆盖父配置的 100
```

### 多继承

```yaml
extends:
  - "base/db"
  - "base/exec"
```

## 步骤模板复用

### 定义模板

在 `configs/common/steps.yaml` 中定义:

```yaml
step_templates:
  login: &step_login
    step_id: "login"
    name: "用户登录"
    http:
      url: "{{ gateway }}/auth"
      method: POST
      body_template: "login.json"
    extract:
      token: "data.token"
```

### 使用模板

```yaml
imports:
  anchors: "common/steps"

pipeline:
  - <<: *step_login
  - step_id: "create_order"
    # ...
```

## 密钥管理

### 环境变量

```yaml
target_db:
  password: "${DB_PASSWORD}"
  # 带默认值
  password: "${DB_PASSWORD:-default_pass}"
```

### 密钥引用

```yaml
target_db:
  password:
    provider: "env"
    key: "DB_PASSWORD"
    default: "fallback"  # 可选
```

### 文件密钥

```yaml
api_key:
  provider: "file"
  path: "/etc/secrets/api_key"
```

## 热加载

配置修改后自动生效（运行中的任务除外）：

```python
from tdc.config.loader import ConfigLoader
from tdc.config.watcher import ConfigWatcher
from tdc.config.cache import ConfigCache

loader = ConfigLoader("./configs", enable_cache=True)
cache = ConfigCache()
watcher = ConfigWatcher(
    config_dir=Path("./configs"),
    loader=loader,
    cache=cache,
    check_interval=5.0  # 5秒检查一次
)

# 启动监听
await watcher.start()

# 停止监听
await watcher.stop()
```

## 配置验证

```bash
# 验证所有配置
tdc config validate --all

# 验证指定目录
tdc config validate --dir configs/tasks/order
```
