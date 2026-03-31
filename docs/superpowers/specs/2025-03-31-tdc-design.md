# 测试数据生成中心（TDC）设计文档

**版本**: v1.0
**日期**: 2025-03-31
**状态**: 待实现

---

## 1. 项目背景与目标

### 1.1 背景

测试团队需要自动化构造测试数据，数据来源包括：
- 内部业务接口调用
- 内部第三方服务接口
- 直接数据库插入

### 1.2 目标

构建一个可配置、可调度、可扩展的测试数据生成中心，支持：
- 定时任务触发（Cron）
- 多接口链式调用（支持上下文传递）
- 数据标记追踪（独立标记表）
- 多实例、多库、多表写入

---

## 2. 整体架构

### 2.1 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                      TDC 单体应用                                 │
├─────────────────────────────────────────────────────────────────┤
│  调度层 (Scheduler - APScheduler)                                │
│         │                                                       │
│         ▼                                                       │
│  任务路由 ─────┬──────────────────────────┐                      │
│                │                          │                      │
│    类型A: 接口调用任务              类型B: 数据插入任务              │
│    (http_source)                   (direct_insert)               │
│                │                          │                      │
│                ▼                          ▼                      │
│  ┌─────────────────────┐    ┌─────────────────────┐             │
│  │   HTTP 调用模块      │    │   数据构造模块       │             │
│  │  - 请求模板渲染      │    │  - 数据模板引擎      │             │
│  │  - 认证/签名        │    │  - 字段生成规则      │             │
│  │  - 响应解析         │    │  - 关联关系处理      │             │
│  └─────────────────────┘    └─────────────────────┘             │
│                │                          │                      │
│                ▼                          ▼                      │
│  ┌─────────────────────┐    ┌─────────────────────┐             │
│  │   双写处理           │    │   批量写入模块       │             │
│  │                     │    │  - 批量INSERT       │             │
│  │  1. 业务数据(可选)    │    │  - 冲突处理         │             │
│  │  2. 标记表(必须)      │    │  - 分库分表路由      │             │
│  │                     │    │                     │             │
│  │  ┌───────────────┐  │    │                     │             │
│  │  │ tdc_data_tag  │  │    │                     │             │
│  │  │ - id          │  │    │                     │             │
│  │  │ - user_id     │  │    │                     │             │
│  │  │ - order_id    │  │    │                     │             │
│  │  │ - data_tag    │  │    │                     │             │
│  │  │ - created_at  │  │    │                     │             │
│  │  └───────────────┘  │    │                     │             │
│  └─────────────────────┘    └─────────────────────┘             │
│                │                          │                      │
│                └──────────┬───────────────┘                      │
│                           ▼                                     │
│              ┌─────────────────────┐                            │
│              │    数据库连接池      │                            │
│              │  - 多实例管理        │                            │
│              │  - 多库多表路由      │                            │
│              └─────────────────────┘                            │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 两种任务类型

| 类型 | 用途 | 数据源 | 输出 |
|------|------|--------|------|
| `http_source` | 调用外部接口构造数据 | 外部HTTP接口 | 业务表（可选）+ 标记表 |
| `direct_insert` | 直接生成数据入库 | 本地数据模板 | 业务表 + 标记表 |

---

## 3. 数据模型设计

### 3.1 标记表（tdc_data_tag）

```sql
CREATE TABLE tdc_data_tag (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL COMMENT '业务用户ID',
    order_id VARCHAR(64) NOT NULL COMMENT '业务订单ID',
    data_tag VARCHAR(128) NOT NULL COMMENT '数据类型标签',
    task_id VARCHAR(64) NOT NULL COMMENT '任务ID，用于追溯',
    ext_info JSON COMMENT '扩展信息',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_order_id (order_id),
    INDEX idx_data_tag (data_tag),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB COMMENT='测试数据标记表';
```

### 3.2 任务执行日志表（tdc_task_log）

```sql
CREATE TABLE tdc_task_log (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    task_name VARCHAR(128) NOT NULL,
    task_type ENUM('http_source', 'direct_insert') NOT NULL,
    status ENUM('running', 'success', 'failed', 'partial') NOT NULL,
    total_count INT UNSIGNED DEFAULT 0,
    success_count INT UNSIGNED DEFAULT 0,
    failed_count INT UNSIGNED DEFAULT 0,
    error_msg TEXT,
    started_at DATETIME,
    finished_at DATETIME,
    INDEX idx_task_id (task_id),
    INDEX idx_status (status),
    INDEX idx_started_at (started_at)
) ENGINE=InnoDB COMMENT='任务执行日志';
```

---

## 4. 配置规范

### 4.1 任务配置（YAML）

**HTTP接口调用任务示例：**

```yaml
task_id: "order_full_flow"
task_name: "订单全流程数据构造"
task_type: "http_source"
schedule: "0 */6 * * *"

# 接口调用链（按顺序执行，支持上下文传递）
pipeline:
  - step_id: "create_user"
    name: "创建用户"
    http:
      url: "https://api.internal.com/user/create"
      method: POST
      body_template: |
        {
          "username": "{{ faker.name }}",
          "phone": "{{ faker.phone_number }}"
        }
    extract:
      user_id: "$.data.user_id"
      user_token: "$.data.token"

  - step_id: "create_order"
    name: "创建订单"
    http:
      url: "https://api.internal.com/order/create"
      method: POST
      headers:
        Authorization: "Bearer {{ context.user_token }}"
      body_template: |
        {
          "user_id": "{{ context.user_id }}",
          "product_id": "{{ faker.random_choice ['P001','P002'] }}",
          "quantity": {{ faker.random_int 1 5 }}
        }
    extract:
      order_id: "$.data.order_id"

  - step_id: "pay_order"
    name: "支付订单"
    condition: "{{ context.order_id is not none }}"
    http:
      url: "https://api.internal.com/order/pay"
      method: POST
      body_template: |
        {
          "order_id": "{{ context.order_id }}",
          "pay_amount": {{ faker.random_int 100 5000 }}
        }

tag_mapping:
  user_id: "{{ context.user_id }}"
  order_id: "{{ context.order_id }}"
  data_tag: "ORDER_FULL_FLOW_{{ now | format_date('%Y%m%d') }}"

target_db:
  instance: "biz_db_01"
```

**直接插入任务示例：**

```yaml
task_id: "user_data_init"
task_name: "初始化测试用户"
task_type: "direct_insert"
schedule: "0 */6 * * *"

data_template:
  table: "user_info"
  batch_size: 1000
  total_count: 10000

  fields:
    user_id:
      type: "faker"
      generator: "uuid4"
    username:
      type: "faker"
      generator: "name"
    status:
      type: "choice"
      values: [1, 2, 3]
      weights: [0.7, 0.2, 0.1]
    created_at:
      type: "function"
      expr: "datetime.now()"

  relations:
    - table: "user_account"
      count: 1
      mapping:
        user_id: "$parent.user_id"
        balance: "{{ faker.random_int 0 10000 }}"

target_db:
  instance: "user_db_master"
  database: "user_db"
  sharding_key: "user_id"
  sharding_count: 8
```

### 4.2 数据库实例配置（db.yaml）

```yaml
instances:
  biz_db_01:
    host: "mysql-biz-01.internal"
    port: 3306
    user: "${BIZ_DB_USER}"
    password: "${BIZ_DB_PASS}"
    pool_size: 10

  user_db_master:
    host: "mysql-user.internal"
    port: 3306
    user: "${USER_DB_USER}"
    password: "${USER_DB_PASS}"
    pool_size: 20
```

---

## 5. 技术实现

### 5.1 技术栈

| 层级 | 技术 | 说明 |
|------|------|------|
| 调度 | APScheduler | 支持Cron表达式 |
| HTTP | httpx | 异步HTTP客户端 |
| 模板 | Jinja2 | 模板渲染 |
| 数据生成 | Faker | 假数据生成 |
| 数据库 | SQLAlchemy + aiomysql | 异步ORM |
| 配置 | Pydantic + YAML | 类型安全配置 |

### 5.2 核心模块

```
tdc/
├── config/              # 配置管理
├── scheduler/           # 调度层
├── pipeline/            # HTTP管道执行
├── generator/           # 数据生成
├── storage/             # 存储层
└── core/                # 核心模型
```

### 5.3 CLI命令

```bash
# 启动调度器
tdc scheduler start --config-dir ./configs

# 立即执行任务
tdc task run --task-id order_full_flow --dry-run

# 查看任务历史
tdc task history --task-id order_full_flow --limit 10

# 数据查询
tdc data query --tag "ORDER_API_TEST" --start-date 2024-01-01

# 配置校验
tdc config validate --file ./configs/order_task.yaml
```

---

## 6. 部署方案

### 6.1 Docker部署

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY tdc/ ./tdc/
COPY configs/ ./configs/
ENV TDC_CONFIG_DIR=/app/configs
CMD ["python", "-m", "tdc.scheduler", "start"]
```

### 6.2 环境变量

| 变量 | 说明 |
|------|------|
| `TDC_CONFIG_DIR` | 配置文件目录 |
| `TDC_LOG_LEVEL` | 日志级别 |
| `*_DB_USER` | 数据库用户名 |
| `*_DB_PASS` | 数据库密码 |
| `HMAC_SECRET` | 接口签名密钥 |

---

## 7. 扩展性预留

### 7.1 分布式调度（未来）

- 当前：APScheduler内存调度
- 未来：Celery Beat + Redis 或 APScheduler + SQLAlchemyJobStore

### 7.2 插件机制

```python
# 自定义认证插件
@auth_plugin.register("custom_oauth")
class CustomOAuthAuth:
    def apply(self, request: Request):
        pass

# 自定义数据生成器
@generator.register("snowflake_id")
class SnowflakeGenerator:
    def generate(self):
        pass
```

---

## 8. 验收标准

- [ ] 支持Cron定时任务调度
- [ ] 支持HTTP接口链式调用（上下文传递）
- [ ] 支持独立标记表写入
- [ ] 支持多实例MySQL连接池
- [ ] 支持批量数据生成和插入
- [ ] 支持YAML配置化任务定义
- [ ] 提供CLI工具管理任务
- [ ] 支持任务执行日志记录

---

**设计者**: Claude
**审核状态**: 待审核
