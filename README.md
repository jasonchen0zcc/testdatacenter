# TDC (Test Data Center) - 测试数据生成中心

一个支持定时调度、HTTP接口链调用、数据模板生成、MySQL批量写入的测试数据生成中心。

## 特性

- **定时调度**: 基于Cron表达式的任务调度
- **HTTP管道**: 支持多接口链式调用，上下文传递
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

### 4. 启动调度器

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

## 项目结构

```
tdc/
├── core/           # 核心模型和常量
├── config/         # 配置管理
├── scheduler/      # 调度器
├── pipeline/       # HTTP管道执行
├── generator/      # 数据生成
├── storage/        # 存储层
└── cli.py          # 命令行入口
```
