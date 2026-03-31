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

### 2. 配置数据库

编辑 `configs/db.yaml`，配置MySQL实例。

### 3. 启动调度器

```bash
tdc scheduler start --config-dir ./configs
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
