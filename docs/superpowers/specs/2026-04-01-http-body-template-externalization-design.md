# HTTP Body 模板外置化设计

**日期**: 2026-04-01
**状态**: 已批准，待实现

## 背景

当前 TDC 项目中，HTTP 请求的 `body_template` 以内联多行字符串形式存放在 `/configs/tasks/*.yaml` 文件中。当请求体比较复杂时，YAML 文件变得难以维护。本设计将 `body_template` 支持外置到独立的 JSON 文件中。

## 目标

1. 支持按 Task 组织模板文件目录
2. 支持简写、相对路径、完整路径多种引用方式
3. 向后兼容现有内联模板配置

## 目录结构

```
configs/
├── tasks/
│   ├── example_order_flow.yaml      # 主配置（瘦身）
│   ├── example_insert.yaml
│   └── ...
├── templates/                        # 新增：模板根目录
│   └── example_order_flow/          # 与 task_id 同名
│       ├── create_user.json
│       ├── create_order.json
│       └── update_inventory.json
└── db.yaml
```

## 配置语法

### 方式1：简写（推荐）

```yaml
pipeline:
  - step_id: "create_user"
    http:
      url: "https://api.example.com/users"
      method: POST
      body_template: "create_user.json"
      # 自动解析为：configs/templates/{task_id}/create_user.json
```

### 方式2：相对路径

```yaml
pipeline:
  - step_id: "create_order"
    http:
      body_template: "./orders/create_order.json"
      # 解析为：configs/templates/{task_id}/orders/create_order.json
```

### 方式3：跨 Task 引用

```yaml
pipeline:
  - step_id: "notify"
    http:
      body_template: "templates/shared/webhook_payload.json"
      # 按原路径解析（相对项目根目录）
```

### 方式4：内联模板（向后兼容）

```yaml
pipeline:
  - step_id: "fallback"
    http:
      body_template: |
        {
          "message": "inline template still works"
        }
```

## 模板文件格式

模板文件使用 JSON 格式，内部支持 Jinja2 模板语法：

```json
{
  "username": "{{ faker.name }}",
  "email": "{{ faker.email }}",
  "phone": "{{ faker.phone_number }}",
  "register_time": "{{ now.isoformat() }}",
  "source": "TDC_AUTO_GENERATE"
}
```

## 解析规则

当 `body_template` 值为：

1. **以 `.json` 结尾** → 尝试作为文件路径解析
   - 纯文件名（不含 `/`）→ `templates/{task_id}/{value}`
   - 以 `./` 开头 → `templates/{task_id}/{value_without_dot_slash}`
   - 其他路径 → 按原路径解析（相对项目根目录）
2. **文件存在** → 读取文件内容作为模板
3. **文件不存在** 或 **不以 `.json` 结尾** → 视为内联模板（向后兼容）

## 模块变更

### 新增：`tdc/config/template_loader.py`

```python
class TemplateLoader:
    """模板加载器，负责解析 body_template 路径并加载内容"""

    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)
        self.template_dir = self.config_dir.parent / "templates"

    def load_body_template(self, template_ref: str, task_id: str) -> str:
        """
        加载 body_template 内容

        Args:
            template_ref: 配置中的 body_template 值
            task_id: 当前任务的 task_id，用于解析简写

        Returns:
            模板内容字符串
        """
        ...
```

### 修改：`tdc/pipeline/http_client.py`

在 HTTP 请求发送前，调用 `TemplateLoader` 解析模板引用，获取最终模板字符串后传给 Jinja2 渲染。

### 不变：`tdc/config/models.py`

`HTTPConfig.body_template` 保持为 `Optional[str]`，无需修改模型定义。

## 错误处理

| 场景 | 行为 |
|------|------|
| 模板文件不存在且以 `.json` 结尾 | 抛出 `FileNotFoundError`，任务失败 |
| 模板文件读取失败（权限等） | 抛出 `IOError`，任务失败 |
| 不以 `.json` 结尾 | 视为内联模板，按原逻辑处理 |
| JSON 模板语法错误 | 在 Jinja2 渲染阶段抛出异常 |

## 测试策略

1. **单元测试**：`TemplateLoader` 的各种路径解析场景
2. **集成测试**：完整 pipeline 执行，验证模板加载和渲染
3. **兼容性测试**：现有内联模板配置仍可正常运行

## 迁移指南

现有配置无需修改即可继续运行。如需迁移到外置模板：

1. 创建 `configs/templates/{task_id}/` 目录
2. 将 YAML 中的 `body_template` 内容复制到 `.json` 文件
3. 将 YAML 中的 `body_template` 替换为文件名
