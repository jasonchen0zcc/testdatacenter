from pathlib import Path


class TemplateLoader:
    """模板加载器，负责解析 body_template 路径并加载内容

    支持三种引用方式：
    1. 简写："create_user.json" -> templates/{task_id}/create_user.json
    2. 相对路径："./orders/create.json" -> templates/{task_id}/orders/create.json
    3. 完整路径："templates/shared/common.json" -> 相对 config_dir 解析

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
