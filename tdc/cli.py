import asyncio
import os
from pathlib import Path
import click

from tdc.scheduler.core import TDScheduler
from tdc.config.loader import ConfigLoader, load_dotenv
from tdc.core.logger import setup_logging, get_logger

# Auto-load .env file on module import
load_dotenv()

# Setup logging (50MB rotation, daily retention)
setup_logging(
    log_dir="logs",
    max_bytes=50 * 1024 * 1024,  # 50MB
    backup_count=0,  # Only keep current file
    log_level=os.environ.get("TDC_LOG_LEVEL", "INFO")
)

logger = get_logger()


@click.group()
@click.option("--config-dir", envvar="TDC_CONFIG_DIR", default="./configs")
@click.pass_context
def main(ctx, config_dir):
    """TDC (Test Data Center) - 测试数据生成中心"""
    ctx.ensure_object(dict)
    ctx.obj["config_dir"] = config_dir


@main.command()
@click.pass_context
def scheduler_start(ctx):
    """启动调度器"""
    config_dir = ctx.obj["config_dir"]

    async def run():
        scheduler = TDScheduler(config_dir)
        await scheduler.initialize()
        scheduler.load_tasks()
        scheduler.start()
        logger.info("scheduler_running", config_dir=config_dir)

        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            scheduler.shutdown()

    asyncio.run(run())


@main.group()
def task():
    """任务管理命令"""
    pass


@task.command("list")
@click.option("--enabled-only", is_flag=True)
@click.pass_context
def task_list(ctx, enabled_only):
    """列出所有任务"""
    config_dir = ctx.obj["config_dir"]
    loader = ConfigLoader(config_dir)

    try:
        tasks = loader.load_task_configs()
        click.echo(f"{'Task ID':<30} {'Name':<30} {'Type':<20} {'Schedule':<20} {'Enabled'}")
        click.echo("-" * 120)

        for t in tasks:
            if enabled_only and not t.enabled:
                continue
            click.echo(f"{t.task_id:<30} {t.task_name:<30} {t.task_type.value:<20} {t.schedule:<20} {t.enabled}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Exit(1)


@task.command("run")
@click.option("--task-id", required=True)
@click.option("--dry-run", is_flag=True)
@click.pass_context
def task_run(ctx, task_id, dry_run):
    """立即执行指定任务"""
    config_dir = ctx.obj["config_dir"]

    async def run():
        scheduler = TDScheduler(config_dir)
        await scheduler.initialize()
        logger.info("running_task", task_id=task_id, dry_run=dry_run)
        result = await scheduler.run_task_now(task_id)
        click.echo(f"Task completed: {result}")

    asyncio.run(run())


@main.command()
@click.option("--file")
@click.pass_context
def config_validate(ctx, file):
    """验证配置文件"""
    config_dir = ctx.obj["config_dir"]
    loader = ConfigLoader(config_dir)

    try:
        if file:
            import yaml
            from tdc.config.models import TaskConfig

            with open(file) as f:
                data = yaml.safe_load(f)
                TaskConfig(**data)
            click.echo(f"Config file is valid: {file}")
        else:
            loader.load_task_configs()
            click.echo("All configs are valid")
    except Exception as e:
        click.echo(f"Config validation failed: {e}", err=True)
        raise click.Exit(1)


if __name__ == "__main__":
    main()
