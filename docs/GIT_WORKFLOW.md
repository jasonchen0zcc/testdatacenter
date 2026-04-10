# Git 工作流程

本文档定义 TDC 项目的标准 Git 工作流程，所有代码修改必须遵循此流程。

## 分支策略

采用 **Git Flow 简化版**：

- `main` - 生产分支，永远可部署
- `feature/*` - 功能分支，从 main 创建，完成后合并回 main

## 工作流程

### 1. 开始新功能

```bash
# 确保本地 main 是最新的
git checkout main
git pull origin main

# 创建功能分支（使用有意义的名称）
git checkout -b feature/assertion-validation
# 或
git checkout -b fix/database-connection-timeout
```

分支命名规范：
- `feature/<简短描述>` - 新功能
- `fix/<简短描述>` - Bug 修复
- `refactor/<简短描述>` - 代码重构
- `docs/<简短描述>` - 文档更新

### 2. 开发过程中的提交

```bash
# 查看修改状态
git status

# 添加特定文件（不要 git add .）
git add tdc/core/assertions.py tdc/config/models.py

# 创建提交（遵循提交信息规范）
git commit -m "feat(assertion): add HTTP response validation

- Add AssertionConfig model for coarse-grained assertions
- Implement status_code, json_path, json_success validation
- Integrate into PipelineEngine.execute_step

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

#### 提交信息规范

格式：`<type>(<scope>): <subject>`

类型 (type)：
- `feat` - 新功能
- `fix` - Bug 修复
- `refactor` - 代码重构
- `docs` - 文档更新
- `test` - 测试相关
- `chore` - 构建/工具/配置

示例：
```
feat(pipeline): add concurrent execution support
fix(engine): resolve token extraction failure
refactor(config): simplify template loader logic
docs(api): update authentication examples
test(assertions): add validation test cases
chore(git): remove cached files from tracking
```

### 3. 完成功能开发

```bash
# 1. 确保所有测试通过
pytest

# 2. 检查代码质量
black tdc/ tests/
ruff check tdc/ tests/

# 3. 查看提交历史
git log --oneline main..HEAD

# 4. 合并到 main（使用 --no-ff 创建合并提交）
git checkout main
git pull origin main
git merge --no-ff feature/assertion-validation

# 5. 推送
git push origin main

# 6. 删除功能分支
git branch -d feature/assertion-validation
```

## 禁止事项

❌ **永远不要**：
- 直接提交到 `main` 分支（紧急热修复除外）
- 使用 `git add .` 添加所有文件
- 提交包含敏感信息的文件（密码、密钥）
- 提交大型二进制文件
- 提交 `__pycache__`、`.idea/` 等缓存/IDE文件

## 常用命令速查

```bash
# 查看状态
git status
git diff --stat

# 添加和提交
git add <file1> <file2>
git commit -m "type(scope): message"

# 分支操作
git branch                    # 列出分支
git checkout -b <branch>      # 创建并切换分支
git checkout <branch>         # 切换分支
git branch -d <branch>        # 删除分支

# 查看历史
git log --oneline -10         # 最近10条
git log --oneline --graph     # 图形化显示

# 撤销操作
git restore <file>            # 撤销文件修改
git restore --staged <file>   # 取消暂存

# 清理（已跟踪但应忽略的文件）
git rm -r --cached <path>
```

## 紧急情况处理

### 误提交了敏感信息

```bash
# 如果尚未推送
git reset --soft HEAD~1
git restore --staged <file>
git checkout -- <file>
# 编辑文件移除敏感信息后再提交

# 如果已推送（需要 force push，谨慎操作）
git filter-branch --force --index-filter \
  'git rm --cached --ignore-unmatch <file>' HEAD
git push --force
```

### 误提交了缓存文件

```bash
# 从跟踪中移除但不删除本地文件
git rm -r --cached __pycache__/
git commit -m "chore: remove cached files"
```

## 与 Claude Code 协作

当使用 Claude Code 开发时：

1. **开始前**：确认当前分支 `git branch`
2. **开发中**：Claude 会自动创建规范化的提交
3. **完成后**：审查提交历史，确保符合规范
4. **合并时**：Claude 使用 `finishing-a-development-branch` skill 完成流程
