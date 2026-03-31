# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

- **Name**: testdatacenter
- **Type**: Python project (Python 3.13)
- **IDE**: PyCharm/IntelliJ IDEA (configured via `.idea/`)
- **Status**: New/empty project - no source code or build configuration yet established

## Common Commands

Since this is a new Python project, you may need to set up:

```bash
# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies (once requirements.txt or pyproject.toml is created)
pip install -r requirements.txt

# Run Python scripts
python <script.py>

# Run tests (once test framework is configured)
pytest
python -m unittest discover -s tests
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
