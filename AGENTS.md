# AI Agent Guidelines for per-transformers

This document provides guidelines for AI agents working on the `per-transformers` repository. It covers the development workflow, conventions, and important commands specific to this project.

## Repository Overview

- **Purpose**: [To be filled in based on repository purpose]
- **Language**: Python 3.12.3
- **Package Manager**: `uv`
- **Testing Framework**: pytest with PySpark
- **Linting**: Ruff (via pre-commit hooks)
- **CI/CD**: Jenkins
- **Container Runtime**: Databricks Runtime 16.4 (for tests)

## Development Workflow

### 1. Before Starting Work

**Always ask for the Linear ticket ID** before implementing any feature, fix, or chore. This ticket ID is used for:
- Branch naming
- Commit messages
- PR titles and descriptions

### 2. Create a Feature Branch

Follow this naming convention based on the type of work:

```bash
# For new features
git checkout -b feat/<TICKET-ID>-short-description

# For bug fixes
git checkout -b fix/<TICKET-ID>-short-description

# For maintenance/chores
git checkout -b chore/<TICKET-ID>-short-description
```

**Example**: `feat/PER-123-add-album-metadata-support`

### 3. Development Steps

1. Make your changes following the codebase patterns
2. Run pre-commit hooks (see section below)
3. Run tests locally (see section below)
4. Commit with proper message format (see section below)
5. Push your branch
6. Create a Pull Request (see PR requirements below)

## Pre-commit Hooks

This repository uses Ruff for linting and formatting via pre-commit hooks.

### Running Pre-commit Hooks

```bash
# Run on all files
pre-commit run --all-files

# Pre-commit will automatically run on staged files when you commit
git commit -m "your message"
```

### What the Hooks Do

- **ruff**: Lints Python code and auto-fixes issues
- **ruff-format**: Formats Python code

Configuration is in `.pre-commit-config.yaml` using Ruff v0.9.6.

## Running Tests

### Prerequisites

1. **Python 3.12.3**: Install with `uv python install 3.12.3`
2. **uv**: Package manager
3. **AWS Credentials**: Configured for `tidal-backoffice-production--personalization` profile
4. **Docker**: For running tests in Databricks Runtime container

### Setup

```bash
# Install Python 3.12.3
uv python install 3.12.3

# Create virtual environment
uv venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
cd utils && sh install_deps.sh
```

### Environment Variables

When running tests locally (outside Docker), set:

```bash
export PYTHONPATH=src
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

### Running Tests

```bash
# Run tests locally (after setting environment variables)
python -m pytest

# Run specific test file
python -m pytest test/path/to/test_file.py

# Run specific test function
python -m pytest test/path/to/test_file.py::test_specific_function
```

**Note**: `make test` runs tests in a Docker container using Databricks Runtime 16.4 image, which mirrors the production environment.

## Commit Message Convention

Follow the Conventional Commits format:

```
<type>(<scope>): <TICKET-ID> <summary>

[optional body]

[optional footer]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `chore`: Maintenance, refactoring, dependencies
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `perf`: Performance improvements
- `refactor`: Code refactoring without functionality change

### Scopes (Examples)

- `writer`: Changes to writer classes
- `embedding`: Changes to embedding data classes
- `opensearch`: Changes to OpenSearch client
- `tests`: Test-related changes
- `deps`: Dependency changes
- `ci`: CI/CD changes

### Examples

```bash
# Feature commit
git commit -m "feat(embedding): PER-123 add album metadata support"

# Bug fix
git commit -m "fix(writer): PER-456 handle null values in track embeddings"

# Chore
git commit -m "chore(deps): PER-789 update pyspark to 3.5.2"

# Multiple line commit with body
git commit -m "refactor(opensearch): PER-234 improve connection pooling

Refactored the OpenSearch client to use connection pooling
for better performance under high load."
```

## Pull Request Requirements

### PR Title

Must match the commit message convention:

```
<type>(<scope>): <TICKET-ID> <summary>
```

### PR Description

Include the following sections:

```markdown
## Summary
Brief description of what this PR does and why.

## Testing
- [ ] Unit tests added/updated
- [ ] Tested locally with sample data
- [ ] Pre-commit hooks pass
- [ ] All tests pass

Describe specific testing performed.

## Risk/Rollout
- Risk level: [Low/Medium/High]
- Rollout plan: [Gradual/Immediate]
- Rollback plan: [Description]

## Linear Ticket
[Link to Linear ticket: PER-XXX]
```

### PR Size Guidelines

- **Preferred**: < 400 lines changed
- **Maximum**: < 800 lines changed
- For larger changes, consider breaking into multiple PRs

## Safety Guidelines

### No Secrets in Code

- Never commit AWS credentials, API keys, or secrets
- Use AWS profiles and environment variables
- Reference: AWS profile `tidal-backoffice-production--personalization`

### Dependency Management

- Justify any new dependencies in PR description
- Use version pinning in `pyproject.toml`
- Update lock file: `uv lock`

## Useful Commands

### Build and Clean

```bash
# Build the package (creates dist/jobs.zip and dist/main.py)
make build
```

### Dependencies

```bash
# Install dependencies from lock file
cd utils && sh install_deps.sh
```

## Repository-Specific Details

### AWS CodeArtifact

Dependencies are installed via AWS CodeArtifact. The URL is generated dynamically:

```bash
./utils/generate_codeartifact_url.sh tidal-backoffice-production--personalization
```

## Key Files Reference

- `pyproject.toml`: Python dependencies and Ruff configuration
- `Makefile`: Build, test, and deployment commands
- `.pre-commit-config.yaml`: Pre-commit hook configuration
- `src/main.py`: Entry point (copied to dist during build)
- `utils/install_deps.sh`: Dependency installation script
- `utils/generate_codeartifact_url.sh`: AWS CodeArtifact URL generator
