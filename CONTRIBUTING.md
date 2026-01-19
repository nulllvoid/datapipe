# Contributing to DataPipe

Thank you for your interest in contributing to DataPipe! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Go 1.21 or later
- Git

### Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/datapipe.git
   cd datapipe
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/nulllvoid/datapipe.git
   ```

## Making Changes

### Branch Naming

Use descriptive branch names:
- `feature/add-elasticsearch-fetcher`
- `fix/state-race-condition`
- `docs/update-readme`

### Code Style

- Follow standard Go conventions
- Run `go fmt` before committing
- Run `golangci-lint run` to check for issues
- Write descriptive commit messages

### Testing

All changes must include tests:

```bash
# Run all tests
go test -v ./...

# Run tests with race detection
go test -race ./...

# Check coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

Target coverage: 80%+ for new code.

### Test Style

Use table-driven tests with parallel execution:

```go
func TestMyFunction(t *testing.T) {
    t.Parallel()
    
    tests := []struct {
        name     string
        input    string
        expected string
    }{
        {"case one", "input1", "output1"},
        {"case two", "input2", "output2"},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()
            result := MyFunction(tt.input)
            if result != tt.expected {
                t.Errorf("got %v, want %v", result, tt.expected)
            }
        })
    }
}
```

## Pull Request Process

1. Ensure all tests pass
2. Update documentation if needed
3. Add entries to CHANGELOG.md (if applicable)
4. Submit a pull request with a clear description
5. Wait for review and address feedback

### PR Title Format

Use conventional commits format:
- `feat: add elasticsearch fetcher`
- `fix: resolve race condition in state`
- `docs: update installation instructions`
- `test: add pipeline timeout tests`
- `refactor: simplify stage execution`

## Code Review

All submissions require review. We aim to:
- Respond within 2 business days
- Provide constructive feedback
- Merge approved PRs promptly

## Reporting Issues

When reporting issues, please include:
- Go version (`go version`)
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant code snippets

## Questions

For questions, open a GitHub Discussion or issue.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

