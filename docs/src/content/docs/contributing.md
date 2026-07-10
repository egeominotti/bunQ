---
title: "Contributing to bunqueue: Development & PR Guide"
description: "Contribute to bunqueue: dev environment setup, coding standards, testing guidelines, and pull request workflow for the Bun job queue."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">project · contributing</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Make bunqueue <em>better.</em></h1>
  <p class="bq-hero-sub">Dev environment setup, coding standards, testing guidelines and the pull request workflow. Everything you need to land a change, whatever your experience level.</p>
</div>

## Code of Conduct

Be respectful and inclusive. We welcome contributors of all backgrounds and experience levels.

## Getting Started

### Prerequisites

- [Bun](https://bun.sh) v1.3.9+
- Git
- A GitHub account

### Setup

```bash
# Fork the repo on GitHub, then:
git clone https://github.com/YOUR_USERNAME/bunqueue.git
cd bunqueue
bun install
```

### Running Tests

```bash
# Run all tests
bun test

# Run specific test file
bun test test/queue.test.ts

# Run with coverage
bun test --coverage
```

### Code Style

We use [Biome](https://biomejs.dev) (one tool for linting + formatting):

```bash
# Lint
bun run lint

# Format code
bun run format

# Lint + format check in one pass (what CI / the pre-commit hook run)
bun run check:biome
```

## Making Changes

### Branch Naming

- `feat/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation
- `refactor/description` - Code refactoring
- `test/description` - Test additions

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add stall detection for workers
fix: resolve memory leak in event listeners
docs: update API reference
refactor: simplify batch operations
test: add DLQ filtering tests
```

### Pull Request Process

1. Create a feature branch
2. Make your changes
3. Add/update tests
4. Update documentation
5. Run `bun test` and `bun run lint`
6. Push and create a PR

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation

## Testing
How was this tested?

## Checklist
- [ ] Tests pass
- [ ] Linting passes
- [ ] Documentation updated
```

## Project Structure

```
src/
├── cli/             # CLI commands
├── client/          # Embedded client SDK
├── domain/          # Core business logic
├── application/     # Use cases
├── infrastructure/  # External services
└── shared/          # Utilities
```

### Key Files

- `src/domain/queue/shard.ts` - Queue sharding logic
- `src/application/queueManager.ts` - Central coordinator
- `src/client/queue.ts` - Client Queue class
- `src/client/worker.ts` - Client Worker class

## Architecture Guidelines

### File Size
- **Max 300 lines per file**
- Split if larger

### Lock Order
1. `jobIndex`
2. `completedJobs`
3. `shards[N]`
4. `processingShards[N]`

### Memory Management
- Use bounded collections
- Clean up event listeners
- Release resources in shutdown

## Testing Guidelines

### Test Structure

```typescript
describe('Feature', () => {
  beforeEach(() => {
    // Setup
  });

  afterEach(() => {
    // Cleanup
  });

  it('should do something', () => {
    // Test
  });
});
```

### What to Test

- Happy path
- Edge cases
- Error handling
- Concurrent operations

## Documentation

### Code Comments

```typescript
/** Brief description */
function simpleFunction() {}

/**
 * Longer description for complex functions
 * @param input - Description
 * @returns Description
 */
function complexFunction(input: string): Result {}
```

### README Updates

Update README.md for:
- New features
- Changed APIs
- New environment variables

## Release Process

1. Update version in `package.json`
2. Update CHANGELOG.md
3. Create PR titled `release: v2.x.x`
4. After merge, tag and publish

## Getting Help

- [GitHub Discussions](https://github.com/egeominotti/bunqueue/discussions)
- [Discord](https://discord.gg/bunqueue)
- [Twitter](https://twitter.com/bunqueue)

## Recognition

Contributors are listed in:
- GitHub contributors page
- README.md acknowledgments

Thank you for contributing!

:::tip[Related]
- [Architecture & System Design](/architecture/) - Understand the codebase
- [Security Best Practices](/security/) - Security guidelines
:::
