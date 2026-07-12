---
title: "Contributing to bunqueue: Development & PR Guide"
description: "Contribute to bunqueue: dev environment setup, coding standards, testing guidelines, and pull request workflow for the Bun job queue."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/contributing.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">project · contributing</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Contribute to bunqueue, ship a <em>PR.</em></h1>
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

There are three suites. All three must pass before any change lands:

```bash
# Unit tests (~5000 tests)
bun test

# TCP integration tests (~50 suites, spawns a real server)
bun scripts/tcp/run-all-tests.ts

# Embedded integration tests (~35 suites)
bun scripts/embedded/run-all-tests.ts
```

Other useful invocations:

```bash
# Run a specific test file
bun test test/queueManager.test.ts

# Run with coverage
bun test --coverage
```

Note: `bun test` preloads `test/preload.ts`, which sets `BUNQUEUE_EMBEDDED=1`. Tests that need real TCP behavior must opt out with an explicit `embedded: false` and spawn a server.

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
5. Run all three test suites (`bun test`, `bun scripts/tcp/run-all-tests.ts`, `bun scripts/embedded/run-all-tests.ts`) and `bun run check:biome`
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
- `src/client/queue/queue.ts` - Client Queue class
- `src/client/worker/worker.ts` - Client Worker class

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

Releases are handled by the maintainer. Every release:

1. Bumps the patch version in `package.json`
2. Updates the changelog (`docs/src/content/docs/changelog.md`)
3. Publishes to npm with `bun publish`

## Getting Help

- [GitHub Discussions](https://github.com/egeominotti/bunqueue/discussions)
- [GitHub Issues](https://github.com/egeominotti/bunqueue/issues)

## Recognition

Contributors are listed in:
- GitHub contributors page
- README.md acknowledgments

Thank you for contributing!

:::tip[Related]
- [Architecture & System Design](/architecture/) - Understand the codebase
- [Security Best Practices](/security/) - Security guidelines
:::
