# Contributing to P2P File Transfer

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites
- Rust 1.70 or later
- Cargo
- Git

### Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/yourusername/p2p-transfer.git
   cd p2p-transfer
   ```

3. Build the project:
   ```bash
   cargo build
   ```

4. Run tests:
   ```bash
   cargo test
   ```

## Project Structure

```
p2p-transfer/
├── src/main.rs           # Binary entry point (delegates to p2p-cli or p2p-gui)
├── p2p-core/             # Core library: identity, TLS, QUIC, handshake,
│   │                     # session, transfer engine, traversal, history, ...
│   └── src/
│       ├── identity.rs, tls.rs, known_peers.rs
│       ├── protocol.rs, handshake.rs, session.rs
│       ├── transfer_file.rs, transfer_folder.rs
│       ├── compression.rs, verification.rs, bandwidth.rs
│       ├── traversal/{mod.rs, stun.rs, punch.rs}
│       └── network/{quic.rs, framing.rs, udp.rs}
├── p2p-cli/              # clap-based CLI
├── p2p-gui/              # Iced 0.12 GUI
├── p2p-rendezvous/       # Matchmaking + relay (`rendezvousd` binary)
│   └── src/{lib.rs, protocol.rs, server.rs, relay.rs, client.rs,
│            bin/rendezvousd.rs}
├── tests/                # Workspace integration + loopback tests
└── README.md, DESIGN.md, TODO.md, CHANGELOG.md
```

Per-crate developer guidance lives in each crate's `AGENTS.md` (root,
`p2p-core/`, `p2p-cli/`, `p2p-gui/`, `p2p-rendezvous/`). Read those
before touching a crate.

## Coding Guidelines

### Style
- Follow Rust standard formatting (`cargo fmt`)
- Use `cargo clippy` to catch common mistakes
- Write idiomatic Rust code
- Add documentation comments for public APIs

### Naming Conventions
- `snake_case` for functions, variables, modules
- `PascalCase` for types, traits, enums
- `SCREAMING_SNAKE_CASE` for constants
- Use descriptive names

### Error Handling
- Use the `Result` type for operations that can fail
- Use custom error types from `error.rs`
- Provide context with error messages
- Don't panic in library code

### Testing
- Write unit tests for all modules
- Write integration tests for features
- Use `#[cfg(test)]` modules for unit tests
- Aim for high test coverage

### Documentation
- Add doc comments (`///`) for public items
- Include examples in doc comments
- Update README.md for user-facing changes
- Update DESIGN.md for architectural changes

## Workflow

### Branching
- `main` - stable releases
- `develop` - development branch
- `feature/*` - new features
- `bugfix/*` - bug fixes
- `hotfix/*` - urgent fixes

### Making Changes

1. Create a branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes
3. Format and lint:
   ```bash
   cargo fmt
   cargo clippy -- -D warnings
   ```

4. Run tests:
   ```bash
   cargo test
   ```

5. Commit with clear messages:
   ```bash
   git commit -m "feat: add UDP discovery implementation"
   ```

### Commit Messages

Follow conventional commits:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test changes
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `chore:` - Maintenance tasks

### Pull Requests

1. Push your branch:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Create a Pull Request on GitHub
3. Fill out the PR template
4. Link related issues
5. Wait for review

### Code Review
- Be respectful and constructive
- Explain reasoning for changes
- Address all feedback
- Keep discussions focused

## Testing

### Running Tests
```bash
# All tests
cargo test

# Specific test
cargo test test_name

# With output
cargo test -- --nocapture

# Integration tests only
cargo test --test '*'
```

### Writing Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        // Arrange
        let input = 42;
        
        // Act
        let result = function_under_test(input);
        
        // Assert
        assert_eq!(result, expected);
    }
}
```

## Performance

### Profiling
```bash
# CPU profiling
cargo flamegraph --bin p2p-transfer

# Memory profiling
cargo instruments -t Allocations
```

### Benchmarking
```bash
cargo bench
```

## Documentation

### Building Docs
```bash
cargo doc --open
```

### Doc Comments
```rust
/// Brief description.
///
/// Longer description with details.
///
/// # Examples
///
/// ```
/// use p2p_core::example;
/// let result = example::function();
/// assert_eq!(result, expected);
/// ```
///
/// # Errors
///
/// Returns an error if...
///
/// # Panics
///
/// Panics if...
pub fn function() -> Result<()> {
    // ...
}
```

## Release Process

1. Update version in `Cargo.toml`
2. Update `CHANGELOG.md`
3. Create a tag: `git tag v0.1.0`
4. Push tag: `git push origin v0.1.0`
5. Create GitHub release
6. Publish to crates.io: `cargo publish`

## Getting Help

- Open an issue for bugs
- Start a discussion for questions
- Join our chat (TBD)
- Check existing issues and PRs

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Code of Conduct

### Our Pledge
We pledge to make participation in our project a harassment-free experience for everyone.

### Our Standards
- Be respectful and inclusive
- Accept constructive criticism
- Focus on what's best for the community
- Show empathy

### Enforcement
Report issues to the maintainers. All complaints will be reviewed and investigated.

---

Thank you for contributing! 🎉
