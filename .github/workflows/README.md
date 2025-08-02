# GitHub Actions Workflows

This repository includes several GitHub Actions workflows to ensure code quality, security, and reliable releases.

## Workflows

### 🔄 CI Pipeline (`ci.yml`)

**Triggers:** Every push to `master`/`main`, every pull request

**Jobs:**

- **Test**: Runs on Ubuntu, Windows, macOS with stable and beta Rust
  - Code formatting check (`cargo fmt --check`)
  - Clippy linting (`cargo clippy`)
  - Build verification
  - Full test suite execution
  - Examples compilation
  - Benchmark compilation check

- **Miri**: Memory safety testing with Rust's Miri interpreter
- **MSRV**: Minimum Supported Rust Version testing (1.88.0)
- **Documentation**: Documentation build with warning checks
- **Security Audit**: Dependency vulnerability scanning with `cargo-audit`
- **Code Coverage**: Coverage analysis with `cargo-llvm-cov` and Codecov upload

### 🌙 Nightly Testing (`nightly.yml`)

**Triggers:** Daily at 2 AM UTC, manual dispatch

**Jobs:**

- **Nightly Tests**: Extended testing with Rust nightly
- **Fuzzing**: Property-based testing with extended iterations
- **Memory Analysis**: Memory leak detection with Valgrind

### 🔒 Security Scanning (`codeql.yml`)

**Triggers:** Push to `master`/`main`, pull requests, weekly schedule

**Jobs:**

- **CodeQL Analysis**: GitHub's semantic code analysis for security vulnerabilities
- Extended security and quality queries

### 📦 Release Pipeline (`release.yml`)

**Triggers:** Git tags starting with `v*`

**Jobs:**

- **Create Release**: GitHub release creation
- **Publish**: Automated publishing to crates.io

## Branch Protection

For optimal security and code quality, consider enabling these branch protection rules on `master`/`main`:

- Require status checks to pass before merging
- Require branches to be up to date before merging
- Required status checks:
  - `Test (ubuntu-latest, stable)`
  - `Test (windows-latest, stable)`
  - `Test (macos-latest, stable)`
  - `Miri`
  - `Minimum Rust Version`
  - `Documentation`
  - `Security Audit`
  - `CodeQL`

## Required Secrets

For full functionality, add these secrets to your repository:

### For Code Coverage

- `CODECOV_TOKEN`: Token from codecov.io for coverage reporting

### For Releases

- `CARGO_REGISTRY_TOKEN`: Token for publishing to crates.io
- `GITHUB_TOKEN`: Automatically provided by GitHub Actions

## Dependencies

### Automatic Updates

- **Dependabot**: Configured to update Cargo dependencies weekly
- **GitHub Actions**: Automated updates for workflow actions

### Security

- **cargo-audit**: Daily vulnerability scanning
- **CodeQL**: Weekly deep security analysis

## Performance

### Caching

- Cargo registry and git dependencies cached across runs
- Separate cache keys per OS and Rust version
- Restore keys for partial cache hits

### Matrix Strategy

- Parallel testing across multiple platforms
- Reduced redundancy for beta testing on non-Ubuntu platforms

## Monitoring

### Test Results

- Test failures reported in PR checks
- Coverage trends tracked in Codecov
- Performance regression detection via benchmarks

### Security

- Vulnerability alerts via GitHub Security tab
- Weekly CodeQL scans
- Automated dependency updates

## Local Testing

To run the same checks locally:

```bash
# Formatting
cargo fmt --all -- --check

# Linting
cargo clippy --all-targets --all-features -- -D warnings

# Testing
cargo test --all-features

# Documentation
cargo doc --all-features --no-deps

# Security audit
cargo install cargo-audit
cargo audit

# Coverage (requires cargo-llvm-cov)
cargo install cargo-llvm-cov
cargo llvm-cov --all-features --workspace
```

## Troubleshooting

### Common Issues

1. **Formatting failures**: Run `cargo fmt --all` locally
2. **Clippy warnings**: Address warnings shown by `cargo clippy`
3. **Test failures**: Ensure all tests pass with `cargo test --all-features`
4. **MSRV failures**: Code may use features newer than Rust 1.88.0

### Performance

- Workflows use caching to reduce runtime
- Matrix builds run in parallel for faster feedback
- Nightly jobs run separately to avoid blocking PRs
