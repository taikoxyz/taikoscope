set shell := ["bash", "-cu"]
set dotenv-load := true

# common configuration

# display a help message about available commands
default:
    @just --list --unsorted

# start the Taikoscope binary for local development (optimized for fast compilation)
dev:
    ENV_FILE=hekla.env ENABLE_DB_WRITES=false ENABLE_GAP_DETECTION=false INSTATUS_MONITORS_ENABLED=false \
    cargo run --profile dev-fast --bin taikoscope -- \
    --gap-min-l1-block 1 \
    --gap-min-l2-block 1

# start the API server for local development (optimized for fast compilation)
dev-api:
    ENV_FILE=hekla.env cargo run --profile dev-fast --bin api-server -- \
    --gap-min-l1-block 1 \
    --gap-min-l2-block 1

# start the API server for mainnet (optimized for fast compilation)
mainnet-api:
    ENV_FILE=mainnet.env cargo run --profile dev-fast --bin api-server -- \
    --gap-min-l1-block 1 \
    --gap-min-l2-block 1

# Check code (fastest compile check without codegen)
check:
    cargo check --workspace --all-targets

# Build workspace (optimized for faster compilation)
build:
    cargo build --profile dev-fast --workspace


# run all recipes required to pass CI workflows (optimized for faster compilation)
ci:
    @just fmt lint lint-dashboard test check-dashboard test-dashboard

# smart CI that only runs relevant tooling based on changed files
ci-smart:
    #!/usr/bin/env bash
    set -euo pipefail

    # Check if there are any changes in dashboard/ directory
    dashboard_changes=$(git diff --name-only HEAD~1 2>/dev/null | grep -c "^dashboard/" || echo "0")
    # Check if there are any changes outside dashboard/ directory (Rust code)
    rust_changes=$(git diff --name-only HEAD~1 2>/dev/null | grep -v "^dashboard/" | wc -l | tr -d ' ')

    # If no git history (new repo), run everything
    if ! git rev-parse HEAD~1 >/dev/null 2>&1; then
        echo "No git history found, running all CI checks..."
        just ci-rust ci-dashboard
        exit 0
    fi

    # Run appropriate CI based on changes
    if [[ "$rust_changes" -gt 0 ]] && [[ "$dashboard_changes" -gt 0 ]]; then
        echo "Changes detected in both Rust and dashboard code, running all CI checks..."
        just ci-rust ci-dashboard
    elif [[ "$rust_changes" -gt 0 ]]; then
        echo "Changes detected in Rust code only, running Rust CI checks..."
        just ci-rust
    elif [[ "$dashboard_changes" -gt 0 ]]; then
        echo "Changes detected in dashboard code only, running dashboard CI checks..."
        just ci-dashboard
    else
        echo "No changes detected, skipping CI checks..."
    fi

# run CI checks for Rust code only (optimized for faster compilation)
ci-rust:
    @just fmt lint test

# run CI checks for dashboard code only
ci-dashboard:
    @just lint-dashboard check-dashboard test-dashboard

# run tests (optimized for faster compilation during development)
test:
    cargo nextest run --cargo-profile dev-fast --workspace --all-targets

# run collection of clippy lints (optimized for faster compilation)
lint:
    RUSTFLAGS="-D warnings" cargo clippy --profile dev-fast --examples --tests --benches --all-features --locked

# format the code using the nightly rustfmt (as we use some nightly lints)
fmt:
    cargo +nightly fmt --all


# --- Dashboard ---

# install dashboard dependencies
install-dashboard:
    cd dashboard && npm install

# start the dashboard dev server
dev-dashboard:
    cd dashboard && VITE_API_BASE="http://localhost:3000" npm run dev

# start the dashboard dev server for mainnet (targets local API)
mainnet-dashboard:
    cd dashboard && VITE_API_BASE="http://localhost:3000" VITE_NETWORK_NAME="mainnet" npm run dev

# build the dashboard for production
build-dashboard:
    cd dashboard && npm run build

# run TypeScript type checks
check-dashboard:
    cd dashboard && npm run check

# run dashboard tests
test-dashboard:
    cd dashboard && npm run test

# lint dashboard files for trailing whitespace
lint-dashboard:
    cd dashboard && npm run lint:whitespace && npm run lint:dashboard


# build and push the unified taikoscope docker image (defaults to arm64/Graviton)
build-taikoscope tag='latest' platform='linux/arm64':
    docker buildx build \
        --label "org.opencontainers.image.commit=$(git rev-parse --short HEAD)" \
        --platform {{platform}} \
        --file Dockerfile.taikoscope \
        --tag ghcr.io/chainbound/taikoscope:{{tag}} \
        --push .

# build and push the api docker image (defaults to arm64/Graviton)
build-api tag='latest' platform='linux/arm64':
    docker buildx build \
        --label "org.opencontainers.image.commit=$(git rev-parse --short HEAD)" \
        --platform {{platform}} \
        --file Dockerfile.api \
        --tag ghcr.io/chainbound/taikoscope-api:{{tag}} \
        --push .

# build and push all docker images
build-all tag='latest' platform='linux/arm64':
    @echo "Building taikoscope image..."
    docker buildx build \
        --label "org.opencontainers.image.commit=$(git rev-parse --short HEAD)" \
        --platform {{platform}} \
        --file Dockerfile.taikoscope \
        --tag ghcr.io/chainbound/taikoscope:{{tag}} \
        --push .
    @echo "Building taikoscope-api image..."
    docker buildx build \
        --label "org.opencontainers.image.commit=$(git rev-parse --short HEAD)" \
        --platform {{platform}} \
        --file Dockerfile.api \
        --tag ghcr.io/chainbound/taikoscope-api:{{tag}} \
        --push .
