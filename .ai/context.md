# AGENTS.md

Zyn is a network fileserver. Repo is multi-language with three independent build chains:

- `zyn/` — Rust server (Cargo, `Cargo.toml`, entrypoint `zyn/src/main.rs`, server code under `zyn/src/node/`).
- `py/` — Python library + clients (`zyn-shell`, `zyn-cli`, `zyn-webserver`); installed editable via `pip install -e py`.
- `js/` — Web client (esbuild + tailwind). Bundled output is written **into** `py/zyn/client/zyn-web-static/zyn.{js,css}` and shipped by `py/setup.py` as data files. Do not edit those generated files.
- `tests/` — Python end-to-end / system tests that spawn the Rust server binary.

There is no top-level Makefile or CI config. The de-facto task runner is the shell scripts in `vm/development/files/` (installed into `/usr/bin/` by the Vagrant VM as `zyn-*`). Outside the VM, invoke them directly.

## Commands

Build/test commands assume the Vagrant dev VM (`vm/development`) where `ZYN_PROJECT_ROOT=/zyn` and `CARGO_TARGET_DIR=/tmp/cargo-build` are exported via `.bashrc`. If you run outside the VM, set these yourself or call `cargo`/`pytest`/`npm` directly in the right subdir.

- Rust server build: `zyn-build server [cargo-args]` → `cargo build` in `zyn/`.
- Rust unit tests: `zyn-test server-unit-tests` → `cargo test` in `zyn/`. Test modules live in `zyn/src/node/test_*.rs` and are gated by `#[cfg(test)]` in `zyn/src/node/mod.rs`.
- System tests: `zyn-test server-system-tests [pytest-args]` → runs only a fixed subset (`test_basic_cases.py test_client.py test_edit_files.py test_multiple_connections.py test_util.py`) in `tests/`. `test_basic_cases.py` and `test_client.py` are NOT discovered by plain `pytest tests/`.
- Python unit tests: `zyn-test py-unit-tests` → `pytest` in `py/zyn/` (only `test_connection.py` currently).
- JS unit tests: `zyn-test js-unit-tests` → `mocha` in `js/` (tests in `js/test/test_*.mjs`). No mocharc; mocha defaults are used.
- Lint: `zyn-static-analysis python` (flake8, config `setup.cfg`, max-line 100, runs against `py/` and `tests/`) and `zyn-static-analysis bash` (shellcheck on `git ls-files '*.sh'`). JS: `npm run static-analysis` in `js/` (uses `standard`).
- Run dev stack: `zyn-run dev-env` (starts server + webserver + JS/CSS watchers). Individual: `zyn-run server`, `zyn-run web-client`, `zyn-run web-client-js`, `zyn-run web-client-css`, `zyn-run shell`.
- Docker images: `zyn-build docker-server <tag>` / `zyn-build docker-web-client <tag>` (build from `docker/dockerfile-zyn` / `dockerfile-web-client`, context = repo root).

## System-test prerequisites (often missed)

System tests in `tests/` will fail without these:

1. The Rust server must be built first. `tests/common.py` reads `ZYN_TESTS_PATH_SERVER_BINARY` and falls back to `zyn/target/debug/zyn` — but the VM sets it to `$CARGO_TARGET_DIR/debug/zyn` (`/tmp/cargo-build/debug/zyn`). If you ran `cargo build` outside the VM with no `CARGO_TARGET_DIR`, the binary is at `zyn/target/debug/zyn`.
2. A GPG fingerprint file must exist at `~/.zyn-test-user-gpg-fingerprint` (created by `vm/development/files/zyn-prepare-encryption-keys.sh`). Server is launched with `--gpg-fingerprint $(cat ...)`.
3. Server is started by the test harness (`manage_server=true` by default) on `127.0.0.1:4433` per test; do not run a server alongside.

Useful pytest flags surfaced in the runner: `--log-level=debug -s --log-cli-level=debug`, `-k <filter>`, `--collect-only`.

Large `tests/data-nonce-*.bin` files (up to 100 MiB) are committed test fixtures — do not delete or regenerate.

## Conventions / gotchas

- Python tests (`tests/test_*.py`, `py/zyn/test_connection.py`) use `unittest.TestCase` (not pytest-native). Keep that style.
- Rust dependencies are pinned with `=x.y.*` in `zyn/Cargo.toml`; do not bump casually. `Cargo.lock` is **gitignored** (see `.gitignore`).
- `docker/dockerfile-zyn` patches the vendored `embedded-websocket-0.3.2` source with `sed` to raise `EMPTY_HEADER; 16` → `20` (iOS sends >16 headers). Preserve this if you change the Dockerfile or bump that crate.
- Web client assets pipeline: edit `js/src/*.mjs` and `js/css/zyn.css`; esbuild/tailwind write into `py/zyn/client/zyn-web-static/`, which the Python `zyn-webserver` then serves. The watch scripts (`web-client-js`, `web-client-css`) are the normal dev loop.
- `js/` `node_modules` is bind-mounted from `/node-modules` inside the VM (see `vm/development/vm.yml`); `npm install` in the repo modifies the mounted dir.
- `readme.md` "Architehture" section is empty; do not trust it for architecture. The real wiring is `zyn/src/main.rs` → `node::node::Node` → modules in `zyn/src/node/`.
