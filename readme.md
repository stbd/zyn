# Zyn

**Zyn** is a network file server that provides **fopen/fclose-like access over a network**. It enables remote file storage with support for multiple concurrent clients, real-time synchronization, and transparent file encryption.

## File Types

Zyn supports two optimized file types for different use cases:

### Random-Access Files

Ideal for **text files, documents, and structured data** where you need arbitrary read/write at any position. Only the delta of the mofication is sent.

### Blob Files

Ideal for large **binary data, videos, archives, and streaming content** where data is sequential and random-access editing is not ideal.

## Encryption

All files are **encrypted at rest using GPG public-key cryptography**. This ensures your data remains secure even if the storage is compromised.

## Architecture

Zyn is built as a **distributed system with multiple client types connecting to a central Rust server**:

```
┌──────────────────────────────────────────────────────────────────────┐
│  Clients                                                             │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Web Browser                                                         │
│  ┌──────────────────────────┐         ┌─────────────────────────┐    │
│  │ JavaScript + HTML/CSS    │ (HTTP)  │  Python HTTP Server     │    │
│  │ File browser, editor     ├────────→│  (Tornado)              │    │
│  │ Real-time sync           │         │  Static assets, login   │    │
│  └──────────────────────────┘         └─────────────────────────┘    │
│           │                                                  │       │
│           │ (After login, WebSocket to backend is used)      │       │
│           │                                                  │       │
│           ├─────────────────────────┐                        │       │
│                                     │                        │       │
│  CLI Clients (Python)               │                        │       │
│  ┌──────────────────────────┐       │                        │       │
│  │ zyn-shell (interactive)  │       │                        │       │
│  │ zyn-cli (non-interactive)│       │                        │       │
│  └──────────────────────────┘       │                        │       │
│           │                         │                        │       │
│           │ (TCP native protocol)   │                        │       │
│           └──────────┬──────────────┘        │───────────────┘       │
│                      │                       │                       │
└──────────────────────┼───────────────────────│───────────────────────┘
                       │                       │
                ┌──────▼───────────────────────▼──┐
                │  Zyn Core Server (Rust)         │
                │  - Multi-threaded operations    │
                │  - GPG encryption/decryption    │
                │  - User authentication          │
                │  - In-memory filesystem         │
                │  - Persistent encrypted storage │
                └─────────────────────────────────┘
```

### Core Components

**Rust Server** (`zyn/src/` - ~9,300 lines)
- **Build System:** Cargo
- **Key Dependencies:** `embedded-websocket`, `serde`, `sha2`, `chrono`, `log`
- **Concurrency:** Multi-threaded with one thread per open file
- **I/O:** Non-blocking TCP with event loop
- **Encryption:** GPG subprocess integration

**Python Clients & Web Server** (`py/` - ~2,300 lines)
- **Framework:** Tornado 6.x
- **Web Server:** HTTP + WebSocket
- **Protocol:** Binary text-based custom protocol
- **CLI Clients:** `zyn-shell` (interactive), `zyn-cli` (non-interactive)

**JavaScript Web Client** (`js/` - ~2,800 lines)
- **Build Tools:** esbuild (bundling), Tailwind CSS (styling)
- **Testing:** Mocha
- **Key Libraries:** pdfjs-dist (PDF), showdown (Markdown), SortableJS (UI)
- **Protocol:** WebSocket via Python gateway

## Production Deployment

Zyn is deployed on **Docker Swarm** with three containers:

```yaml
Services:
  zyn:              # Rust server (core)
  web-client:       # Python web server
  traefik:          # Reverse proxy (HTTP/TCP routing)
```

### Deployment Steps

1. **Create custom docker-compose file**
   ```bash
   cp docker/docker-compose-prod-base.yml docker/docker-compose-prod-custom.yml
   # Edit to customize for your environment
   ```

2. **Generate GPG key with subkey**
   ```bash
   gpg --generate-key
   gpg --list-keys --fingerprint --with-keygrip
   ```

3. **Create Docker secrets**
   ```bash
   docker secret create zyn_gpg_fingerprint <fingerprint>
   docker secret create zyn_gpg_keygrip <keygrip>
   docker secret create zyn_gpg_password <password>
   gpg --export-secret-key <key-email> | docker secret create zyn_gpg_secret_key -
   ```

4. **Deploy stack**
   ```bash
   docker stack deploy \
     --compose-file docker/docker-compose-prod-base.yml \
     --compose-file docker/docker-compose-prod-custom.yml \
     zyn
   ```

### Configuration

**Environment Variables:**
- `ZYN_DEFAULT_USERNAME` - Initial admin user
- `ZYN_DEFAULT_USER_PASSWORD` - Initial admin password
- `ZYN_MAX_NUMBER_OF_FILESYSTEM_ELEMENTS` - Capacity (default 5,000)
- `ZYN_MAX_SIZE_BLOB_FILE` - Blob file limit
- `ZYN_MAX_SIZE_RANDOM_ACCESS` - Random-access limit
- `ZYN_MAX_INACTIVITY_SECONDS` - Client timeout
- `ZYN_TOKEN_DURATION_SECONDS` - Auth token lifetime

**Storage:**
- Volume: `/data` - Persistent encrypted storage
- Format: GPG-encrypted JSON files + file blocks

## Development

Currently the easiest approach is to use a VM defined under vm/development/ managed by Ansible playbook

### Quick Start

```bash
cd vm/development
vagrant up              # Create development VM
vagrant ssh             # Connect to VM

# Development environment with full build toolchain:
# - Rust + Cargo
# - Python 3 + pip
# - Node.js + npm
# - GPG
```

### Development Commands

**Build:**
```bash
zyn-build server                      # Build Rust server
zyn-build docker-server <tag>         # Build server Docker image
zyn-build docker-web-client <tag>     # Build web client image
```

**Test:**
```bash
zyn-test server-unit-tests            # Rust unit tests
zyn-test py-unit-tests                # Python unit tests
zyn-test js-unit-tests                # JavaScript unit tests
zyn-test server-system-tests          # E2E system tests
```

**Run:**
```bash
zyn-run dev-env                       # Start full stack (server + web)
zyn-run server                        # Start Rust server only
zyn-run web-client                    # Start Python web server
zyn-run web-client-js                 # Watch JS changes
zyn-run web-client-css                # Watch CSS changes
zyn-run shell                         # Interactive CLI client
```

**Lint:**
```bash
zyn-static-analysis python            # Flake8 (Python)
zyn-static-analysis bash              # ShellCheck (shell scripts)
npm run static-analysis               # StandardJS (JavaScript)
```

### Testing

**System Tests:**
```bash
# Run all system tests
zyn-test server-system-tests

# Run specific test
zyn-test server-system-tests -k test_basic_cases

# Verbose output
zyn-test server-system-tests -s --log-level=debug
```

## Contributing

- **Linting:** All code must pass linters before commit (100 char line limit for Python)
- **Tests:** System tests in `tests/` must pass
- **Code Style:** Follow existing patterns in each language

## License

See LICENSE file for details.
