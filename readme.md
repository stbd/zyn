# Zyn

**Zyn** is a network file server that provides **fopen/fclose-like access over a network**. It enables remote file storage with support for multiple concurrent clients, real-time synchronization, and transparent file encryption.

## Purpose

Zyn makes editing remote files as simple as editing local files. Whether you're collaborating on text documents, managing configurations, or handling binary data, Zyn provides:

- **Network-transparent file access** - Open, read, write, and delete files over TCP/WebSocket
- **Easy file editing** - Support for two optimized file types suited to different use cases
- **Multi-client support** - Multiple concurrent clients with automatic conflict detection
- **Secure at rest** - All files encrypted with GPG before storage
- **Real-time notifications** - Live updates when files change

## Architecture

Zyn is built as a **distributed system with multiple client types connecting to a central Rust server**:

```
┌──────────────────────────────────────────────────────────────────────┐
│  Clients                                                             │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Web Browser                                                         │
│  ┌──────────────────────────┐                                        │
│  │ JavaScript + HTML/CSS    │ (HTTP)       ┌─────────────────────┐   │
│  │ File browser, editor     ├─────────────→│  Python HTTP Server │   │
│  │ Real-time sync           │ (upgrade)    │  (Tornado)          │   │
│  └──────────────────────────┘              └────────┬────────────┘   │
│                                           (WebSocket)   │            │
│                                                         │            │
│  CLI Clients (Python)                                   │            │
│  ┌──────────────────────────┐                           │            │
│  │ zyn-shell (interactive)  │   TCP native protocol     │            │
│  │ zyn-cli (non-interactive)├───────────────────────────┼────┐       │
│  └──────────────────────────┘                           │    │       │
│                                                         │    │       │
└─────────────────────────────────────────────────────────┼────┼───────┘
                                                          │    │
                          ┌───────────────────────────────┘    │
                          │                                    │
                    ┌─────▼──────────────────────────────┐     │
                    │  Zyn Core Server (Rust)           │◄─────┘
                    │  - Multi-threaded operations      │
                    │  - GPG encryption/decryption      │
                    │  - User authentication/permissions│
                    │  - In-memory filesystem           │
                    │  - Persistent encrypted storage   │
                    └───────────────────────────────────┘
```

### Client Connection Flow

**Web Browser:**
1. Browser connects to HTTP server (Python/Tornado) on port 8081
2. Serves static HTML/CSS/JavaScript assets
3. JavaScript client initiates HTTP request for WebSocket upgrade
4. Connection upgraded to WebSocket (HTTP 101 Upgrade)
5. WebSocket connection proxies directly to Zyn backend (Rust) on port 8080
6. Browser can then send native Zyn protocol messages

**CLI Clients:**
- `zyn-shell` and `zyn-cli` connect directly to Zyn backend (Rust) using native TCP protocol
- No HTTP/WebSocket layer needed for CLI clients

### Core Components

**Rust Server** (`zyn/src/` - ~9,300 lines)
- **`node.rs`** - Main orchestration engine managing clients, filesystem, and file operations
- **`filesystem.rs`** - In-memory file tree representation with encryption support
- **`file.rs`** - File data operations (read/write/insert/delete) with block management
- **`file_handle.rs`** - Individual file instances with dedicated service threads
- **`socket.rs`** - TCP connection handling with non-blocking I/O
- **`connection.rs`** - Protocol detection (native Zyn TCP vs WebSocket)
- **`crypto.rs`** - GPG-based file encryption/decryption
- **`user_authority.rs`** - Authentication (passwords, groups) and authorization

**Python Clients & Web Server** (`py/` - ~2,300 lines)
- **`connection.py`** - Client protocol implementation for Python
- **`client/web.py`** - Tornado HTTP server with WebSocket gateway to Rust backend
- **`client/shell.py`** - Interactive CLI client (`zyn-shell`)
- **`client/cli.py`** - Non-interactive CLI client (`zyn-cli`)
- **`client/client.py`** - Filesystem sync client

**JavaScript Web Client** (`js/` - ~2,800 lines)
- **`connection.mjs`** - Browser-side protocol implementation over WebSocket
- **`client.mjs`** - High-level UI coordination
- **`file.mjs`** - File editor components (text, markdown, PDF)
- **`messages.mjs`** - Message serialization/parsing

## File Types

Zyn supports two optimized file types for different use cases:

### Random-Access Files

Ideal for **text files, documents, and structured data** where you need arbitrary read/write at any position.

**Features:**
- **Block-based storage** - File divided into configurable blocks (default 5 MB max)
- **Arbitrary operations:**
  - `Write(offset, data)` - Overwrite at position
  - `Insert(offset, data)` - Insert data, shifting content forward
  - `Delete(offset, size)` - Remove bytes, shifting content back
  - `Read(offset, size)` - Read from any position
- **Atomic editing** - Batch operations commit together with revision tracking
- **Conflict detection** - Stale writes detected via revision numbers

### Blob Files

Ideal for **binary data, videos, large archives, and streaming content** where data is sequential.

**Features:**
- **Sequential streaming** - Optimized for append-like patterns
- **Simple operations:**
  - `Write(offset, data)` - Append or overwrite sequentially
  - `Read(offset, size)` - Read sequentially
- **System locking** - Prevents concurrent writes during streaming
- **Large file support** - Default 10 MB max configurable per file

## Encryption

All files are **encrypted at rest using GPG public-key cryptography**. This ensures your data remains secure even if the storage is compromised.

### How It Works

1. **Key Setup (Production)**
   - A GPG key pair with subkey is generated and stored securely
   - Public key fingerprint is provided to the server
   - Private key and passphrase stored in Docker secrets

2. **File Storage**
   - **Metadata files** - Encrypted with GPG (revision, timestamps, block info)
   - **File data blocks** - Each block encrypted separately with GPG
   - On-disk format: Standard GPG encrypted binary

3. **File Access**
   - When files are read: Server decrypts via `gpg2 --decrypt`
   - When files are written: Server encrypts via `gpg2 --encrypt`
   - In-memory: Decrypted only during active operations

4. **Key Management**
   ```bash
   # Production: Keys stored in Docker secrets
   docker secret create zyn_gpg_fingerprint <fingerprint>
   docker secret create zyn_gpg_keygrip <keygrip>
   docker secret create zyn_gpg_secret_key <secret.gpg>
   docker secret create zyn_gpg_password <password>
   ```

## Technologies

### Rust Server
- **Build System:** Cargo
- **Key Dependencies:** `embedded-websocket`, `serde`, `sha2`, `chrono`, `log`
- **Concurrency:** Multi-threaded with one thread per open file
- **I/O:** Non-blocking TCP with event loop
- **Encryption:** GPG subprocess integration

### Python Client/Web Server
- **Framework:** Tornado 6.x
- **Web Server:** HTTP + WebSocket
- **Protocol:** Binary text-based custom protocol
- **CLI Clients:** `zyn-shell` (interactive), `zyn-cli` (non-interactive)

### JavaScript Web Client
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

### Development Stack

**Full Stack Startup:**
```bash
# All services together
zyn-run dev-env

# Or individually in separate terminals:
zyn-run server              # Port 8080
zyn-run web-client          # Port 8081 (proxies to server)
zyn-run web-client-js       # Watch JS
zyn-run web-client-css      # Watch CSS
```

**Access:**
- Web UI: `http://localhost:8081`
- Rust server (native): `127.0.0.1:8080`
- Default credentials: `admin` / `admin`

### Project Structure

```
zyn/
├── zyn/                 # Rust server
│   ├── src/
│   │   ├── main.rs
│   │   ├── node/        # Core modules
│   │   │   ├── mod.rs
│   │   │   ├── node.rs
│   │   │   ├── file.rs
│   │   │   ├── crypto.rs
│   │   │   └── ...
│   │   └── ...
│   └── Cargo.toml
├── py/                  # Python clients
│   ├── zyn/
│   │   ├── connection.py
│   │   ├── client/
│   │   │   ├── shell.py
│   │   │   ├── web.py
│   │   │   └── client.py
│   │   └── ...
│   └── setup.py
├── js/                  # JavaScript web client
│   ├── src/
│   │   ├── connection.mjs
│   │   ├── client.mjs
│   │   └── ...
│   ├── css/
│   │   └── zyn.css
│   └── package.json
├── tests/               # System tests
│   ├── test_basic_cases.py
│   ├── test_client.py
│   └── ...
├── docker/              # Deployment configs
│   ├── dockerfile-zyn
│   ├── dockerfile-web-client
│   └── docker-compose-*.yml
└── vm/development/      # Dev environment
    ├── vm.yml
    └── files/zyn-*.sh
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

**Test Prerequisites:**
- Rust binary built: `zyn/target/debug/zyn`
- GPG fingerprint file: `~/.zyn-test-user-gpg-fingerprint`
- Port available: `127.0.0.1:4433`

### Build Artifacts

**JavaScript Output:**
```bash
npm run zyn-compile-js      # Produces: py/zyn/client/zyn-web-static/zyn.js
npm run zyn-compile-css     # Produces: py/zyn/client/zyn-web-static/zyn.css
```

**Note:** JavaScript/CSS files are bundled into Python package as data files, not edited directly.

## Contributing

- **Linting:** All code must pass linters before commit (100 char line limit for Python)
- **Tests:** System tests in `tests/` must pass
- **Code Style:** Follow existing patterns in each language
- **Git:** `Cargo.lock` is gitignored (dependencies pinned in `Cargo.toml`)

## License

See LICENSE file for details.
