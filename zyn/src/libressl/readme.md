Generated with
```
bindgen --no-derive-copy --no-derive-debug --whitelist-var "TLS_.*" --whitelist-type "tls.*"  --whitelist-function "tls_.*" /usr/include/tls.h > /zyn/zyn/src/libressl/tls.rs
```
