to run server

```bash
mkcert example.com "*.example.com" example.test localhost 127.0.0.1 ::1   
cargo run -p server  -- --tls-cert example.com+5.pem  --tls-key example.com+5-key.pem
```

to run native client
```bash
cargo run -p client
```

to run wasm client
```bash
cd .\client\ 
trunk serve
```
Note: we might have to turn off certain chrome flags to test on a local computer with WASM, see: https://moq.dev/blog/tls-and-quic/ 

to build for linux:
- Run Docker
- then do ``cross build --target x86_64-unknown-linux-gnu --release``
- copy through ssh: ``scp -P 22 server root@174.138.45.57:~/server ``