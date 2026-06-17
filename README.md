to run server

```bash
mkcert example.com "*.example.com" example.test localhost 127.0.0.1 ::1   
cargo run -p server  -- --tls-cert example.com+5.pem  --tls-key example.com+5-key.pem
```
