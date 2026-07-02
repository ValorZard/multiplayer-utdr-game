to run server
```bash
cd server
mkcert example.com "*.example.com" example.test localhost 127.0.0.1 ::1   
cargo run -- --tls-cert example.com+5.pem  --tls-key example.com+5-key.pem
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

to build the server for linux:
- Run Docker
- then do ``cross build -p server --target x86_64-unknown-linux-gnu --release``
- kill previous running server ``ssh root@174.138.45.57 "pkill server"``
- copy through ssh: ``scp -P 22 target/x86_64-unknown-linux-gnu/release/server root@174.138.45.57:~/server``
- make server binary executable again ``ssh root@174.138.45.57 "chmod +x ~/server"``
- restart machine ``ssh root@174.138.45.57 "reboot"``
- (Cron is the way I run the server on the cloud linux machine:
  https://help.ubuntu.com/community/CronHowto)