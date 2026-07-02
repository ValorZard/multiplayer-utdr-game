cross build -p server --target x86_64-unknown-linux-gnu --release
ssh root@174.138.45.57 "pkill server"
scp -P 22 target/x86_64-unknown-linux-gnu/release/server root@174.138.45.57:~/server
ssh root@174.138.45.57 "chmod +x ~/server"
ssh root@174.138.45.57 "reboot"