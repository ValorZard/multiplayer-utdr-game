cross build -p server --target x86_64-unknown-linux-gnu --release
# parentheses is subshell, so doesn't affect the main shell
(cd client && trunk build --release)
ssh root@174.138.45.57 "pkill server"
scp target/x86_64-unknown-linux-gnu/release/server root@174.138.45.57:~/server
ssh root@174.138.45.57 "rm -rf ~/dist"
scp -r client/dist root@174.138.45.57:~/dist
ssh root@174.138.45.57 "chmod +x ~/server"
ssh root@174.138.45.57 "reboot"
