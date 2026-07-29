#!/usr/bin/env bash
# Uploads client builds to https://valorzard.itch.io/rps-test with butler.
# Requires `butler login` to have been run once beforehand.
set -euo pipefail

ITCH_PROJECT="valorzard/rps-test"
# Tag every upload with the commit it was built from so itch's version history is traceable.
VERSION="$(git rev-parse --short HEAD)"

# Web (HTML5) build; Trunk.toml already sets public_url = "." so assets resolve
# from the subpath itch serves games under.
(cd client && trunk build --release)
butler push client/dist "$ITCH_PROJECT:html5" --userversion "$VERSION"

staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT
mkdir "$staging/windows" "$staging/linux"

# Native Windows build. Assets are fetched at runtime next to the exe
# (see client/src/assets.rs), so ship the assets/ folder alongside the binary.
cargo build -p client --release
cp target/release/client.exe "$staging/windows/rps-test.exe"
cp -r client/assets "$staging/windows/assets"
butler push "$staging/windows" "$ITCH_PROJECT:windows" --userversion "$VERSION"

# Native Linux build, cross-compiled in Docker the same way as upload_server.sh.
cross build -p client --target x86_64-unknown-linux-gnu --release
cp target/x86_64-unknown-linux-gnu/release/client "$staging/linux/rps-test"
cp -r client/assets "$staging/linux/assets"
butler push "$staging/linux" "$ITCH_PROJECT:linux" --userversion "$VERSION"

butler status "$ITCH_PROJECT"
