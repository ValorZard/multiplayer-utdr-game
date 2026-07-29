use anyhow::{Context, Result};

/// Fetches an asset's raw bytes given a path relative to the `assets/` folder,
/// working the same way on both the wasm32 (browser) and native builds.
///
/// - On wasm32, this issues an HTTP GET relative to the page URL,
///     so it relies on `Trunk.toml`'s `public_url = "."`
///     and the assets actually being copied into `dist/`
///     (see the `copy-dir` link in `index.html`)
///     this is what lets it keep working once itch.io serves the game from a hashed subpath.
/// - On native, this reads from disk next to the executable,
///     since `upload_client.sh` ships an `assets/` folder alongside the binary.
///     It falls back to `CARGO_MANIFEST_DIR` so `cargo run` works without staging
///     assets next to `target/debug/client.exe`.
#[cfg(target_arch = "wasm32")]
pub async fn fetch_asset_bytes(relative_path: &str) -> Result<Vec<u8>> {
    use gloo_net::http::Request;

    let path = format!("assets/{relative_path}");
    let response = Request::get(&path)
        .send()
        .await
        .with_context(|| format!("failed to request asset '{path}'"))?;

    if !response.ok() {
        anyhow::bail!("asset '{path}' returned HTTP {}", response.status());
    }

    response
        .binary()
        .await
        .with_context(|| format!("failed to read asset '{path}' body"))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn fetch_asset_bytes(relative_path: &str) -> Result<Vec<u8>> {
    if let Some(exe_dir) = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        && let Ok(bytes) = std::fs::read(exe_dir.join("assets").join(relative_path))
    {
        return Ok(bytes);
    }

    // Fall back to the source tree's assets/ folder for `cargo run`, where the
    // exe lives under target/{debug,release} rather than next to a shipped
    // assets/ folder.
    let dev_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("assets")
        .join(relative_path);
    std::fs::read(&dev_path).with_context(|| {
        format!(
            "failed to read asset '{relative_path}' (tried next to the executable and {})",
            dev_path.display()
        )
    })
}
