//! Rebuild readable files from saved evidence without network requests.
use anyhow::{Context, Result};
use std::path::PathBuf;

fn main() -> Result<()> {
    let root = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .context("Usage: cargo run --example export_run -- RUN_DIRECTORY")?,
    );
    stellar_raven_jev::export::write_run_index(&root)?;
    println!("{}", root.join("INDEX.md").display());
    Ok(())
}
