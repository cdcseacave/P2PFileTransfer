//! P2P File Transfer Application

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    #[cfg(feature = "cli")]
    {
        use p2p_cli::run_cli;
        run_cli().await?;
    }

    #[cfg(feature = "gui")]
    #[cfg(not(feature = "cli"))]
    {
        use p2p_gui::run_gui;
        run_gui()?;
    }

    #[cfg(not(any(feature = "cli", feature = "gui")))]
    {
        error!("No interface enabled. Build with --features cli or --features gui");
        std::process::exit(1);
    }

    Ok(())
}
