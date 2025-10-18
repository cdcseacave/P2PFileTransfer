//! P2P File Transfer Application

use anyhow::Result;

fn main() -> Result<()> {
    // Parse command-line arguments and run appropriate mode
    #[cfg(feature = "cli")]
    {
        use p2p_cli::run_cli_sync;
        run_cli_sync()?;
    }

    #[cfg(feature = "gui")]
    #[cfg(not(feature = "cli"))]
    {
        use p2p_gui::run_gui;
        run_gui()?;
    }

    #[cfg(not(any(feature = "cli", feature = "gui")))]
    {
        eprintln!("No interface enabled. Build with --features cli or --features gui");
        std::process::exit(1);
    }

    Ok(())
}
