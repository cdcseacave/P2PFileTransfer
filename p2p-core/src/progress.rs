//! Unified progress tracking for file transfers

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

pub struct ProgressState {
    total_bytes: u64,
    transferred_bytes: u64,
    progress_bar: ProgressBar,
    /// Optional shared bar (parallel mode: overall total across all connections)
    global_bar: Option<ProgressBar>,
    /// True when bar is owned by a MultiProgress — skip draw-target manipulation.
    managed: bool,
}

impl ProgressState {
    pub fn new(total_bytes: u64) -> Self {
        let progress_bar = ProgressBar::new(total_bytes);
        progress_bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes} ({bytes_per_sec}, ETA: {eta})",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏ "),
        );

        if total_bytes == 0 {
            progress_bar.set_draw_target(indicatif::ProgressDrawTarget::hidden());
        } else {
            progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));
        }

        Self {
            total_bytes,
            transferred_bytes: 0,
            progress_bar,
            global_bar: None,
            managed: false,
        }
    }

    /// Wrap pre-created bars that are already attached to a `MultiProgress`.
    ///
    /// `local` is this connection's bar; `global` is the shared overall bar.
    /// The bars must already have their style and steady-tick configured by the caller.
    /// Draw-target changes are skipped so the MultiProgress stays in control.
    pub fn from_bars(local: ProgressBar, global: ProgressBar) -> Self {
        let total = local.length().unwrap_or(0);
        Self {
            total_bytes: total,
            transferred_bytes: 0,
            progress_bar: local,
            global_bar: Some(global),
            managed: true,
        }
    }

    /// Create a child progress bar, add it to `multi`, and return the wrapped state.
    ///
    /// `label` is shown as a fixed prefix (e.g. `"[Conn  1]"`).
    /// `global` is an optional shared overall bar incremented alongside this one.
    pub fn new_child(
        label: &str,
        total_bytes: u64,
        multi: &MultiProgress,
        global: Option<ProgressBar>,
    ) -> Self {
        let bar = multi.add(ProgressBar::new(total_bytes));
        bar.set_style(
            ProgressStyle::with_template(&format!(
                "  {label} {{bar:35.cyan/blue}} {{bytes}}/{{total_bytes}} ({{bytes_per_sec}}) {{msg}}"
            ))
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏ "),
        );
        bar.enable_steady_tick(std::time::Duration::from_millis(100));

        Self {
            total_bytes,
            transferred_bytes: 0,
            progress_bar: bar,
            global_bar: global,
            managed: true,
        }
    }

    pub fn add_bytes(&mut self, bytes: u64) {
        self.transferred_bytes += bytes;
        self.progress_bar.inc(bytes);
        if let Some(ref gb) = self.global_bar {
            gb.inc(bytes);
        }
    }

    pub fn set_total_bytes(&mut self, total_bytes: u64) {
        if self.total_bytes == total_bytes {
            return;
        }

        if self.total_bytes == 0 && total_bytes > 0 && !self.managed {
            self.progress_bar
                .set_draw_target(indicatif::ProgressDrawTarget::stderr());
            self.progress_bar
                .enable_steady_tick(std::time::Duration::from_millis(100));
        }

        self.total_bytes = total_bytes;
        self.progress_bar.set_length(total_bytes);
        self.progress_bar.tick();
    }

    /// Update the message shown alongside the bar (e.g. current filename).
    pub fn set_message(&mut self, msg: String) {
        self.progress_bar.set_message(msg);
    }

    pub fn finish(&self) {
        self.progress_bar.finish_with_message("done");
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn transferred_bytes(&self) -> u64 {
        self.transferred_bytes
    }

    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes > 0 {
            (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
        } else {
            0.0
        }
    }

    pub fn is_complete(&self) -> bool {
        self.transferred_bytes >= self.total_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_state() {
        let mut state = ProgressState::new(1000);

        assert_eq!(state.total_bytes(), 1000);
        assert_eq!(state.transferred_bytes(), 0);
        assert_eq!(state.progress_percent(), 0.0);
        assert!(!state.is_complete());

        state.add_bytes(250);
        assert_eq!(state.transferred_bytes(), 250);
        assert_eq!(state.progress_percent(), 25.0);
        assert!(!state.is_complete());

        state.add_bytes(750);
        assert_eq!(state.transferred_bytes(), 1000);
        assert_eq!(state.progress_percent(), 100.0);
        assert!(state.is_complete());
    }

    #[test]
    fn test_progress_updates() {
        let mut state = ProgressState::new(500);

        state.add_bytes(100);
        assert_eq!(state.transferred_bytes(), 100);
        assert_eq!(state.progress_percent(), 20.0);

        state.add_bytes(200);
        assert_eq!(state.transferred_bytes(), 300);
        assert_eq!(state.progress_percent(), 60.0);

        state.add_bytes(200);
        assert_eq!(state.transferred_bytes(), 500);
        assert_eq!(state.progress_percent(), 100.0);
        assert!(state.is_complete());
    }
}
