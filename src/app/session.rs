use std::time::{Duration, Instant};

use super::{App, SESSION_SAVE_DEBOUNCE};

enum SessionSaveJob {
    Clear,
    Save {
        snapshot: crate::persist::SessionSnapshot,
        history: Option<crate::persist::SessionHistorySnapshot>,
    },
}

impl App {
    #[cfg(test)]
    pub(crate) fn session_writer_for_test(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<crate::persist::SessionWriter>> {
        self.session_writer.clone()
    }

    pub(crate) fn persist_workspace_identity_now(&mut self) -> std::io::Result<()> {
        if !self.policy.persist_session {
            return Err(std::io::Error::other("session persistence is disabled"));
        }
        if let Some(thread) = self.session_save_thread.take() {
            thread
                .join()
                .map_err(|_| std::io::Error::other("session save thread failed"))?;
        }
        let snapshot = crate::persist::capture(
            &self.state.workspaces,
            &self.state.terminals,
            &self.terminal_runtimes,
            self.state.active,
            self.state.selected,
        );
        save_identity_snapshot(&self.session_writer, snapshot, self.persist_pane_history)
    }

    // The caller serializes persistence and defers critical lifecycle events
    // until the completion is consumed.
    pub(crate) fn start_deferred_identity_save(
        &mut self,
    ) -> std::io::Result<std::sync::mpsc::Receiver<std::io::Result<()>>> {
        if !self.policy.persist_session {
            return Err(std::io::Error::other("session persistence is disabled"));
        }
        self.reserve_deferred_session_writer()?;
        let snapshot = crate::persist::capture(
            &self.state.workspaces,
            &self.state.terminals,
            &self.terminal_runtimes,
            self.state.active,
            self.state.selected,
        );
        let writer = self.session_writer.clone();
        let persist_history = self.persist_pane_history;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("herdr-identity-save".into())
            .spawn(move || {
                let result = save_identity_snapshot(&writer, snapshot, persist_history);
                let _ = tx.send(result);
            })?;
        self.session_save_thread = Some(worker);
        self.ordered_save_pending = true;
        Ok(rx)
    }

    // The caller retains the pane continuation and gates concurrent mutations
    // until the completion is consumed.
    pub(crate) fn start_deferred_pane_checkpoint(
        &mut self,
    ) -> std::io::Result<std::sync::mpsc::Receiver<std::io::Result<()>>> {
        if !self.policy.persist_session {
            return Err(std::io::Error::other("session persistence is disabled"));
        }
        self.reserve_deferred_session_writer()?;
        let job = self.capture_session_save_job();
        let SessionSaveJob::Save { snapshot, history } = job else {
            return Err(std::io::Error::other("cannot checkpoint an empty session"));
        };
        let writer = self.session_writer.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("herdr-pane-checkpoint".into())
            .spawn(move || {
                let result = match writer.lock() {
                    Ok(mut writer) => writer.save_checkpoint_checked(&snapshot, history.as_ref()),
                    Err(_) => Err(std::io::Error::other("session writer is poisoned")),
                };
                let _ = tx.send(result);
            })?;
        self.session_save_thread = Some(worker);
        self.ordered_save_pending = true;
        Ok(rx)
    }

    pub(crate) fn start_deferred_shutdown_save(
        &mut self,
    ) -> std::io::Result<Option<std::sync::mpsc::Receiver<std::io::Result<()>>>> {
        if !self.policy.persist_session
            || (self.pane_exit_checkpoint_pending && !self.state.session_dirty)
        {
            return Ok(None);
        }
        self.reserve_deferred_session_writer()?;
        let job = self.capture_session_save_job();
        let writer = self.session_writer.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("herdr-shutdown-save".into())
            .spawn(move || {
                let result = match writer.lock() {
                    Ok(mut writer) => match job {
                        SessionSaveJob::Clear => writer.clear_checked(),
                        SessionSaveJob::Save { snapshot, history } => {
                            writer.save_checkpoint_checked(&snapshot, history.as_ref())
                        }
                    },
                    Err(_) => Err(std::io::Error::other("session writer is poisoned")),
                };
                let _ = tx.send(result);
            })?;
        self.session_save_thread = Some(worker);
        self.ordered_save_pending = true;
        Ok(Some(rx))
    }

    fn reserve_deferred_session_writer(&mut self) -> std::io::Result<()> {
        if self.ordered_save_pending {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "save completion is not yet consumed",
            ));
        }
        if self
            .session_save_thread
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            if let Some(thread) = self.session_save_thread.take() {
                thread
                    .join()
                    .map_err(|_| std::io::Error::other("session save thread failed"))?;
            }
        }
        if self.session_save_thread.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "session writer is busy",
            ));
        }
        Ok(())
    }

    // Call only after the event loop has consumed and classified the result.
    pub(crate) fn finish_deferred_save(&mut self) {
        self.ordered_save_pending = false;
    }
    pub(super) fn schedule_session_save(&mut self) {
        if self.policy.persist_session {
            self.pane_exit_checkpoint_pending = false;
            self.session_save_deadline = Some(Instant::now() + SESSION_SAVE_DEBOUNCE);
        }
    }

    pub(crate) fn sync_session_save_schedule(&mut self) {
        if self.state.session_dirty {
            self.state.session_dirty = false;
            self.schedule_session_save();
        }
    }

    fn reap_finished_session_save(&mut self) {
        if self
            .session_save_thread
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            if let Some(thread) = self.session_save_thread.take() {
                let _ = thread.join();
            }
        }
    }

    fn capture_session_save_job(&self) -> SessionSaveJob {
        if self.state.workspaces.is_empty() {
            SessionSaveJob::Clear
        } else {
            let snapshot = crate::persist::capture(
                &self.state.workspaces,
                &self.state.terminals,
                &self.terminal_runtimes,
                self.state.active,
                self.state.selected,
            );
            let history = self.persist_pane_history.then(|| {
                crate::persist::capture_history(
                    &snapshot,
                    &self.state.workspaces,
                    &self.terminal_runtimes,
                )
            });
            SessionSaveJob::Save { snapshot, history }
        }
    }

    pub(crate) fn start_background_session_save(&mut self) {
        if !self.policy.persist_session {
            self.session_save_deadline = None;
            return;
        }

        self.reap_finished_session_save();
        if self.session_save_thread.is_some() {
            self.session_save_deadline = Some(Instant::now() + Duration::from_millis(250));
            return;
        }

        let job = self.capture_session_save_job();
        let previous_checkpoint = self.pane_exit_checkpoint_pending;
        self.pane_exit_checkpoint_pending = false;
        self.session_save_deadline = None;
        let writer = self.session_writer.clone();
        match std::thread::Builder::new()
            .name("herdr-session-save".into())
            .spawn(move || run_session_save_job(job, &writer))
        {
            Ok(thread) => self.session_save_thread = Some(thread),
            Err(err) => {
                tracing::warn!(err = %err, "failed to spawn session save thread; retry scheduled");
                self.pane_exit_checkpoint_pending = previous_checkpoint;
                self.session_save_deadline = Some(Instant::now() + Duration::from_millis(250));
            }
        }
    }

    pub(crate) fn save_session_now(&mut self) {
        if let Some(thread) = self.session_save_thread.take() {
            let _ = thread.join();
        }

        if !self.policy.persist_session {
            self.session_save_deadline = None;
            return;
        }

        run_session_save_job(self.capture_session_save_job(), &self.session_writer);
        self.pane_exit_checkpoint_pending = false;
        self.session_save_deadline = None;
    }

    pub(crate) fn checkpoint_session_before_pane_exit(&mut self) {
        if !self.policy.persist_session
            || (self.pane_exit_checkpoint_pending && !self.state.session_dirty)
        {
            return;
        }
        self.save_session_now();
        self.pane_exit_checkpoint_pending = true;
        self.state.session_dirty = false;
    }

    // Called only after the event loop consumes a successful checked save.
    // The original exit then takes the existing no-op checkpoint branch.
    pub(crate) fn accept_deferred_pane_checkpoint(&mut self) {
        self.pane_exit_checkpoint_pending = true;
        self.state.session_dirty = false;
        self.session_save_deadline = None;
    }

    pub(crate) fn finish_checkpointed_pane_exit(&mut self) {
        if self.pane_exit_checkpoint_pending {
            self.state.session_dirty = false;
            self.session_save_deadline = Some(Instant::now() + SESSION_SAVE_DEBOUNCE);
        }
    }

    pub(crate) fn save_session_on_shutdown(&mut self) {
        if self.pane_exit_checkpoint_pending && !self.state.session_dirty {
            self.session_save_deadline = None;
            return;
        }
        self.save_session_now();
    }
}

fn save_identity_snapshot(
    writer: &std::sync::Mutex<crate::persist::SessionWriter>,
    snapshot: crate::persist::SessionSnapshot,
    persist_pane_history: bool,
) -> std::io::Result<()> {
    let mut writer = writer
        .lock()
        .map_err(|_| std::io::Error::other("session writer is poisoned"))?;
    let history_path = crate::session::data_dir().join("session-history.json");
    let history_exists = match std::fs::symlink_metadata(history_path) {
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => return Err(err),
    };
    if persist_pane_history || history_exists {
        let history = crate::persist::load_history()
            .ok_or_else(|| std::io::Error::other("history is not yet persisted"))?;
        let fingerprint = crate::persist::layout_fingerprint(&snapshot)
            .ok_or_else(|| std::io::Error::other("layout fingerprint unavailable"))?;
        if history.layout_fingerprint.as_deref() != Some(fingerprint.as_str()) {
            return Err(std::io::Error::other(
                "history does not match current layout",
            ));
        }
    }
    writer.save_identity_layout_checked(&snapshot)
}

fn run_session_save_job(
    job: SessionSaveJob,
    writer: &std::sync::Mutex<crate::persist::SessionWriter>,
) {
    let mut writer = match writer.lock() {
        Ok(writer) => writer,
        Err(err) => {
            tracing::warn!(err = %err, "session writer is poisoned; refusing to modify session");
            return;
        }
    };
    match job {
        SessionSaveJob::Clear => writer.clear(),
        SessionSaveJob::Save { snapshot, history } => writer.save(&snapshot, history.as_ref()),
    }
}
