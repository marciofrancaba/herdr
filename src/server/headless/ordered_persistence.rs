//! Single-flight headless persistence. The App owns a suspended pane continuation;
//! the loop owns its completion and the notification context. No lifecycle replay
//! is permitted after an uncertain save. This is private experimental integration.
use super::*;
use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, TryRecvError};

const CONTROL_BACKLOG: usize = 64;
const COMPLETION_POLL: Duration = Duration::from_millis(100);

type SaveResult = Receiver<io::Result<()>>;

pub(super) struct PaneCompletion {
    pub pane_id: crate::layout::PaneId,
    pub terminal_id: Option<String>,
    pub focus_before: Vec<(u64, Option<super::client_views::ShellFocusTarget>)>,
    pub focused_tabs_before: HashSet<String>,
}

enum Operation {
    Shutdown {
        result: Option<SaveResult>,
    },
    Identity {
        request: api::ApiRequestMessage,
        workspace_id: String,
        instance_id: String,
        result: SaveResult,
    },
    Checkpoint {
        context: PaneCompletion,
        // None while an older writer is still active, or after an unknown result.
        result: Option<SaveResult>,
        failure: Option<String>,
    },
}

#[derive(Default)]
pub(super) struct OrderedPersistence {
    operation: Option<Operation>,
    controls: VecDeque<ServerEvent>,
    stop_deadline: Option<Instant>,
    last_render: Option<Instant>,
    deferred_titles: HashSet<crate::layout::PaneId>,
    pub(super) shutdown_result: Option<Result<(), String>>,
}

fn error_response(id: String, code: &str, message: String) -> String {
    serde_json::to_string(&api::schema::ErrorResponse {
        id,
        error: api::schema::ErrorBody {
            code: code.into(),
            message,
        },
    })
    .expect("serializable error")
}

impl HeadlessServer {
    pub(super) fn persistence_pending(&self) -> bool {
        self.ordered_persistence.operation.is_some()
    }

    pub(super) fn begin_ordered_shutdown(&mut self) {
        assert!(!self.persistence_pending());
        self.ordered_persistence.operation = Some(Operation::Shutdown { result: None });
        self.ordered_persistence.stop_deadline = Some(Instant::now() + Duration::from_secs(5));
    }

    pub(super) fn defer_pane_completion(&mut self, context: PaneCompletion) {
        assert!(
            !self.persistence_pending(),
            "only one checkpoint continuation"
        );
        assert!(self.app.pending_pane_checkpoint.is_some());
        self.ordered_persistence.operation = Some(Operation::Checkpoint {
            context,
            result: None,
            failure: None,
        });
        self.poll_ordered_persistence();
    }

    // This bypasses App's generic API entry, which synchronizes terminal titles.
    // Only the explicitly read-only snapshot is served during persistence.
    pub(super) fn answer_during_persistence(&mut self, msg: api::ApiRequestMessage) {
        let response = match msg.request.method {
            api::schema::Method::SessionSnapshot(_) => {
                serde_json::to_string(&api::schema::SuccessResponse {
                    id: msg.request.id,
                    result: api::schema::ResponseResult::SessionSnapshot {
                        snapshot: Box::new(self.app.session_snapshot()),
                    },
                })
                .expect("serializable snapshot")
            }
            api::schema::Method::WorkspaceList(_) => {
                serde_json::to_string(&api::schema::SuccessResponse {
                    id: msg.request.id,
                    result: api::schema::ResponseResult::WorkspaceList {
                        workspaces: self.app.session_metadata_snapshot().workspaces,
                    },
                })
                .expect("serializable workspace list")
            }
            api::schema::Method::WorkspaceGet(target) => {
                match self
                    .app
                    .session_metadata_snapshot()
                    .workspaces
                    .into_iter()
                    .find(|workspace| workspace.workspace_id == target.workspace_id)
                {
                    Some(workspace) => serde_json::to_string(&api::schema::SuccessResponse {
                        id: msg.request.id,
                        result: api::schema::ResponseResult::WorkspaceInfo { workspace },
                    })
                    .expect("serializable workspace"),
                    None => error_response(
                        msg.request.id,
                        "workspace_not_found",
                        "Workspace not found".into(),
                    ),
                }
            }
            api::schema::Method::ServerStop(_) => {
                self.should_quit.store(true, Ordering::Release);
                error_response(
                    msg.request.id,
                    "shutdown_pending",
                    "Stop requested; waiting for the in-flight persistence result".into(),
                )
            }
            _ => {
                let unknown = matches!(
                    &self.ordered_persistence.operation,
                    Some(Operation::Checkpoint {
                        failure: Some(_),
                        ..
                    })
                );
                error_response(
                    msg.request.id,
                    if unknown {
                        "persistence_uncommitted"
                    } else {
                        "persistence_busy"
                    },
                    if unknown {
                        "Checkpoint outcome is unknown; lifecycle is suspended and mutation was not applied. Stop exits without replay; inspect persisted session before recovery.".into()
                    } else {
                        "Session persistence is pending; mutation was not applied".into()
                    },
                )
            }
        };
        let _ = msg.respond_to.send(response);
    }

    pub(super) fn begin_identity_persistence(&mut self, msg: api::ApiRequestMessage) {
        let api::schema::Method::WorkspaceIdentityEnsureDurable(ref target) = msg.request.method
        else {
            unreachable!();
        };
        let (index, instance_id) =
            match self.app.prepare_workspace_identity(&msg.request.id, target) {
                Ok(value) => value,
                Err(response) => {
                    let _ = msg.respond_to.send(response);
                    return;
                }
            };
        let workspace_id = self.app.public_workspace_id(index);
        match self.app.start_deferred_identity_save() {
            Ok(result) => {
                self.ordered_persistence.operation = Some(Operation::Identity {
                    request: msg,
                    workspace_id,
                    instance_id,
                    result,
                });
            }
            Err(err) => {
                let response = error_response(
                    msg.request.id,
                    "workspace_identity_uncommitted",
                    format!("Identity was not acknowledged durable: {err}"),
                );
                let _ = msg.respond_to.send(response);
            }
        }
    }

    pub(super) fn poll_ordered_persistence(&mut self) -> bool {
        let Some(mut operation) = self.ordered_persistence.operation.take() else {
            return false;
        };
        if let Operation::Shutdown { result } = &mut operation {
            if result.is_none() {
                match self.app.start_deferred_shutdown_save() {
                    Ok(Some(receiver)) => *result = Some(receiver),
                    Ok(None) => {
                        self.ordered_persistence.shutdown_result = Some(Ok(()));
                        return true;
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        self.ordered_persistence.operation = Some(operation);
                        return false;
                    }
                    Err(err) => {
                        self.ordered_persistence.shutdown_result = Some(Err(err.to_string()));
                        return true;
                    }
                }
            }
        }
        if let Operation::Checkpoint {
            result, failure, ..
        } = &mut operation
        {
            if result.is_none() && failure.is_none() {
                match self.app.start_deferred_pane_checkpoint() {
                    Ok(receiver) => *result = Some(receiver),
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    Err(err) => *failure = Some(err.to_string()),
                }
            }
        }
        let receiver = match &operation {
            Operation::Identity { result, .. } => Some(result),
            Operation::Checkpoint { result, .. } | Operation::Shutdown { result } => {
                result.as_ref()
            }
        };
        let completion = match receiver.map(Receiver::try_recv) {
            None | Some(Err(TryRecvError::Empty)) => {
                self.ordered_persistence.operation = Some(operation);
                return false;
            }
            Some(Err(TryRecvError::Disconnected)) => {
                Err(io::Error::other("persistence worker disconnected"))
            }
            Some(Ok(result)) => result,
        };
        self.app.finish_deferred_save();
        match operation {
            Operation::Shutdown { .. } => {
                self.ordered_persistence.shutdown_result =
                    Some(completion.map_err(|err| err.to_string()));
            }
            Operation::Identity {
                request,
                workspace_id,
                instance_id,
                ..
            } => {
                // Never retain a workspace vector index across the worker wait.
                let valid = self
                    .app
                    .parse_workspace_id(&workspace_id)
                    .and_then(|index| self.app.state.workspaces.get(index))
                    .is_some_and(|workspace| {
                        workspace.instance_id.as_deref() == Some(&instance_id)
                    })
                    && self
                        .app
                        .state
                        .workspaces
                        .iter()
                        .filter(|workspace| {
                            workspace
                                .instance_id
                                .as_deref()
                                .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
                                == uuid::Uuid::parse_str(&instance_id).ok()
                        })
                        .count()
                        == 1;
                let response = match completion {
                    Ok(()) if valid => serde_json::to_string(&api::schema::SuccessResponse {
                        id: request.request.id,
                        result: api::schema::ResponseResult::WorkspaceIdentity { workspace_id, instance_id },
                    }).expect("serializable identity"),
                    result => error_response(request.request.id, "workspace_identity_uncommitted",
                        format!("Identity durability or live identity is unknown: {result:?}; live_match={valid}")),
                };
                let _ = request.respond_to.send(response);
            }
            Operation::Checkpoint { context, .. } => match completion {
                Ok(()) => {
                    self.app.accept_deferred_pane_checkpoint();
                    let updates = self
                        .app
                        .resume_checkpointed_pane_event()
                        .expect("staged checkpoint continuation");
                    for update in &updates {
                        self.forward_semantic_agent_notification(update);
                        self.forward_pane_state_update_notifications_to_clients(update);
                    }
                    self.finish_pane_completion(context);
                }
                Err(err) => {
                    // Keep the continuation and mutation gate. Never replay or
                    // infer that a partially written checkpoint was uncommitted.
                    warn!(error = %err, "pane checkpoint uncertain; lifecycle remains suspended");
                    self.ordered_persistence.operation = Some(Operation::Checkpoint {
                        context,
                        result: None,
                        failure: Some(err.to_string()),
                    });
                }
            },
        }
        if !self.persistence_pending() {
            self.ordered_persistence.stop_deadline = None;
            for pane_id in std::mem::take(&mut self.ordered_persistence.deferred_titles) {
                self.app.render_dirty.request_terminal_title(pane_id);
            }
            self.app.render_dirty.request_generic();
        }
        true
    }

    // An explicit stop cannot safely cancel a filesystem operation. After a
    // bounded grace interval the caller closes sockets and exits with unknown
    // durability, without joining the saver or writing a competing final save.
    pub(super) fn ordered_stop_expired(&mut self) -> bool {
        if !(self.should_quit.load(Ordering::Acquire)
            || self.app.state.should_quit
            || self.host_shutdown_requested())
        {
            return false;
        }
        let deadline = self
            .ordered_persistence
            .stop_deadline
            .get_or_insert_with(|| Instant::now() + Duration::from_secs(5));
        Instant::now() >= *deadline
    }

    pub(super) fn finish_pane_completion(&mut self, context: PaneCompletion) {
        self.reconcile_client_shell_locations();
        self.finish_shell_location_reconciliation(
            context.focus_before,
            &context.focused_tabs_before,
        );
        self.reapply_controlled_shell_tab_geometry(false);
        if self.app.find_pane(context.pane_id).is_none() {
            if let Some(terminal_id) = context.terminal_id {
                self.shutdown_terminal_stream_clients(
                    &terminal_id,
                    format!("terminal {terminal_id} exited"),
                );
            }
        }
    }

    pub(super) fn replay_ordered_controls(&mut self) {
        // Finite FIFO, replayed only after the persistence operation has ended.
        while !self.persistence_pending() {
            if self.should_quit.load(Ordering::Acquire)
                || self.app.state.should_quit
                || self.host_shutdown_requested()
            {
                // Stop terminates input admission. Queued client mutations are
                // discarded, not replayed after the persistence result arrives.
                self.ordered_persistence.controls.clear();
                return;
            }
            let Some(event) = self.ordered_persistence.controls.pop_front() else {
                break;
            };
            self.handle_server_event(event);
        }
    }

    /// Dedicated pump: normal scheduling, internal drains and mutation paths
    /// never run while a snapshot is outstanding. Once the finite control queue
    /// fills, the existing client channel provides backpressure, including PTY
    /// traffic behind it. This is not an unlimited responsiveness guarantee.
    pub(super) async fn wait_ordered_persistence(&mut self) {
        enum Waiting {
            Api(api::ApiRequestMessage),
            Client(ServerEvent),
            Timer,
        }
        let now = Instant::now();
        if self.app.render_dirty.is_pending()
            && self
                .ordered_persistence
                .last_render
                .is_none_or(|last| now.duration_since(last) >= Duration::from_millis(100))
        {
            let render = self.app.render_dirty.take();
            self.ordered_persistence
                .deferred_titles
                .extend(render.terminal_title_sources);
            if render.generic || self.pty_sources_visible_to_any_render_target(&render.pty_sources)
            {
                self.render_and_stream();
            }
            self.ordered_persistence.last_render = Some(now);
        }
        let room = self.ordered_persistence.controls.len() < CONTROL_BACKLOG;
        let event = tokio::select! {
            Some(msg) = self.app.api_rx.recv() => Waiting::Api(msg),
            Some(event) = self.server_event_rx.recv(), if room => Waiting::Client(event),
            _ = tokio::time::sleep(COMPLETION_POLL) => Waiting::Timer,
        };
        match event {
            Waiting::Api(msg) => self.answer_during_persistence(msg),
            Waiting::Client(event) => self.handle_ordered_client_event(event),
            Waiting::Timer => {}
        }
    }

    fn handle_ordered_client_event(&mut self, event: ServerEvent) {
        if (self.should_quit.load(Ordering::Acquire)
            || self.app.state.should_quit
            || self.host_shutdown_requested())
            && !matches!(&event, ServerEvent::ClientShellEndpointRequest { .. })
        {
            // Requests below receive an explicit refusal; no client input or
            // mutation is accepted during the bounded stop grace period.
            return;
        }
        // Once a control is queued, preserve FIFO for subsequent input too.
        // Otherwise a close/focus/resize could be overtaken by typing.
        if !self.ordered_persistence.controls.is_empty()
            && !matches!(
                &event,
                ServerEvent::QuitSignal | ServerEvent::ClientShellEndpointRequest { .. }
            )
        {
            assert!(self.ordered_persistence.controls.len() < CONTROL_BACKLOG);
            self.ordered_persistence.controls.push_back(event);
            return;
        }
        match event {
            ServerEvent::ClientInput { .. }
            | ServerEvent::ClientShellPaneInput { .. }
            | ServerEvent::ClientShellPopupInput { .. } => {
                self.handle_server_event(event);
            }
            ServerEvent::ClientShellEndpointRequest {
                client_id,
                boot_id,
                request,
            } => {
                let response = crate::server::client_commands::error_message(
                    boot_id,
                    request.id,
                    "persistence_busy",
                    "Session persistence is pending; command was not applied",
                );
                self.send_to_client(client_id, response);
            }
            ServerEvent::QuitSignal => {}
            other => self.ordered_persistence.controls.push_back(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{EmptyParams, Method, Request, WorkspaceTarget};
    use crate::server::headless::tests::test_headless_server;

    fn request(method: Method) -> (api::ApiRequestMessage, Receiver<String>) {
        let (respond_to, receiver) = std::sync::mpsc::channel();
        (
            api::ApiRequestMessage {
                request: Request {
                    id: "ordered".into(),
                    method,
                },
                respond_to,
                response_write_complete: None,
            },
            receiver,
        )
    }

    fn identity_wait() -> (
        HeadlessServer,
        std::sync::mpsc::SyncSender<io::Result<()>>,
        Receiver<String>,
    ) {
        let mut server = test_headless_server();
        let mut workspace = crate::workspace::Workspace::test_new("ordered");
        workspace.instance_id = Some("3d3bb0d5-224e-4968-bfa0-389787ca3036".into());
        server.app.state.workspaces = vec![workspace];
        server.app.state.ensure_test_terminals();
        let workspace_id = server.app.public_workspace_id(0);
        let instance_id = server.app.state.workspaces[0].instance_id.clone().unwrap();
        let (request, response) =
            request(Method::WorkspaceIdentityEnsureDurable(WorkspaceTarget {
                workspace_id: workspace_id.clone(),
            }));
        let (sender, result) = std::sync::mpsc::sync_channel(1);
        server.app.ordered_save_pending = true;
        server.ordered_persistence.operation = Some(Operation::Identity {
            request,
            workspace_id,
            instance_id,
            result,
        });
        (server, sender, response)
    }

    #[test]
    fn ordered_pending_rejects_mutation_but_serves_snapshot_and_workspace_reads() {
        let (mut server, _sender, _response) = identity_wait();
        let before = server.app.session_snapshot();
        let id = server.app.public_workspace_id(0);
        let (msg, response) = request(Method::WorkspaceFocus(WorkspaceTarget {
            workspace_id: id.clone(),
        }));
        server.handle_api_request_with_shutdown_check(msg);
        assert!(response.recv().unwrap().contains("persistence_busy"));
        assert_eq!(server.app.session_snapshot(), before);
        for method in [
            Method::SessionSnapshot(EmptyParams {}),
            Method::WorkspaceList(EmptyParams {}),
            Method::WorkspaceGet(WorkspaceTarget {
                workspace_id: id.clone(),
            }),
        ] {
            let (msg, response) = request(method);
            server.handle_api_request_with_shutdown_check(msg);
            assert!(response.recv().unwrap().contains("\"result\""));
        }
        assert_eq!(server.app.session_snapshot(), before);
        assert!(server.persistence_pending());
    }

    #[test]
    fn ordered_identity_rechecks_public_id_and_uuid_after_completion() {
        let (mut server, sender, response) = identity_wait();
        server.app.state.workspaces[0].instance_id = Some(uuid::Uuid::new_v4().to_string());
        sender.send(Ok(())).unwrap();
        server.poll_ordered_persistence();
        assert!(response
            .recv()
            .unwrap()
            .contains("workspace_identity_uncommitted"));
        assert!(!server.persistence_pending());
        assert!(!server.app.ordered_save_pending);
    }

    #[test]
    fn ordered_identity_ack_waits_for_consumption_and_never_repeats() {
        let (mut server, sender, response) = identity_wait();
        assert!(!server.poll_ordered_persistence());
        sender.send(Ok(())).unwrap();
        assert!(server.persistence_pending());
        assert!(response.try_recv().is_err());
        assert!(server.poll_ordered_persistence());
        assert!(response.recv().unwrap().contains("workspace_identity"));
        assert!(!server.poll_ordered_persistence());
        assert!(response.try_recv().is_err());
    }

    #[test]
    fn ordered_disconnected_worker_is_not_a_durable_ack() {
        let (mut server, sender, response) = identity_wait();
        drop(sender);
        server.poll_ordered_persistence();
        assert!(response
            .recv()
            .unwrap()
            .contains("workspace_identity_uncommitted"));
    }

    #[test]
    fn ordered_pending_stops_both_event_drains_before_consumption() {
        let (mut server, _sender, _response) = identity_wait();
        server
            .server_event_tx
            .try_send(ServerEvent::ClientDisconnected { client_id: 12 })
            .unwrap();
        let count = server.server_event_rx.len();
        assert!(!server.drain_server_events());
        assert_eq!(server.server_event_rx.len(), count);
        assert_eq!(
            server.drain_internal_events_with_forwarding_up_to(64),
            (false, false)
        );
    }

    #[test]
    fn ordered_failed_checkpoint_retains_continuation_without_replay() {
        let (mut server, _, _) = identity_wait();
        let pane_id = server.app.state.workspaces[0].tabs[0].root_pane;
        server.app.pending_pane_checkpoint = Some((
            AppEvent::PaneDied {
                pane_id,
                exit_reason: crate::platform::ChildExitReason::Interrupted,
            },
            Vec::new(),
        ));
        let context = PaneCompletion {
            pane_id,
            terminal_id: None,
            focus_before: Vec::new(),
            focused_tabs_before: HashSet::new(),
        };
        let (sender, result) = std::sync::mpsc::sync_channel(1);
        server.ordered_persistence.operation = Some(Operation::Checkpoint {
            context,
            result: Some(result),
            failure: None,
        });
        sender
            .send(Err(io::Error::other("injected uncertain history write")))
            .unwrap();
        server.poll_ordered_persistence();
        assert!(server.persistence_pending());
        assert!(server.app.pending_pane_checkpoint.is_some());
        assert!(server.app.find_pane(pane_id).is_some());
        assert!(!server.poll_ordered_persistence());
        assert!(server.app.pending_pane_checkpoint.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn ordered_handoff_refuses_older_writer_before_disconnecting_clients() {
        let mut server = test_headless_server();
        server.clients.insert(
            7,
            ClientConnection::new(
                (80, 24),
                Default::default(),
                1,
                protocol::RenderEncoding::SemanticFrame,
                None,
            ),
        );
        let (release, wait) = std::sync::mpsc::sync_channel(1);
        server.app.session_save_thread = Some(std::thread::spawn(move || {
            let _ = wait.recv();
        }));
        let result = server.perform_live_handoff(Default::default());
        release.send(()).unwrap();
        server
            .app
            .session_save_thread
            .take()
            .unwrap()
            .join()
            .unwrap();
        assert!(matches!(result, Err(ref err) if err.kind() == io::ErrorKind::WouldBlock));
        assert!(!server.handoff_in_progress);
        assert!(server.clients.contains_key(&7));
    }

    #[test]
    fn ordered_stop_deadline_does_not_consume_or_replay_unfinished_save() {
        let (mut server, _sender, response) = identity_wait();
        server.should_quit.store(true, Ordering::Release);
        assert!(!server.ordered_stop_expired());
        server.ordered_persistence.stop_deadline = Some(Instant::now() - Duration::from_millis(1));
        assert!(server.ordered_stop_expired());
        assert!(server.persistence_pending());
        assert!(response.try_recv().is_err());
    }

    #[test]
    fn ordered_input_cannot_overtake_queued_detach() {
        let (mut server, _sender, _response) = identity_wait();
        server.handle_ordered_client_event(ServerEvent::ClientDetach { client_id: 7 });
        server.handle_ordered_client_event(ServerEvent::ClientInput {
            client_id: 7,
            data: b"draft".to_vec(),
        });
        assert_eq!(server.ordered_persistence.controls.len(), 2);
        assert!(matches!(
            server.ordered_persistence.controls.pop_front(),
            Some(ServerEvent::ClientDetach { client_id: 7 })
        ));
        assert!(
            matches!(server.ordered_persistence.controls.pop_front(), Some(ServerEvent::ClientInput { client_id: 7, data }) if data == b"draft")
        );
    }

    #[tokio::test]
    async fn ordered_control_backlog_is_finite_and_does_not_apply_while_pending() {
        let (mut server, _sender, _response) = identity_wait();
        for client_id in 0..CONTROL_BACKLOG as u64 {
            server
                .server_event_tx
                .try_send(ServerEvent::ClientDisconnected { client_id })
                .unwrap();
            server.wait_ordered_persistence().await;
        }
        assert_eq!(server.ordered_persistence.controls.len(), CONTROL_BACKLOG);
        server
            .server_event_tx
            .try_send(ServerEvent::ClientDisconnected { client_id: 999 })
            .unwrap();
        server.wait_ordered_persistence().await;
        assert_eq!(server.ordered_persistence.controls.len(), CONTROL_BACKLOG);
        assert_eq!(server.server_event_rx.len(), 1);
    }
}

#[cfg(all(test, unix))]
mod real_worker_tests {
    use super::*;
    use crate::server::headless::tests::test_headless_server;

    #[tokio::test]
    async fn ordered_real_worker_keeps_owned_pty_input_output_and_detach_order() {
        let _config_lock = crate::config::test_config_env_lock().lock().unwrap();
        let private =
            std::env::temp_dir().join(format!("herdr-ordered-pty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&private).unwrap();
        let previous_config = std::env::var_os("XDG_CONFIG_HOME");
        let previous_session = std::env::var_os(crate::session::SESSION_ENV_VAR);
        std::env::set_var("XDG_CONFIG_HOME", &private);
        std::env::remove_var(crate::session::SESSION_ENV_VAR);
        let mut server = test_headless_server();
        server.app.state.default_shell = "/bin/sh".into();
        let (respond_to, created) = std::sync::mpsc::channel();
        server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
            request: api::schema::Request {
                id: "private-create".into(),
                method: api::schema::Method::WorkspaceCreate(api::schema::WorkspaceCreateParams {
                    source_workspace_id: None,
                    cwd: Some(private.to_string_lossy().into()),
                    focus: false,
                    label: Some("private-ordered-pty".into()),
                    env: HashMap::from([
                        ("HOME".into(), private.to_string_lossy().into()),
                        ("ENV".into(), "/dev/null".into()),
                        ("BASH_ENV".into(), "/dev/null".into()),
                    ]),
                }),
            },
            respond_to,
            response_write_complete: None,
        });
        let created = created.recv().unwrap();
        let response: api::schema::SuccessResponse = serde_json::from_str(&created).unwrap();
        let api::schema::ResponseResult::WorkspaceCreated {
            workspace,
            root_pane,
            ..
        } = response.result
        else {
            panic!("workspace was not created");
        };
        let (workspace_index, pane_id) = server.app.parse_pane_id(&root_pane.pane_id).unwrap();
        let terminal_id = server.app.state.workspaces[workspace_index]
            .terminal_id(pane_id)
            .unwrap()
            .clone();
        let (control_tx, _control_rx) = std::sync::mpsc::channel();
        let (render_tx, render_rx) = std::sync::mpsc::sync_channel(1);
        server.clients.insert(
            7,
            ClientConnection::new_with_mode(
                ClientConnectionMode::TerminalAttach {
                    terminal_id: terminal_id.to_string(),
                },
                (80, 24),
                Default::default(),
                1,
                protocol::RenderEncoding::TerminalAnsi,
                Some(crate::server::client_transport::ClientWriter::test_channel(
                    control_tx, render_tx,
                )),
            ),
        );
        server.app.policy.persist_session = true;
        server.app.persist_pane_history = false;
        let writer = server.app.session_writer_for_test();
        let held_writer = writer.lock().unwrap();
        let (respond_to, identity_response) = std::sync::mpsc::channel();
        server.begin_identity_persistence(api::ApiRequestMessage {
            request: api::schema::Request {
                id: "blocked-real-worker".into(),
                method: api::schema::Method::WorkspaceIdentityEnsureDurable(
                    api::schema::WorkspaceTarget {
                        workspace_id: workspace.workspace_id,
                    },
                ),
            },
            respond_to,
            response_write_complete: None,
        });
        // Collect failures, then release/join the real worker and remove the
        // owned PTY before restoring process-wide test environment variables.
        let outcome: Result<(), String> = async {
            if !server.persistence_pending() {
                return Err("real worker was not admitted".into());
            }
            server.handle_ordered_client_event(ServerEvent::ClientInput {
                client_id: 7,
                data: b"printf '\\117\\113\\137\\123\\101\\126\\105\\n'\r".to_vec(),
            });
            let deadline = Instant::now() + Duration::from_secs(3);
            let mut seen = false;
            while Instant::now() < deadline {
                server.wait_ordered_persistence().await;
                if let Ok(bytes) = render_rx.try_recv() {
                    if let Ok(ServerMessage::Terminal(frame)) =
                        protocol::read_message(&mut std::io::Cursor::new(bytes), MAX_FRAME_SIZE)
                    {
                        if String::from_utf8_lossy(&frame.bytes).contains("OK_SAVE") {
                            seen = true;
                            break;
                        }
                    }
                }
            }
            if !seen {
                return Err("real PTY output was not streamed while writer stalled".into());
            }
            if identity_response.try_recv().is_ok() {
                return Err("identity acknowledged before writer release".into());
            }
            server.handle_ordered_client_event(ServerEvent::ClientDetach { client_id: 7 });
            server.handle_ordered_client_event(ServerEvent::ClientInput {
                client_id: 7,
                data: b"printf '\\102\\101\\104\\137\\117\\122\\104\\105\\122\\n'\r".to_vec(),
            });
            if server.ordered_persistence.controls.len() != 2 {
                return Err("detach/input ordering lost".into());
            }
            Ok(())
        }
        .await;
        drop(held_writer);
        let deadline = Instant::now() + Duration::from_secs(3);
        while server.persistence_pending() && Instant::now() < deadline {
            server.poll_ordered_persistence();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let ack = identity_response.try_recv().ok();
        server.replay_ordered_controls();
        let detached = !server.clients.contains_key(&7);
        server.clients.insert(
            8,
            ClientConnection::new_with_mode(
                ClientConnectionMode::TerminalAttach {
                    terminal_id: terminal_id.to_string(),
                },
                (80, 24),
                Default::default(),
                2,
                protocol::RenderEncoding::TerminalAnsi,
                None,
            ),
        );
        server.handle_server_event(ServerEvent::ClientInput {
            client_id: 8,
            data: b"printf '\\106\\105\\116\\103\\105\\n'\r".to_vec(),
        });
        let screen_text = |server: &HeadlessServer| {
            let Some(runtime) = server.app.terminal_runtimes.get(&terminal_id) else {
                return (String::new(), "runtime missing".to_string());
            };
            let seq = runtime.content_seq();
            let Some((_, snapshot, _)) = runtime.screen_text_snapshot_with_seq() else {
                return (String::new(), format!("snapshot unavailable; seq={seq}"));
            };
            let diagnostic = format!(
                "seq={seq}, rows={}, cols={}",
                snapshot.rows.len(),
                snapshot.cols
            );
            // A manually inserted 80x24 client does not resize the 160x50
            // headless PTY. Taking only the final 24 rows discards output near
            // its top. Read the bounded complete private fixture screen.
            if snapshot.rows.len() > 1000 {
                return (
                    String::new(),
                    format!("unexpected oversized screen; {diagnostic}"),
                );
            }
            (
                crate::terminal::snapshot_text(&snapshot.rows, snapshot.rows.len(), false, false)
                    .text,
                diagnostic,
            )
        };
        let fence_deadline = Instant::now() + Duration::from_secs(3);
        let mut witness = String::new();
        let mut screen_diagnostic = String::new();
        while Instant::now() < fence_deadline {
            (witness, screen_diagnostic) = screen_text(&server);
            if witness.contains("FENCE") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let fence_seen = witness.contains("FENCE");
        let bad_order = witness.contains("BAD_ORDER");
        // Prove that this exact command and decoder expose the forbidden
        // marker when delivered through a valid client. Capture the negative
        // witness first; this positive control deliberately changes the screen.
        server.handle_server_event(ServerEvent::ClientInput {
            client_id: 8,
            data: b"printf '\\102\\101\\104\\137\\117\\122\\104\\105\\122\\n'\r".to_vec(),
        });
        let positive_deadline = Instant::now() + Duration::from_secs(3);
        let mut positive_witness = String::new();
        let mut positive_diagnostic = String::new();
        while Instant::now() < positive_deadline {
            (positive_witness, positive_diagnostic) = screen_text(&server);
            if positive_witness.contains("BAD_ORDER") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let positive_seen = positive_witness.contains("BAD_ORDER");
        eprintln!("PTY witness: detached={detached}, fence_seen={fence_seen}, bad_order={bad_order}, positive_seen={positive_seen}, screen_diagnostic={screen_diagnostic}, positive_diagnostic={positive_diagnostic}, screen={witness:?}, positive_screen={positive_witness:?}");
        // A stop requested during persistence must also discard input for a
        // still-valid client, both already queued and newly received. Resetting
        // the stop flag below is fixture-only, to execute an observation fence.
        let stop_writer = server.app.session_writer_for_test();
        let held_stop_writer = stop_writer.lock().unwrap();
        let (stop_response, _stop_ack) = std::sync::mpsc::channel();
        server.begin_identity_persistence(api::ApiRequestMessage {
            request: api::schema::Request {
                id: "stop-blocked-real-worker".into(),
                method: api::schema::Method::WorkspaceIdentityEnsureDurable(
                    api::schema::WorkspaceTarget {
                        workspace_id: server.app.public_workspace_id(workspace_index),
                    },
                ),
            },
            respond_to: stop_response,
            response_write_complete: None,
        });
        let stop_was_pending = server.persistence_pending();
        let stop_input = b"printf '\\123\\124\\117\\120\\137\\102\\101\\104\\n'\r".to_vec();
        server
            .ordered_persistence
            .controls
            .push_back(ServerEvent::ClientInput {
                client_id: 8,
                data: stop_input.clone(),
            });
        server.should_quit.store(true, Ordering::Release);
        server.handle_ordered_client_event(ServerEvent::ClientInput {
            client_id: 8,
            data: stop_input.clone(),
        });
        drop(held_stop_writer);
        let stop_completion_deadline = Instant::now() + Duration::from_secs(3);
        while server.persistence_pending() && Instant::now() < stop_completion_deadline {
            server.poll_ordered_persistence();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        server.replay_ordered_controls();
        let stop_queue_empty = server.ordered_persistence.controls.is_empty();
        server.should_quit.store(false, Ordering::Release);
        server.handle_server_event(ServerEvent::ClientInput {
            client_id: 8,
            data: b"printf '\\123\\124\\117\\120\\137\\106\\105\\116\\103\\105\\n'\r".to_vec(),
        });
        let stop_deadline = Instant::now() + Duration::from_secs(3);
        let mut stop_witness = String::new();
        while Instant::now() < stop_deadline {
            stop_witness = screen_text(&server).0;
            if stop_witness.contains("STOP_FENCE") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let stop_fence = stop_witness.contains("STOP_FENCE");
        let stop_bad = stop_witness.contains("STOP_BAD");
        server.handle_server_event(ServerEvent::ClientInput {
            client_id: 8,
            data: stop_input,
        });
        let positive_stop_deadline = Instant::now() + Duration::from_secs(3);
        let mut stop_positive = false;
        while Instant::now() < positive_stop_deadline {
            stop_positive = screen_text(&server).0.contains("STOP_BAD");
            if stop_positive {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        eprintln!("Stop input witness: was_pending={stop_was_pending}, queue_empty={stop_queue_empty}, fence={stop_fence}, rejected_marker={stop_bad}, positive={stop_positive}, screen={stop_witness:?}");
        for (_, runtime) in server.app.terminal_runtimes.drain() {
            runtime.shutdown();
        }
        if let Some(worker) = server.app.session_save_thread.take() {
            worker.join().unwrap();
        }
        drop(server);
        match previous_config {
            Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
            None => std::env::remove_var("XDG_CONFIG_HOME"),
        }
        match previous_session {
            Some(value) => std::env::set_var(crate::session::SESSION_ENV_VAR, value),
            None => std::env::remove_var(crate::session::SESSION_ENV_VAR),
        }
        std::fs::remove_dir_all(private).unwrap();
        assert!(outcome.is_ok(), "{outcome:?}");
        assert!(
            ack.is_some_and(|value| value.contains("\"result\"")),
            "real save must acknowledge after release"
        );
        assert!(
            detached && fence_seen && !bad_order,
            "input after queued detach must not reach the old PTY: detached={detached}, fence_seen={fence_seen}, bad_order={bad_order}, screen_diagnostic={screen_diagnostic}, screen={witness:?}"
        );
        assert!(
            positive_seen,
            "positive input marker was not detected: {positive_witness:?}"
        );
        assert!(stop_queue_empty && stop_fence && !stop_bad && stop_positive,
            "stop input gate failed: queue_empty={stop_queue_empty}, fence={stop_fence}, bad={stop_bad}, positive={stop_positive}, screen={stop_witness:?}");
    }
}
