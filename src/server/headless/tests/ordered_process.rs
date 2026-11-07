use super::*;
use crate::api::schema::{Method, Request, WorkspaceTarget};
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHILD_FLAG: &str = "HERDR_ORDERED_STOP_TEST_CHILD";
const TEST_NAME: &str =
    "server::headless::tests::ordered_process::actual_process_stops_with_unknown_blocked_identity";
const SENTINEL: &[u8] = b"private-owned-session-before-test\n";

#[test]
fn actual_process_stops_with_unknown_blocked_identity() {
    if std::env::var_os(CHILD_FLAG).is_some() {
        child_case();
        return;
    }
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "herdr-ordered-stop-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_FLAG, "1")
        .env("XDG_CONFIG_HOME", &root)
        .env("TMPDIR", &root)
        .env_remove(crate::session::SESSION_ENV_VAR)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let exited = loop {
        if child.try_wait().unwrap().is_some() {
            break true;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            break false;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    let output = child.wait_with_output().unwrap();
    let _ = std::fs::remove_dir_all(&root);
    assert!(exited, "private child exceeded stop deadline");
    assert!(
        output.status.success(),
        "private child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("ORDERED_STOP_UNKNOWN_OK"));
}

fn child_case() {
    let path = crate::session::data_dir().join("session.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, SENTINEL).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();
    let mut server = super::test_headless_server();
    server.app.state.default_shell = "/bin/sh".into();
    let private = std::env::var("TMPDIR").unwrap();
    let (create_tx, create_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: Request {
            id: "private-stop-create".into(),
            method: Method::WorkspaceCreate(api::schema::WorkspaceCreateParams {
                source_workspace_id: None,
                cwd: Some(private.clone()),
                focus: false,
                label: Some("private-stop-test".into()),
                env: std::collections::HashMap::from([
                    ("HOME".into(), private),
                    ("ENV".into(), "/dev/null".into()),
                    ("BASH_ENV".into(), "/dev/null".into()),
                ]),
            }),
        },
        respond_to: create_tx,
        response_write_complete: None,
    });
    let created: api::schema::SuccessResponse =
        serde_json::from_str(&create_rx.recv().unwrap()).unwrap();
    let api::schema::ResponseResult::WorkspaceCreated {
        workspace,
        root_pane,
        ..
    } = created.result
    else {
        panic!("private workspace creation failed");
    };
    let workspace_id = workspace.workspace_id;
    let (_, pane_id) = server.app.parse_pane_id(&root_pane.pane_id).unwrap();
    server.app.policy.persist_session = true;
    let writer = server.app.session_writer_for_test();
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _guard = writer.lock().unwrap();
        locked_tx.send(()).unwrap();
        loop {
            std::thread::park();
        }
    });
    locked_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (respond_to, _response) = std::sync::mpsc::channel();
    server.begin_identity_persistence(api::ApiRequestMessage {
        request: Request {
            id: "private-stop-test".into(),
            method: Method::WorkspaceIdentityEnsureDurable(WorkspaceTarget { workspace_id }),
        },
        respond_to,
        response_write_complete: None,
    });
    assert!(server.persistence_pending());
    loop {
        match server
            .app
            .event_tx
            .try_send(AppEvent::TerminalBell { pane_id, count: 1 })
        {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                panic!("private event channel closed early")
            }
        }
    }
    let events = server.app.event_tx.clone();
    let (producer_started_tx, producer_started_rx) = std::sync::mpsc::sync_channel(1);
    let (producer_done_tx, producer_done_rx) = std::sync::mpsc::sync_channel(1);
    let producer = std::thread::spawn(move || {
        producer_started_tx.send(()).unwrap();
        let refused = events
            .blocking_send(AppEvent::TerminalBell { pane_id, count: 1 })
            .is_err();
        producer_done_tx.send(refused).unwrap();
    });
    producer_started_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    assert!(
        producer_done_rx.try_recv().is_err(),
        "full event channel must backpressure producer"
    );
    server.should_quit.store(true, Ordering::Release);
    let result = runtime.block_on(server.run());
    assert!(
        result.is_err(),
        "unresolved save must not be reported as clean shutdown"
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("persistence unresolved"));
    assert!(
        producer_done_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        "shutdown must close the channel and release blocked producers"
    );
    producer.join().unwrap();
    drop(server);
    assert_eq!(std::fs::read(path).unwrap(), SENTINEL);
    println!("ORDERED_STOP_UNKNOWN_OK");
}
