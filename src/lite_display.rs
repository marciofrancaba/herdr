//! Cooperative presentation hints, never agent or permission authority.
use crate::detect::Agent;
use std::time::Instant;
pub(crate) const TOKEN: &str = "_o0_lite_profile";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity {
    pub pid: u32,
    pub start: u64,
    pub agent: Agent,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Observation {
    pub identity: Identity,
    pub profile: Option<String>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    pid: u32,
    profile: String,
}
pub(crate) fn parse(environ: &[u8], pid: u32) -> Option<String> {
    let mut entries = environ
        .split(|b| *b == 0)
        .filter_map(|s| s.strip_prefix(b"O0_LITE_PROFILE_DISPLAY="));
    let raw = entries.next()?;
    if entries.next().is_some() || raw.len() > 256 {
        return None;
    }
    let m: Marker = serde_json::from_slice(raw).ok()?;
    let b = m.profile.as_bytes();
    (m.pid == pid
        && pid > 0
        && !b.is_empty()
        && b.len() <= 64
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit()))
    .then_some(())?;
    if !b
        .iter()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_' || *c == b'-')
    {
        return None;
    }
    Some(m.profile)
}
#[derive(Debug, Default)]
pub(crate) struct State {
    latest: Option<Instant>,
    current: Option<Observation>,
}
impl State {
    pub fn observe(&mut self, value: Observation, at: Instant) -> bool {
        if self.latest.is_some_and(|old| at <= old) {
            return false;
        }
        self.latest = Some(at);
        let changed = self.current.as_ref() != Some(&value);
        self.current = Some(value);
        changed
    }
    pub fn clear(&mut self, expected: Identity, at: Instant) -> bool {
        if self.latest.is_some_and(|old| at <= old)
            || self.current.as_ref().is_none_or(|v| v.identity != expected)
        {
            return false;
        }
        self.latest = Some(at);
        self.current = None;
        true
    }
    pub fn profile(&self, engine: Option<&str>) -> Option<&str> {
        let v = self.current.as_ref()?;
        (engine == Some(crate::detect::agent_label(v.identity.agent)))
            .then_some(v.profile.as_deref())
            .flatten()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn item(pid: u32, start: u64, p: Option<&str>) -> Observation {
        Observation {
            identity: Identity {
                pid,
                start,
                agent: Agent::Codex,
            },
            profile: p.map(str::to_owned),
        }
    }
    #[test]
    fn strict_marker() {
        assert_eq!(
            parse(br#"O0_LITE_PROFILE_DISPLAY={"pid":3,"profile":"lean"}"#, 3),
            Some("lean".into())
        );
        for s in [
            r#"{"pid":2,"profile":"lean"}"#,
            r#"{"pid":3,"profile":"Lean"}"#,
            r#"{"pid":3,"profile":"lean","extra":1}"#,
            r#"{"pid":3,"pid":3,"profile":"lean"}"#,
            r#"{"pid":3,"profile":""}"#,
        ] {
            assert!(parse(format!("O0_LITE_PROFILE_DISPLAY={s}").as_bytes(), 3).is_none());
        }
        assert!(parse(b"O0_LITE_PROFILE_DISPLAY={}\0O0_LITE_PROFILE_DISPLAY={}", 3).is_none());
        assert!(parse(
            format!("O0_LITE_PROFILE_DISPLAY={}", "x".repeat(257)).as_bytes(),
            3
        )
        .is_none());
    }
    #[test]
    fn same_engine_generation_replacement_and_late_clear() {
        let mut s = State::default();
        let t = Instant::now();
        let a = item(3, 1, Some("lean"));
        let b = item(4, 2, Some("lead"));
        assert!(s.observe(a.clone(), t));
        assert!(s.observe(b.clone(), t + Duration::from_secs(1)));
        assert!(!s.clear(a.identity, t + Duration::from_secs(2)));
        assert_eq!(s.profile(Some("codex")), Some("lead"));
        assert!(!s.observe(a, t));
        assert!(s.clear(b.identity, t + Duration::from_secs(3)));
        assert_eq!(s.profile(Some("codex")), None);
        assert!(!s.observe(b, t + Duration::from_secs(2)));
    }
    #[test]
    fn same_identity_marker_arrival_and_removal_are_not_cached() {
        let mut s = State::default();
        let t = Instant::now();
        s.observe(item(3, 1, None), t);
        assert!(s.observe(item(3, 1, Some("lean")), t + Duration::from_secs(1)));
        assert_eq!(s.profile(Some("codex")), Some("lean"));
        assert!(s.observe(item(3, 1, None), t + Duration::from_secs(2)));
        assert_eq!(s.profile(Some("codex")), None);
    }

    #[test]
    fn pid_reuse_plain_native_and_engine_mismatch() {
        let mut s = State::default();
        let t = Instant::now();
        s.observe(item(3, 1, Some("lean")), t);
        assert_eq!(s.profile(Some("claude")), None);
        s.observe(item(3, 2, None), t + Duration::from_secs(1));
        assert_eq!(s.profile(Some("codex")), None);
        assert_eq!(State::default().profile(Some("codex")), None);
    }
}
