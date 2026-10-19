//! Session claims and deletions (`docs/plans/2026-10-19-session-lifecycle.md` 1.1-1.4).
//!
//! A claim is how a window instance takes a session: it names an attempt `(callerInstanceId,
//! attemptId)` and an `inputDigest` of what it is about to open with. The same attempt asked twice
//! gets the same answer; a different digest under the same attempt is refused; a terminal attempt
//! stays terminal. Ownership of the resource is a separate thing, the `claimToken`: a second
//! attempt by the same holder shares the token, a later claimant takes the session over
//! (`transferred`), and the token is what release / stop / handoff are checked against.
//!
//! Only the state machine lives here; processes, registry and persistence are the Runtime's.
use std::collections::HashMap;

use alwith_runtime_protocol::{
    ClaimPhase, ClaimRetirement, ClaimStatus, DeletionOutcome, DeletionStage, ReplacementStage, SessionDeletion,
    SessionReplacement,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClaimKey {
    pub caller_instance_id: String,
    pub attempt_id: String,
}

#[derive(Debug)]
struct Claim {
    status: ClaimStatus,
    owner_id: String,
    input_digest: String,
}

/// Why a claim operation was refused. `code` is the wire name (`claim/...`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimError {
    pub code: &'static str,
    pub message: String,
}

impl ClaimError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

/// What `abort` decided; the Runtime carries out the process side.
#[derive(Debug, PartialEq, Eq)]
pub enum AbortOutcome {
    /// The attempt is withdrawn; nothing else held anything (never claimed, reserved, or a shared
    /// attempt whose token lives on under another attempt).
    Aborted(ClaimStatus),
    /// The attempt alone held a live session: the claim is now aborted and the Runtime releases
    /// the session (the process stays, headless).
    ReleaseAndAbort { session_id: String, status: ClaimStatus },
    /// A later claimant took the session over; the abort changes nothing.
    Transferred(ClaimStatus),
    /// Already terminal (failed, retired otherwise, aborted): the terminal state, unchanged.
    Terminal(ClaimStatus),
}

#[derive(Debug)]
struct Replacement {
    status: SessionReplacement,
    owner_id: String,
    input_digest: String,
    /// The instance being replaced: a frame it still emits while being stopped must not publish
    /// the replacement on it.
    previous_instance_id: String,
}

#[derive(Debug, Default)]
pub struct Claims {
    runtime_instance: String,
    next_token: u64,
    by_token: HashMap<String, Claim>,
    by_key: HashMap<ClaimKey, String>,
    deleting: HashMap<String, SessionDeletion>,
    replacements: HashMap<String, Replacement>,
}

impl Claims {
    pub fn new(runtime_instance: String) -> Self {
        Self { runtime_instance, ..Self::default() }
    }

    fn mint_token(&mut self) -> String {
        self.next_token += 1;
        format!("{}.{}", self.runtime_instance, self.next_token)
    }

    pub fn status(&self, key: &ClaimKey) -> Option<ClaimStatus> {
        self.by_key.get(key).map(|token| self.by_token[token].status.clone())
    }

    fn active_token(&self, session_id: &str) -> Option<String> {
        self.by_token
            .iter()
            .find(|(_, claim)| {
                claim.status.session_id == session_id
                    && matches!(claim.status.phase, ClaimPhase::Active { .. } | ClaimPhase::Replacing { .. })
            })
            .map(|(token, _)| token.clone())
    }

    /// Record a claim. `live`: the agent already running the session, which the Runtime attached
    /// to under this owner before calling; `None` leaves the claim `reserved`.
    pub fn claim(
        &mut self, key: ClaimKey, owner_id: &str, session_id: &str, input_digest: &str, live: Option<(String, String)>,
    ) -> Result<ClaimStatus, ClaimError> {
        if let Some(token) = self.by_key.get(&key) {
            let claim = &self.by_token[token];
            if claim.input_digest != input_digest {
                return Err(ClaimError::new(
                    "claim/digest_mismatch",
                    format!("attempt {} was made with a different input", key.attempt_id),
                ));
            }
            return Ok(claim.status.clone());
        }
        if let Some(deletion) = self.deleting.get(session_id) {
            return Err(ClaimError::new(
                "claim/deleting",
                format!("session {session_id} is being deleted ({:?})", deletion.stage),
            ));
        }
        if let Some(token) = self.active_token(session_id) {
            let claim = self.by_token.get_mut(&token).expect("active token is recorded");
            if claim.owner_id == owner_id && matches!(claim.status.phase, ClaimPhase::Active { .. }) {
                // The same holder again: one ownership, shared by both attempts.
                self.by_key.insert(key, token);
                return Ok(claim.status.clone());
            }
            claim.status.phase = ClaimPhase::Retired { how: ClaimRetirement::Transferred };
        }
        let token = self.mint_token();
        let status = ClaimStatus {
            session_id: session_id.to_string(),
            caller_instance_id: key.caller_instance_id.clone(),
            attempt_id: key.attempt_id.clone(),
            claim_token: token.clone(),
            phase: match live {
                Some((agent_id, agent_instance_id)) => ClaimPhase::Active { agent_id, agent_instance_id },
                None => ClaimPhase::Reserved,
            },
        };
        self.by_token.insert(
            token.clone(),
            Claim { status: status.clone(), owner_id: owner_id.to_string(), input_digest: input_digest.to_string() },
        );
        self.by_key.insert(key, token);
        Ok(status)
    }

    /// The session appeared on `agent_id`, owned by `owner_id`: the reserved claim that was waiting
    /// for it becomes active. Returns the claim that changed.
    pub fn activate(
        &mut self, owner_id: &str, session_id: &str, agent_id: &str, agent_instance_id: &str,
    ) -> Option<ClaimStatus> {
        let (token, phase) = self
            .by_token
            .iter()
            .find(|(_, claim)| {
                claim.owner_id == owner_id
                    && claim.status.session_id == session_id
                    && matches!(claim.status.phase, ClaimPhase::Reserved | ClaimPhase::Replacing { .. })
            })
            .map(|(token, claim)| (token.clone(), claim.status.phase.clone()))?;
        let active =
            ClaimPhase::Active { agent_id: agent_id.to_string(), agent_instance_id: agent_instance_id.to_string() };
        match phase {
            ClaimPhase::Reserved => {
                let claim = self.by_token.get_mut(&token).expect("found token");
                claim.status.phase = active;
                Some(claim.status.clone())
            }
            ClaimPhase::Replacing { replacement_id } => {
                if self
                    .replacements
                    .get(&replacement_id)
                    .is_some_and(|replacement| replacement.previous_instance_id == agent_instance_id)
                {
                    // The old process is still draining; only a new instance publishes.
                    return None;
                }
                // The replacement publishes: the old ownership retires, the same attempts carry on
                // under a new token so nothing minted for the old process can act on the new one.
                let successor_token = self.mint_token();
                let old = self.by_token.get_mut(&token).expect("found token");
                old.status.phase = ClaimPhase::Retired { how: ClaimRetirement::Replaced };
                let mut status = old.status.clone();
                status.claim_token = successor_token.clone();
                status.phase = active;
                let successor = Claim {
                    status: status.clone(),
                    owner_id: old.owner_id.clone(),
                    input_digest: old.input_digest.clone(),
                };
                self.by_token.insert(successor_token.clone(), successor);
                for held in self.by_key.values_mut().filter(|held| **held == token) {
                    *held = successor_token.clone();
                }
                if let Some(replacement) = self.replacements.get_mut(&replacement_id) {
                    replacement.status.stage = ReplacementStage::Published { claim: status.clone() };
                }
                Some(status)
            }
            _ => unreachable!("only reserved and replacing claims are found"),
        }
    }

    // ---- Replacement (2.4) ----

    pub fn replacement(&self, replacement_id: &str) -> Option<SessionReplacement> {
        self.replacements.get(replacement_id).map(|replacement| replacement.status.clone())
    }

    /// Record the intent to replace the process under a live claim. Idempotent on
    /// `replacement_id`; the claim is untouched until `replace_commit`.
    pub fn replace_begin(
        &mut self, claim_token: &str, owner_id: &str, replacement_id: &str, input_digest: &str,
    ) -> Result<SessionReplacement, ClaimError> {
        if let Some(existing) = self.replacements.get(replacement_id) {
            if existing.input_digest != input_digest {
                return Err(ClaimError::new(
                    "replace/digest_mismatch",
                    format!("replacement {replacement_id} was begun with a different input"),
                ));
            }
            return Ok(existing.status.clone());
        }
        let claim = self
            .by_token
            .get(claim_token)
            .ok_or_else(|| ClaimError::new("claim/unknown", format!("no claim {claim_token}")))?;
        if claim.owner_id != owner_id {
            return Err(ClaimError::new("claim/not_holder", format!("claim {claim_token} is not held by {owner_id}")));
        }
        let ClaimPhase::Active { agent_instance_id, .. } = &claim.status.phase else {
            return Err(ClaimError::new(
                "claim/not_active",
                format!("claim {claim_token} is {:?}", claim.status.phase),
            ));
        };
        let previous_instance_id = agent_instance_id.clone();
        let status = SessionReplacement {
            replacement_id: replacement_id.to_string(),
            session_id: claim.status.session_id.clone(),
            claim_token: claim_token.to_string(),
            stage: ReplacementStage::Prepared,
        };
        self.replacements.insert(
            replacement_id.to_string(),
            Replacement {
                status: status.clone(),
                owner_id: owner_id.to_string(),
                input_digest: input_digest.to_string(),
                previous_instance_id,
            },
        );
        Ok(status)
    }

    /// The point of no return: the claim enters `replacing`. The Runtime then stops the old process.
    /// Idempotent: a committed replacement answers its current stage.
    pub fn replace_commit(&mut self, replacement_id: &str, owner_id: &str) -> Result<SessionReplacement, ClaimError> {
        let replacement = self
            .replacements
            .get(replacement_id)
            .ok_or_else(|| ClaimError::new("replace/unknown", format!("no replacement {replacement_id}")))?;
        if replacement.owner_id != owner_id {
            return Err(ClaimError::new(
                "claim/not_holder",
                format!("replacement {replacement_id} is not {owner_id}'s"),
            ));
        }
        if replacement.status.stage != ReplacementStage::Prepared {
            return Ok(replacement.status.clone());
        }
        let token = replacement.status.claim_token.clone();
        let claim = self.by_token.get_mut(&token).expect("a prepared replacement names a recorded claim");
        match &claim.status.phase {
            ClaimPhase::Active { .. } => {
                claim.status.phase = ClaimPhase::Replacing { replacement_id: replacement_id.to_string() };
            }
            ClaimPhase::Retired { how: ClaimRetirement::Transferred } => {
                let replacement = self.replacements.get_mut(replacement_id).expect("looked up above");
                replacement.status.stage = ReplacementStage::Transferred;
                return Err(ClaimError::new("replace/transferred", format!("claim {token} was taken over")));
            }
            phase => {
                return Err(ClaimError::new("claim/not_active", format!("claim {token} is {phase:?}")));
            }
        }
        Ok(self.replacements.get(replacement_id).expect("looked up above").status.clone())
    }

    /// The old process is gone (stopped by the Runtime after commit): the holder may start the new one.
    pub fn replace_stopped(&mut self, replacement_id: &str) -> Option<SessionReplacement> {
        let replacement = self.replacements.get_mut(replacement_id)?;
        if replacement.status.stage == ReplacementStage::Prepared {
            replacement.status.stage = ReplacementStage::Stopped;
        }
        Some(replacement.status.clone())
    }

    /// Withdraw a replacement. Before commit: the intent goes, the old claim stays. After commit and
    /// before publish: nothing runs the session; the claim retires as `how` and the session is left
    /// for a new claim. After publish: the successor claim retires as `how`. Already taken over:
    /// `replace/transferred`.
    pub fn replace_abort(
        &mut self, replacement_id: &str, owner_id: &str, how: ClaimRetirement,
    ) -> Result<SessionReplacement, ClaimError> {
        let replacement = self
            .replacements
            .get(replacement_id)
            .ok_or_else(|| ClaimError::new("replace/unknown", format!("no replacement {replacement_id}")))?;
        if replacement.owner_id != owner_id {
            return Err(ClaimError::new(
                "claim/not_holder",
                format!("replacement {replacement_id} is not {owner_id}'s"),
            ));
        }
        let stage = replacement.status.stage.clone();
        let token = replacement.status.claim_token.clone();
        match stage {
            ReplacementStage::Prepared => {}
            ReplacementStage::Stopped => {
                let claim = self.by_token.get_mut(&token).expect("a replacement names a recorded claim");
                if matches!(claim.status.phase, ClaimPhase::Replacing { .. }) {
                    claim.status.phase = ClaimPhase::Retired { how };
                }
            }
            ReplacementStage::Published { claim } => {
                self.retire(&claim.claim_token, owner_id, how)?;
            }
            ReplacementStage::Transferred => {
                return Err(ClaimError::new(
                    "replace/transferred",
                    format!("replacement {replacement_id} was taken over"),
                ));
            }
            ReplacementStage::Aborted => return Ok(replacement.status.clone()),
        }
        let replacement = self.replacements.get_mut(replacement_id).expect("looked up above");
        replacement.status.stage = ReplacementStage::Aborted;
        Ok(replacement.status.clone())
    }

    pub fn abort(&mut self, key: ClaimKey) -> AbortOutcome {
        let Some(token) = self.by_key.get(&key).cloned() else {
            // Earlier than its claim: leave the terminal state for the claim to find.
            let status = ClaimStatus {
                session_id: String::new(),
                caller_instance_id: key.caller_instance_id.clone(),
                attempt_id: key.attempt_id.clone(),
                claim_token: self.mint_token(),
                phase: ClaimPhase::Aborted,
            };
            self.by_token.insert(
                status.claim_token.clone(),
                Claim { status: status.clone(), owner_id: String::new(), input_digest: String::new() },
            );
            self.by_key.insert(key, status.claim_token.clone());
            return AbortOutcome::Aborted(status);
        };
        let shared = self.by_key.values().filter(|held| **held == token).count() > 1;
        let claim = self.by_token.get_mut(&token).expect("held token is recorded");
        match &claim.status.phase {
            ClaimPhase::Reserved => {
                claim.status.phase = ClaimPhase::Aborted;
                AbortOutcome::Aborted(claim.status.clone())
            }
            ClaimPhase::Active { .. } if shared => {
                // Only this attempt's association goes; the ownership lives on under the others.
                let mut status = claim.status.clone();
                status.phase = ClaimPhase::Aborted;
                status.claim_token = self.mint_token();
                self.by_token.insert(
                    status.claim_token.clone(),
                    Claim { status: status.clone(), owner_id: String::new(), input_digest: String::new() },
                );
                self.by_key.insert(key, status.claim_token.clone());
                AbortOutcome::Aborted(status)
            }
            ClaimPhase::Active { .. } => {
                claim.status.phase = ClaimPhase::Aborted;
                AbortOutcome::ReleaseAndAbort {
                    session_id: claim.status.session_id.clone(),
                    status: claim.status.clone(),
                }
            }
            ClaimPhase::Replacing { .. } => {
                // Nothing runs the session between commit and publish; withdrawing is just the record.
                claim.status.phase = ClaimPhase::Aborted;
                AbortOutcome::Aborted(claim.status.clone())
            }
            ClaimPhase::Retired { how: ClaimRetirement::Transferred } => {
                AbortOutcome::Transferred(claim.status.clone())
            }
            ClaimPhase::Retired { .. } | ClaimPhase::Failed { .. } | ClaimPhase::Aborted => {
                AbortOutcome::Terminal(claim.status.clone())
            }
        }
    }

    /// The holder lets go of a live claim by its token. Returns the claim (now retired) so the
    /// Runtime can release or stop the process.
    pub fn retire(&mut self, token: &str, owner_id: &str, how: ClaimRetirement) -> Result<ClaimStatus, ClaimError> {
        let claim = self
            .by_token
            .get_mut(token)
            .ok_or_else(|| ClaimError::new("claim/unknown", format!("no claim {token}")))?;
        if claim.owner_id != owner_id {
            return Err(ClaimError::new("claim/not_holder", format!("claim {token} is not held by {owner_id}")));
        }
        match &claim.status.phase {
            ClaimPhase::Active { .. } => {
                claim.status.phase = ClaimPhase::Retired { how };
                Ok(claim.status.clone())
            }
            ClaimPhase::Retired { how: ClaimRetirement::Transferred } => {
                Err(ClaimError::new("claim/transferred", format!("claim {token} was taken over")))
            }
            phase => Err(ClaimError::new("claim/not_active", format!("claim {token} is {phase:?}"))),
        }
    }

    /// The process under these claims is gone. `stopped`: its holder asked for that (the Runtime's
    /// `stop`), so the claims retire as `stopped`; otherwise they failed.
    pub fn agent_exited(&mut self, agent_id: &str, stopped: bool) -> Vec<ClaimStatus> {
        let mut settled = Vec::new();
        for claim in self.by_token.values_mut() {
            if matches!(&claim.status.phase, ClaimPhase::Active { agent_id: held, .. } if held == agent_id) {
                claim.status.phase = if stopped {
                    ClaimPhase::Retired { how: ClaimRetirement::Stopped }
                } else {
                    ClaimPhase::Failed { reason: format!("agent {agent_id} exited") }
                };
                settled.push(claim.status.clone());
            }
        }
        settled
    }

    /// The holder itself is gone (its connection or window closed): what it reserved is aborted,
    /// what it held is released.
    pub fn owner_gone(&mut self, owner_id: &str) -> Vec<ClaimStatus> {
        let mut changed = Vec::new();
        for claim in self.by_token.values_mut().filter(|claim| claim.owner_id == owner_id) {
            match claim.status.phase {
                ClaimPhase::Reserved | ClaimPhase::Replacing { .. } => claim.status.phase = ClaimPhase::Aborted,
                ClaimPhase::Active { .. } => {
                    claim.status.phase = ClaimPhase::Retired { how: ClaimRetirement::Released }
                }
                _ => continue,
            }
            changed.push(claim.status.clone());
        }
        changed
    }

    pub fn deletion(&self, session_id: &str) -> Option<&SessionDeletion> {
        self.deleting.get(session_id)
    }

    pub fn deletions(&self) -> Vec<SessionDeletion> {
        let mut all: Vec<_> = self.deleting.values().cloned().collect();
        all.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        all
    }

    pub fn restore_deletions(&mut self, deletions: Vec<SessionDeletion>) {
        for mut deletion in deletions {
            // No process of the previous Runtime survives into this one (`attention::reclaim_orphans`).
            if deletion.stage == DeletionStage::Stopping {
                deletion.stage = DeletionStage::Unlinking;
            }
            self.deleting.insert(deletion.session_id.clone(), deletion);
        }
    }

    /// Open (or find) the deletion of a session: every live claim on it retires as deleted.
    pub fn delete_begin(&mut self, session_id: &str, now_ms: i64) -> SessionDeletion {
        for claim in self.by_token.values_mut().filter(|claim| claim.status.session_id == session_id) {
            if matches!(
                claim.status.phase,
                ClaimPhase::Active { .. } | ClaimPhase::Reserved | ClaimPhase::Replacing { .. }
            ) {
                claim.status.phase = ClaimPhase::Retired { how: ClaimRetirement::Deleted };
            }
        }
        self.deleting
            .entry(session_id.to_string())
            .or_insert_with(|| SessionDeletion {
                session_id: session_id.to_string(),
                stage: DeletionStage::Stopping,
                began_at: now_ms,
            })
            .clone()
    }

    pub fn delete_stage(&mut self, session_id: &str, stage: DeletionStage) -> Option<SessionDeletion> {
        let deletion = self.deleting.get_mut(session_id)?;
        deletion.stage = stage;
        Some(deletion.clone())
    }

    /// The deleter reports the unlink. `deleted` closes the deletion for good; a failure keeps it,
    /// with the state the deleter found, until it is retried or abandoned.
    pub fn delete_end(
        &mut self, session_id: &str, outcome: DeletionOutcome,
    ) -> Result<Option<SessionDeletion>, ClaimError> {
        if !self.deleting.contains_key(session_id) {
            return Err(ClaimError::new("delete/unknown", format!("session {session_id} is not being deleted")));
        }
        Ok(match outcome {
            DeletionOutcome::Deleted => {
                self.deleting.remove(session_id);
                None
            }
            DeletionOutcome::Failed { actual_resource_state } => {
                self.delete_stage(session_id, DeletionStage::Failed { actual_resource_state })
            }
        })
    }

    /// The user gives the deletion up: the session may be claimed again.
    pub fn delete_abandon(&mut self, session_id: &str) -> bool {
        self.deleting.remove(session_id).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(attempt: &str) -> ClaimKey {
        ClaimKey { caller_instance_id: "window-1".into(), attempt_id: attempt.into() }
    }

    fn live() -> Option<(String, String)> {
        Some(("agent".into(), "agent#1".into()))
    }

    #[test]
    fn the_same_attempt_gets_the_same_answer_and_a_different_input_is_refused() {
        let mut claims = Claims::new("rt".into());
        let first = claims.claim(key("a"), "w1", "s", "d1", None).unwrap();
        assert_eq!(first.phase, ClaimPhase::Reserved);
        assert_eq!(claims.claim(key("a"), "w1", "s", "d1", live()).unwrap(), first, "no second claim, same token");
        let refused = claims.claim(key("a"), "w1", "s", "d2", None).unwrap_err();
        assert_eq!(refused.code, "claim/digest_mismatch");
        assert_eq!(claims.status(&key("a")), Some(first));
    }

    #[test]
    fn a_reserved_claim_activates_when_its_session_appears_on_the_holders_agent() {
        let mut claims = Claims::new("rt".into());
        let reserved = claims.claim(key("a"), "w1", "s", "d", None).unwrap();
        assert_eq!(claims.activate("w2", "s", "agent", "agent#1"), None, "another holder's agent is not it");
        let active = claims.activate("w1", "s", "agent", "agent#1").unwrap();
        assert_eq!(active.claim_token, reserved.claim_token);
        assert_eq!(active.phase, ClaimPhase::Active { agent_id: "agent".into(), agent_instance_id: "agent#1".into() });
        assert_eq!(claims.activate("w1", "s", "agent", "agent#1"), None, "already active");
    }

    #[test]
    fn a_later_claimant_takes_over_and_the_earlier_abort_reports_transferred() {
        let mut claims = Claims::new("rt".into());
        let mine = claims.claim(key("a"), "w1", "s", "d", live()).unwrap();
        let theirs = claims
            .claim(ClaimKey { caller_instance_id: "window-2".into(), attempt_id: "b".into() }, "w2", "s", "d", live())
            .unwrap();
        assert_ne!(theirs.claim_token, mine.claim_token);
        assert_eq!(claims.status(&key("a")).unwrap().phase, ClaimPhase::Retired { how: ClaimRetirement::Transferred });
        assert!(matches!(claims.abort(key("a")), AbortOutcome::Transferred(_)));
        assert_eq!(
            claims.retire(&mine.claim_token, "w1", ClaimRetirement::Released).unwrap_err().code,
            "claim/transferred"
        );
        assert_eq!(
            claims.retire(&theirs.claim_token, "w1", ClaimRetirement::Released).unwrap_err().code,
            "claim/not_holder"
        );
        assert_eq!(
            claims.retire(&theirs.claim_token, "w2", ClaimRetirement::Stopped).unwrap().phase,
            ClaimPhase::Retired { how: ClaimRetirement::Stopped }
        );
    }

    #[test]
    fn the_same_holder_shares_one_ownership_and_abort_withdraws_only_its_own_attempt() {
        let mut claims = Claims::new("rt".into());
        let first = claims.claim(key("a"), "w1", "s", "d", live()).unwrap();
        let second = claims.claim(key("b"), "w1", "s", "d", None).unwrap();
        assert_eq!(second.claim_token, first.claim_token);
        assert!(matches!(claims.abort(key("b")), AbortOutcome::Aborted(status) if status.phase == ClaimPhase::Aborted));
        assert!(
            matches!(claims.status(&key("a")).unwrap().phase, ClaimPhase::Active { .. }),
            "the other attempt still holds"
        );
        assert!(
            matches!(claims.abort(key("a")), AbortOutcome::ReleaseAndAbort { session_id, .. } if session_id == "s")
        );
        assert!(
            matches!(claims.abort(key("a")), AbortOutcome::Terminal(status) if status.phase == ClaimPhase::Aborted)
        );
    }

    #[test]
    fn an_abort_that_arrives_first_leaves_a_terminal_state_for_the_late_claim() {
        let mut claims = Claims::new("rt".into());
        assert!(matches!(claims.abort(key("a")), AbortOutcome::Aborted(status) if status.session_id.is_empty()));
        let late = claims.claim(key("a"), "w1", "s", "", live()).unwrap();
        assert_eq!(late.phase, ClaimPhase::Aborted);
        assert_eq!(claims.active_token("s"), None, "nothing was taken");
    }

    #[test]
    fn processes_and_holders_going_away_settle_their_claims() {
        let mut claims = Claims::new("rt".into());
        claims.claim(key("a"), "w1", "s", "d", live()).unwrap();
        claims.claim(key("r"), "w1", "t", "d", None).unwrap();
        let failed = claims.agent_exited("agent", false);
        assert_eq!(failed.len(), 1);
        assert!(matches!(&failed[0].phase, ClaimPhase::Failed { reason } if reason.contains("agent")));
        claims.claim(key("s2"), "w1", "u", "d", Some(("agent-2".into(), "agent-2#1".into()))).unwrap();
        let stopped = claims.agent_exited("agent-2", true);
        assert_eq!(stopped[0].phase, ClaimPhase::Retired { how: ClaimRetirement::Stopped });
        let gone = claims.owner_gone("w1");
        assert_eq!(gone.len(), 1, "the failed one is already terminal");
        assert_eq!(gone[0].phase, ClaimPhase::Aborted);
    }

    #[test]
    fn deletion_retires_claims_rejects_new_ones_and_ends_only_on_report() {
        let mut claims = Claims::new("rt".into());
        claims.claim(key("a"), "w1", "s", "d", live()).unwrap();
        let begun = claims.delete_begin("s", 7);
        assert_eq!(begun.stage, DeletionStage::Stopping);
        assert_eq!(claims.status(&key("a")).unwrap().phase, ClaimPhase::Retired { how: ClaimRetirement::Deleted });
        assert_eq!(claims.claim(key("b"), "w1", "s", "d", None).unwrap_err().code, "claim/deleting");
        assert_eq!(
            claims.claim(key("a"), "w1", "s", "d", None).unwrap().phase,
            ClaimPhase::Retired { how: ClaimRetirement::Deleted },
            "the attempt that was there re-asks and gets its terminal state, not the deletion"
        );
        assert_eq!(claims.delete_begin("s", 9).began_at, 7, "idempotent");
        claims.delete_stage("s", DeletionStage::Unlinking).unwrap();
        let failed = claims
            .delete_end("s", DeletionOutcome::Failed { actual_resource_state: "record still open".into() })
            .unwrap()
            .unwrap();
        assert_eq!(failed.stage, DeletionStage::Failed { actual_resource_state: "record still open".into() });
        assert_eq!(claims.claim(key("c"), "w1", "s", "d", None).unwrap_err().code, "claim/deleting");
        assert_eq!(claims.delete_end("s", DeletionOutcome::Deleted).unwrap(), None);
        assert_eq!(claims.delete_end("s", DeletionOutcome::Deleted).unwrap_err().code, "delete/unknown");
        assert!(matches!(claims.claim(key("c"), "w1", "s", "d", None).unwrap().phase, ClaimPhase::Reserved));
    }

    #[test]
    fn a_replacement_retires_the_old_token_and_publishes_a_successor_for_the_same_attempt() {
        let mut claims = Claims::new("rt".into());
        let mine = claims.claim(key("a"), "w1", "s", "d", live()).unwrap();
        assert_eq!(claims.replace_begin(&mine.claim_token, "w2", "r1", "x").unwrap_err().code, "claim/not_holder");
        let prepared = claims.replace_begin(&mine.claim_token, "w1", "r1", "x").unwrap();
        assert_eq!(prepared.stage, ReplacementStage::Prepared);
        assert_eq!(claims.replace_begin(&mine.claim_token, "w1", "r1", "x").unwrap(), prepared, "idempotent");
        assert_eq!(
            claims.replace_begin(&mine.claim_token, "w1", "r1", "y").unwrap_err().code,
            "replace/digest_mismatch"
        );
        assert!(
            matches!(claims.status(&key("a")).unwrap().phase, ClaimPhase::Active { .. }),
            "prepare touches nothing"
        );

        let committed = claims.replace_commit("r1", "w1").unwrap();
        assert_eq!(committed.stage, ReplacementStage::Prepared, "the stage moves when the process is gone");
        assert_eq!(claims.status(&key("a")).unwrap().phase, ClaimPhase::Replacing { replacement_id: "r1".into() });
        assert_eq!(claims.replace_stopped("r1").unwrap().stage, ReplacementStage::Stopped);
        assert_eq!(
            claims.replace_commit("r1", "w1").unwrap().stage,
            ReplacementStage::Stopped,
            "re-asked: current stage"
        );

        assert_eq!(
            claims.activate("w1", "s", "agent", "agent#1"),
            None,
            "a late frame of the old instance publishes nothing"
        );
        let published = claims.activate("w1", "s", "agent", "agent#2").unwrap();
        assert_ne!(published.claim_token, mine.claim_token, "a new token for the new process");
        assert_eq!(
            published.phase,
            ClaimPhase::Active { agent_id: "agent".into(), agent_instance_id: "agent#2".into() }
        );
        assert_eq!(claims.status(&key("a")).unwrap().claim_token, published.claim_token, "the attempt carries on");
        assert_eq!(
            claims.retire(&mine.claim_token, "w1", ClaimRetirement::Released).unwrap_err().code,
            "claim/not_active",
            "the old token cannot be revived"
        );
        assert_eq!(claims.replacement("r1").unwrap().stage, ReplacementStage::Published { claim: published.clone() });
        let aborted = claims.replace_abort("r1", "w1", ClaimRetirement::Stopped).unwrap();
        assert_eq!(aborted.stage, ReplacementStage::Aborted);
        assert_eq!(claims.status(&key("a")).unwrap().phase, ClaimPhase::Retired { how: ClaimRetirement::Stopped });
    }

    #[test]
    fn aborting_a_replacement_before_publish_leaves_the_session_claimable_and_after_takeover_reports_it() {
        let mut claims = Claims::new("rt".into());
        let mine = claims.claim(key("a"), "w1", "s", "d", live()).unwrap();
        claims.replace_begin(&mine.claim_token, "w1", "r1", "x").unwrap();
        assert_eq!(
            claims.replace_abort("r1", "w1", ClaimRetirement::Released).unwrap().stage,
            ReplacementStage::Aborted
        );
        assert!(
            matches!(claims.status(&key("a")).unwrap().phase, ClaimPhase::Active { .. }),
            "before commit the claim stays"
        );

        claims.replace_begin(&mine.claim_token, "w1", "r2", "x").unwrap();
        claims.replace_commit("r2", "w1").unwrap();
        claims.replace_stopped("r2");
        assert_eq!(
            claims.replace_abort("r2", "w1", ClaimRetirement::Released).unwrap().stage,
            ReplacementStage::Aborted
        );
        assert_eq!(claims.status(&key("a")).unwrap().phase, ClaimPhase::Retired { how: ClaimRetirement::Released });
        assert!(matches!(claims.claim(key("b"), "w1", "s", "d", None).unwrap().phase, ClaimPhase::Reserved));

        let mut other = Claims::new("rt".into());
        let held = other.claim(key("x"), "w1", "t", "d", live()).unwrap();
        other.replace_begin(&held.claim_token, "w1", "r3", "x").unwrap();
        other
            .claim(ClaimKey { caller_instance_id: "w9".into(), attempt_id: "y".into() }, "w9", "t", "d", live())
            .unwrap();
        assert_eq!(other.replace_commit("r3", "w1").unwrap_err().code, "replace/transferred");
        assert_eq!(other.replace_abort("r3", "w1", ClaimRetirement::Released).unwrap_err().code, "replace/transferred");
    }

    #[test]
    fn a_restarted_runtime_resumes_deletions_at_unlinking() {
        let mut claims = Claims::new("rt".into());
        claims.restore_deletions(vec![
            SessionDeletion { session_id: "a".into(), stage: DeletionStage::Stopping, began_at: 1 },
            SessionDeletion {
                session_id: "b".into(),
                stage: DeletionStage::Failed { actual_resource_state: "x".into() },
                began_at: 2,
            },
        ]);
        assert_eq!(claims.deletion("a").unwrap().stage, DeletionStage::Unlinking);
        assert_eq!(claims.deletion("b").unwrap().stage, DeletionStage::Failed { actual_resource_state: "x".into() });
        assert!(claims.delete_abandon("a"));
        assert!(!claims.delete_abandon("a"));
        assert_eq!(claims.deletions().len(), 1);
    }
}
