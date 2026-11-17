//! The native runs feature at the process level: the compiled binary starts a
//! run through the model, keeps the run's log, reports the transition on
//! stderr, stops a run that outlives the join timeout, and finds the run again
//! after the process died.

mod support;

use std::fs;

use serde_json::json;
use support::{Binary, MockProvider};

/// The marker the run's command prints, so the log can be recognised.
const MARKER: &str = "maki-native-run-marker";
/// The command of the run that prints the marker and ends.
const PRINT_COMMAND: &str = "printf maki-native-run-marker";
/// The command of the run that outlives the process that started it.
const SLEEP_COMMAND: &str = "sleep 20";
/// How long `--print` waits for a run before it stops it.
const JOIN_TIMEOUT: &str = "30";
/// A join timeout short enough to keep the test quick.
const SHORT_JOIN_TIMEOUT: &str = "1";
/// The tool call the model opens the run with.
const TOOL_ID: &str = "toolu_spawn";
/// What the model says once the run is going.
const ANSWER: &str = "the run is going";
/// The prompt the first process gets.
const PROMPT: &str = "start a background run";
/// The prompt the second process gets.
const LIST_PROMPT: &str = "what runs are there";

/// The arguments of a `maki` run. `--yolo` answers the permission prompt the
/// `spawn` tool raises, which nobody can answer in a headless run.
fn args(extra: &[&str], join_timeout: &str, prompt: &str) -> Vec<String> {
    let mut all = vec![
        "--print".to_owned(),
        "--trust".to_owned(),
        "--yolo".to_owned(),
        "--model".to_owned(),
        support::MOCK_MODEL.to_owned(),
        "--join-timeout".to_owned(),
        join_timeout.to_owned(),
    ];
    all.extend(extra.iter().map(|arg| (*arg).to_owned()));
    all.push(prompt.to_owned());
    all
}

/// The shell run the model starts.
fn spawn_run(command: &str) -> serde_json::Value {
    json!({
        "kind": "shell",
        "description": "a run for the test",
        "command": command,
    })
}

/// A provider that answers the first request with a run and the rest with
/// text.
fn provider_for(command: &'static str) -> MockProvider {
    MockProvider::serve(move |index, _body| {
        if index == 0 {
            support::tool_use_sse(TOOL_ID, "spawn", &spawn_run(command))
        } else {
            support::text_sse(ANSWER)
        }
    })
}

#[test]
fn a_run_started_through_the_binary_keeps_its_log() {
    let _home = support::isolated_home();
    let project = support::project();
    let provider = provider_for(PRINT_COMMAND);

    let outcome = Binary::spawn(
        &args(&[], JOIN_TIMEOUT, PROMPT),
        project.path(),
        &support::child_env(&provider),
    )
    .finish();

    assert_eq!(outcome.code, Some(0), "stderr: {}", outcome.stderr);
    let logs = support::run_logs();
    assert_eq!(logs.len(), 1, "one run log: {logs:?}");
    let log = fs::read_to_string(&logs[0]).expect("the run log");
    assert!(
        log.contains(MARKER),
        "the log holds the marker: {log}\nstderr: {}",
        outcome.stderr
    );
    assert!(
        outcome.stderr.contains("maki: run sh-"),
        "the transition is on stderr: {}",
        outcome.stderr
    );
    assert!(
        outcome.stderr.contains("started [shell]"),
        "the transition is on stderr: {}",
        outcome.stderr
    );
}

#[test]
fn a_run_that_outlives_the_join_timeout_is_stopped() {
    let _home = support::isolated_home();
    let project = support::project();
    let provider = provider_for(SLEEP_COMMAND);

    let outcome = Binary::spawn(
        &args(&[], SHORT_JOIN_TIMEOUT, PROMPT),
        project.path(),
        &support::child_env(&provider),
    )
    .finish();

    assert_eq!(outcome.code, Some(0), "stderr: {}", outcome.stderr);
    assert!(
        outcome
            .stderr
            .contains("maki: join timeout of 1s reached, stopped 1 run(s): sh-"),
        "the timeout is on stderr: {}",
        outcome.stderr
    );
}

#[test]
fn a_run_survives_the_death_of_its_session_and_comes_back() {
    let _home = support::isolated_home();
    let project = support::project();
    let first = provider_for(SLEEP_COMMAND);

    let binary = Binary::spawn(
        &args(&[], JOIN_TIMEOUT, PROMPT),
        project.path(),
        &support::child_env(&first),
    );
    // The run started and the session reached the disk, so the next start can
    // continue it and find the journal it left.
    assert!(
        support::wait_for(|| !support::run_logs().is_empty()),
        "the run started"
    );
    assert!(
        support::wait_for(|| !support::session_files().is_empty()),
        "the session reached the disk"
    );
    let outcome = binary.kill();
    assert_ne!(outcome.code, Some(0), "the process died hard");

    // The journal kept the run, so the next start knows about it.
    let journals = support::run_journals();
    assert_eq!(journals.len(), 1, "one journal: {journals:?}");
    let journal = fs::read_to_string(&journals[0]).expect("the journal");
    assert!(
        journal.contains("\"t\":\"spawn\""),
        "the run is in it: {journal}"
    );

    // The next start continues the session, so it reads the same journal.
    let second = MockProvider::serve(|index, _body| {
        if index == 0 {
            support::tool_use_sse(TOOL_ID, "runs", &json!({}))
        } else {
            support::text_sse(ANSWER)
        }
    });
    let outcome = Binary::spawn(
        &args(&["--continue"], JOIN_TIMEOUT, LIST_PROMPT),
        project.path(),
        &support::child_env(&second),
    )
    .finish();

    assert_eq!(outcome.code, Some(0), "stderr: {}", outcome.stderr);
    // The model saw the run the dead process left behind.
    let seen = second
        .requests()
        .iter()
        .map(|request| request.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(seen.contains("sh-"), "the run is in the request: {seen}");
    assert!(
        seen.contains("was lost"),
        "the run is reported as lost: {seen}"
    );
    // The journal learned how the run ended, so a third start has nothing to
    // settle again.
    let journal = fs::read_to_string(&journals[0]).expect("the journal");
    assert!(
        journal.contains("\"t\":\"term\"") && journal.contains("\"reason\":\"lost\""),
        "the journal settled the run: {journal}"
    );
    // The log of the run the dead process started is still there.
    let logs = support::run_logs();
    assert_eq!(logs.len(), 1, "the run log survived: {logs:?}");
}
