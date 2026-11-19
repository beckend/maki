//! The Esc key at the process level: the real binary under a pty, the keys a
//! user types, and the provider that answers a turn or stops answering it.
//!
//! The double Esc stops the agent of the view in front: the parent in the
//! parent view, the run in a run's view. The provider is the witness: a
//! cancelled turn drops the request in flight, and the client hangs up.

#![cfg(unix)]

mod support;

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use support::tui::Tui;
use support::{MOCK_MODEL, MockProvider, Reply};

/// The key that stops a turn, once the app expects it.
const ESC: &[u8] = b"\x1b";
/// The key that takes the row in front.
const ENTER: &[u8] = b"\r";
/// The prompt the binary gets on its command line.
const PROMPT: &str = "tell me something";
/// The prompt the parent gets in its second turn.
const SECOND_PROMPT: &str = "and now something else";
/// What the parent answers when its first turn ends.
const ANSWER: &str = "the run is going";
/// The tool the model calls to start a native run.
const SPAWN_TOOL: &str = "spawn";
/// The id of the tool call the model opens.
const TOOL_ID: &str = "toolu_spawn";
/// The label the run gets, so the test can see it on screen.
const RUN_LABEL: &str = "esc-e2e-reader";
/// The label of the run in the status bar, when its view is in front.
const RUN_LABEL_IN_BAR: &str = "[esc-e2e-reader]";
/// The prompt the run gets, so the test can tell its request from the
/// parent's.
const RUN_PROMPT: &str = "run-marker-42";
/// What the app draws while it works. It is the terminal title, an escape
/// sequence the app writes whole.
const WORKING: &str = "state=working";
/// What the app draws when it has nothing to do. It is the terminal title,
/// an escape sequence the app writes whole.
const IDLE: &str = "state=idle";
/// The hint the app draws when the first Esc armed the cancel.
const CANCEL_HINT: &str = "Press esc again to stop";
/// What the app draws when the turn in front was cancelled.
const TURN_CANCELLED: &str = "Cancelled.";
/// What the app draws when the task in front was cancelled.
const TASK_CANCELLED: &str = "Cancelled";
/// What the app draws in the input of a task's view.
const TASK_INPUT: &str = "message for this subagent";
/// What the app draws when the runs picker is open.
const RUNS_PICKER: &str = " Runs ";
/// What the app draws when the command palette offers the runs command.
const RUNS_COMMAND: &str = "List background runs and open their tabs";
/// The request of the parent in the parent view: it is the only one there.
const PARENT_REQUEST: usize = 0;

/// The arguments of a `maki` run in a terminal. The prompt goes on the
/// command line: the standard input is a terminal here, and a prompt read
/// from it would never arrive.
fn args(prompt: &str) -> Vec<String> {
    vec![
        "--trust".to_owned(),
        "--yolo".to_owned(),
        "--model".to_owned(),
        MOCK_MODEL.to_owned(),
        prompt.to_owned(),
    ]
}

/// The environment the child gets: the sandbox, the mock, and the terminal
/// the TUI draws on.
fn env(sandbox: &support::Sandbox, provider: &MockProvider) -> Vec<(String, String)> {
    let mut env = sandbox.child_env(provider);
    env.push(("MAKI_TRUECOLOR".to_owned(), "1".to_owned()));
    env.push(("TERM".to_owned(), "xterm-256color".to_owned()));
    env
}

/// Starts the binary under a pty, or tells the world that this machine has
/// none: a sandbox without a pty cannot show a TUI, and the test says so
/// instead of failing.
fn start(args: &[String], cwd: &Path, env: &[(String, String)]) -> Option<Tui> {
    let tui = Tui::spawn(args, cwd, env);
    if tui.is_none() {
        eprintln!("skipped: this machine has no pty to show the TUI on");
    }
    tui
}

/// The run the model starts: an agent that goes on working after the turn
/// that started it is over.
fn spawn_input() -> serde_json::Value {
    json!({
        "kind": "agent",
        "description": RUN_LABEL,
        "prompt": RUN_PROMPT,
        "subagent_type": "general",
        "notify": "never",
    })
}

/// The provider of the run's view. The parent's first request starts the
/// run, the parent's second request ends its turn, and the run's own request
/// stays in flight until somebody stops it. The parent's second turn waits
/// too, so the parent has a request of its own while the run is stopped.
fn provider_for_run() -> MockProvider {
    let parents = AtomicUsize::new(0);
    MockProvider::serve_with(move |_index, body| {
        let text = body.to_string();
        if text.contains(SECOND_PROMPT) {
            return Reply::Hold;
        }
        if text.contains(TOOL_ID) {
            return Reply::Body(support::text_sse(ANSWER));
        }
        if parents.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::Body(support::tool_use_sse(TOOL_ID, SPAWN_TOOL, &spawn_input()));
        }
        Reply::Hold
    })
}

/// The request of the run: it carries the run's prompt, and none of the
/// parent's tool call, whose history the run knows nothing about.
fn run_request(provider: &MockProvider) -> Option<usize> {
    provider.requests().iter().position(|request| {
        let text = request.to_string();
        text.contains(RUN_PROMPT) && !text.contains(TOOL_ID)
    })
}

/// The terminal writes only the cells that change, so a word can come in
/// pieces: the diff skips a cell whose character did not change and moves
/// the cursor over it. The screen is put back together, so the search finds
/// the word the user sees.
#[test]
fn a_word_the_terminal_wrote_in_pieces_is_one_word_again() {
    // The status bar before the flash: the warning about the provider the
    // app fell back to, whose letters the flash shares.
    let before = b"\x1b[40;1H\x1b[38;2;255;180;84m \xe2\xa0\x8b\x1b[1m\x1b[38;2;149;230;203m [BUILD]\x1b[22m\x1b[39m Opencode Go: unknown provider 'opencode-go' (using static fallback)\x1b[38;2;90;102;115m..pX1PCD5";
    // The flash: the same letters as the warning, so the diff skips them
    // and the word arrives in four pieces.
    let flash = b"\x1b[40;12H\x1b[38;2;255;143;64mPr\x1b[40;15H\x1b[38;2;255;143;64mss esc agai\x1b[40;27H\x1b[38;2;255;143;64m t\x1b[40;30H\x1b[38;2;255;143;64m stop...";
    let mut bytes = before.to_vec();
    bytes.extend_from_slice(flash);

    assert!(
        !String::from_utf8_lossy(&bytes).contains(CANCEL_HINT),
        "the bytes do not hold the word in one piece"
    );
    let screen = support::tui::screen_of(&bytes);
    assert!(
        screen.iter().any(|row| row.contains(CANCEL_HINT)),
        "the screen holds the word: {screen:?}"
    );

    // A word that is drawn across two rows is not a word on the screen.
    let across = b"\x1b[40;100HPress\x1b[39;1H esc again to stop";
    let screen = support::tui::screen_of(across);
    assert!(
        !screen.iter().any(|row| row.contains(CANCEL_HINT)),
        "a word across two rows is no word: {screen:?}"
    );
}

/// The parent's view: the turn in front is the parent's own, and the double
/// Esc ends it.
#[test]
fn a_double_esc_in_the_parent_view_stops_the_parent_turn() {
    let sandbox = support::sandbox();
    let project = support::project();
    let provider = MockProvider::serve_with(|_index, _body| Reply::Hold);
    let Some(mut tui) = start(&args(PROMPT), project.path(), &env(&sandbox, &provider)) else {
        return;
    };

    // The turn is in flight: the request reached the provider and the app
    // says it is working.
    assert!(
        support::wait_for(|| provider.requests().len() == 1),
        "the request reached the provider"
    );
    assert!(
        tui.wait_for_title(WORKING),
        "the app is working: {}",
        tui.drawn()
    );

    // The first Esc arms the cancel, and the app says so.
    let before = tui.mark();
    tui.write(ESC);
    assert!(
        tui.wait_for(CANCEL_HINT),
        "the app armed the cancel: {}",
        tui.drawn()
    );

    // The second Esc ends the turn, and the client hangs up on the request
    // that was in flight.
    tui.write(ESC);
    assert!(
        support::wait_for(|| provider.hung_up() == vec![PARENT_REQUEST]),
        "the client hung up on the turn: {:?}",
        provider.hung_up()
    );
    assert!(
        tui.wait_for_since(before, TURN_CANCELLED),
        "the turn is cancelled: {}",
        tui.drawn_since(before)
    );
    assert!(
        tui.wait_for_title_since(before, IDLE),
        "the app is idle: {}",
        tui.drawn_since(before)
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "the cancelled turn sent no second request"
    );
    assert!(tui.alive(), "a cancel is not an exit");
}

/// A run's view: the double Esc stops the run in front, and the parent goes
/// on with the request it had in flight.
#[test]
fn a_double_esc_in_a_run_view_stops_that_run_and_not_the_parent() {
    let sandbox = support::sandbox();
    let project = support::project();
    let provider = provider_for_run();
    let Some(mut tui) = start(&args(PROMPT), project.path(), &env(&sandbox, &provider)) else {
        return;
    };

    // The parent starts the run and ends its turn, so the run lives past it.
    assert!(
        tui.wait_for(ANSWER),
        "the parent's turn ended: {}",
        tui.drawn()
    );
    assert!(
        support::wait_for(|| run_request(&provider).is_some()),
        "the run's request arrived"
    );

    // The parent asks for something else, so it has a request of its own
    // while the run is stopped.
    tui.write(SECOND_PROMPT.as_bytes());
    tui.write(ENTER);
    assert!(
        support::wait_for(|| provider
            .requests()
            .iter()
            .any(|request| request.to_string().contains(SECOND_PROMPT))),
        "the parent's second request arrived"
    );

    // The run's view comes to the front: the picker opens on it, and the
    // tab of the run takes the keyboard.
    tui.write(b"/runs");
    assert!(
        tui.wait_for(RUNS_COMMAND),
        "the palette offers the runs command: {}",
        tui.drawn()
    );
    tui.write(ENTER);
    assert!(
        tui.wait_for(RUNS_PICKER),
        "the runs picker is open: {}",
        tui.drawn()
    );
    tui.write(ENTER);
    assert!(
        tui.wait_for(TASK_INPUT),
        "the run's view is in front: {}",
        tui.drawn()
    );
    assert!(
        tui.wait_for(RUN_LABEL_IN_BAR),
        "the status bar names the run: {}",
        tui.screen().join("\n")
    );

    // The first Esc arms the cancel in the run's view, and the app says so.
    let before = tui.mark();
    tui.write(ESC);
    assert!(
        tui.wait_for(CANCEL_HINT),
        "the app armed the cancel: {}",
        tui.drawn()
    );

    // The second Esc stops the run in front: its request hangs up, and the
    // parent's request does not.
    tui.write(ESC);
    let run = run_request(&provider).expect("the run made a request");
    assert!(
        support::wait_for(|| provider.hung_up() == vec![run]),
        "the run's request hung up: {:?}",
        provider.hung_up()
    );
    assert!(
        tui.wait_for_since(before, TASK_CANCELLED),
        "the run shows as cancelled: {}",
        tui.drawn_since(before)
    );
    assert!(
        !tui.shows(TURN_CANCELLED),
        "the parent's turn went on: {}",
        tui.screen().join("\n")
    );
    assert!(tui.alive(), "a cancel is not an exit");
}
