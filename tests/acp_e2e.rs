//! The ACP server at the process level: the compiled binary speaks ndjson
//! over stdio, answers a prompt against the mock provider, and says why the
//! turn ended.

mod support;

use serde_json::{Value, json};
use support::{Binary, MockProvider};

/// The protocol version the client asks for.
const PROTOCOL_VERSION: i64 = 1;
/// The question the client sends.
const QUESTION: &str = "what is the answer?";
/// What the model answers.
const ANSWER: &str = "the answer from the model";

/// The arguments of the ACP server.
fn args() -> Vec<String> {
    vec![
        "--trust".to_owned(),
        "acp".to_owned(),
        "--model".to_owned(),
        support::MOCK_MODEL.to_owned(),
    ]
}

/// One ndjson request.
fn request(id: i64, method: &str, params: Value) -> String {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string()
}

#[test]
fn the_acp_server_answers_a_prompt_over_ndjson() {
    let _home = support::isolated_home();
    let project = support::project();
    let provider = MockProvider::serve(|_index, _body| support::text_sse(ANSWER));
    let mut binary = Binary::with_stdin(&args(), project.path(), &support::child_env(&provider));

    binary.write_line(&request(
        1,
        "initialize",
        json!({"protocolVersion": PROTOCOL_VERSION, "clientCapabilities": {}}),
    ));
    let initialize = binary.response(1);
    assert_eq!(
        initialize["result"]["protocolVersion"], PROTOCOL_VERSION,
        "the protocol version: {initialize}"
    );

    binary.write_line(&request(
        2,
        "session/new",
        json!({"cwd": project.path().to_string_lossy(), "mcpServers": []}),
    ));
    let session = binary.response(2);
    let session_id = session["result"]["sessionId"]
        .as_str()
        .expect("a session id")
        .to_owned();
    assert!(!session_id.is_empty(), "the session id: {session}");

    binary.write_line(&request(
        3,
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": QUESTION}]}),
    ));
    let prompt = binary.response(3);
    assert_eq!(
        prompt["result"]["stopReason"], "end_turn",
        "the turn ended: {prompt}"
    );

    // The model got the question the client sent.
    let seen = provider
        .requests()
        .iter()
        .map(|request| request.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        seen.contains(QUESTION),
        "the question reached the model: {seen}"
    );

    binary.close_stdin();
    let outcome = binary.finish();
    assert_eq!(outcome.code, Some(0), "stderr: {}", outcome.stderr);
    // The answer reached the client as a message chunk.
    assert!(
        outcome
            .stdout
            .lines()
            .any(|line| line.contains("agent_message_chunk") && line.contains(ANSWER)),
        "the answer reached the client: {}",
        outcome.stdout
    );
}
