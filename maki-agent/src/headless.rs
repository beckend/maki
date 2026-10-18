use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use maki_config::{ModelPolicy, ProjectConfig, SessionDefaults};
use maki_providers::model::Model;
use maki_providers::provider::{self, Provider};
use maki_providers::{AgentError, Message, Timeouts};
use maki_storage::StateDir;
use maki_storage::id::SessionRef;
use maki_storage::sessions::SessionClaim;
use serde_json::Value;
use smol::lock::Mutex;
use tracing::error;

use crate::agent::{self, RunContext, RunContextBuilder};
use crate::cancel::{CancelMap, CancelToken};
use crate::permissions::{PermissionManager, PluginRuleStore};
use crate::prompt::ResolvedPrompts;
use crate::runs::{Joined, UnitHub, WakeSink};
use crate::session::{Resumed, SessionTrack};
use crate::template;
use crate::tools::{FileAccess, LocalTools, RequestTools, ToolAudience, ToolRegistry};
use crate::types::EventSender;
use crate::{
    Agent, AgentConfig, AgentEvent, AgentInput, AgentMode, AgentParams, DoneReason,
    EventStreamGuard, ImageSource, InputSource, McpHandle, McpSession, PermissionsConfig,
    RunLedger, SessionEvents, SessionMailbox, ToolDeferral, ToolOutputLines, event_stream,
};

/// Closing turns in a row that may be needed to drain quiet items.
const MAX_CLOSING_TURNS: u32 = 3;

/// Hands wakes from `drive_main` to the task that owns the agent.
struct ChannelWake(flume::Sender<()>);

impl WakeSink for ChannelWake {
    fn start_main_run(&self, _preamble: Vec<Message>) {
        let _ = self.0.send(());
    }
}

fn blank_turn(defaults: SessionDefaults, mode: AgentMode) -> AgentInput {
    AgentInput::from_defaults(
        String::new(),
        mode,
        Vec::new(),
        defaults,
        InputSource::Headless,
    )
}

/// Runs the turns that come after the first one: a turn for every wake
/// `drive_main` hands over, and a closing turn that delivers what the wake
/// budget kept quiet. Returns the result of the last turn, once no run is
/// left or the join bound passed (then everything still running is stopped).
async fn follow_up(
    agent: &mut Agent<'_>,
    hub: &Arc<UnitHub>,
    wake_rx: &flume::Receiver<()>,
    defaults: SessionDefaults,
    join_timeout: Duration,
) -> Result<DoneReason, AgentError> {
    enum Step {
        Wake,
        Joined(Joined),
    }
    let mut last = Ok(DoneReason::EndTurn);
    let mut closing = 0;
    let deadline = hub.deadline_after(join_timeout);
    hub.main_turn_ends();
    loop {
        // Past the bound a wake no longer wins: a model that starts a run in
        // every turn would otherwise keep the process alive for good.
        let step = if hub.deadline_passed(deadline) {
            Step::Joined(hub.join_until(deadline).await)
        } else {
            futures_lite::future::or(
                async {
                    let _ = wake_rx.recv_async().await;
                    Step::Wake
                },
                async { Step::Joined(hub.join_until(deadline).await) },
            )
            .await
        };
        match step {
            Step::Wake if !hub.main_has_unread() => {
                // A closing turn already read what this wake was for.
                hub.main_turn_ends();
                continue;
            }
            Step::Wake => closing = 0,
            Step::Joined(Joined::Quiet) => return last,
            Step::Joined(Joined::Pending) if closing < MAX_CLOSING_TURNS => {
                closing += 1;
                hub.main_turn_begins();
            }
            Step::Joined(Joined::Pending) => return last,
            Step::Joined(Joined::TimedOut { stopped }) => {
                eprintln!(
                    "maki: join timeout of {}s reached, stopped {} run(s): {}",
                    join_timeout.as_secs(),
                    stopped.len(),
                    stopped.join(", ")
                );
                return last;
            }
        }
        last = agent.run(blank_turn(defaults, AgentMode::Build)).await;
        hub.main_turn_ends();
        if !matches!(last, Ok(reason) if reason != DoneReason::Cancelled) {
            return last;
        }
    }
}

const SESSION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolve a provider, or report the failure on the event stream and let the
/// caller give up. Every site that reaches a provider comes through here, so a
/// failed connect cannot be silent on one path and loud on another. What giving
/// up means is the caller's call: abandon the session, or skip this turn.
async fn connect(
    model: &mut Model,
    timeouts: Timeouts,
    event_tx: &EventSender,
) -> Option<Arc<dyn Provider>> {
    match provider::from_model_async(model, timeouts).await {
        Ok(p) => Some(Arc::from(p)),
        Err(e) => {
            error!(error = %e, "provider error");
            let _ = event_tx.send(AgentEvent::error(&e));
            None
        }
    }
}

pub struct HeadlessParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub prompt: String,
    pub images: Vec<ImageSource>,
    pub prompt_slots: ResolvedPrompts,
    pub excluded_tools: Vec<&'static str>,
    pub mcp_handle: Option<McpHandle>,
    pub initial_wd: PathBuf,
    pub resumed: Resumed,
    /// The right to write [`Self::resumed`]'s session, taken before its
    /// transcript was read.
    pub claim: SessionClaim,
    /// Where the transcript is written. The same dir the caller resolved the
    /// session from, so a run cannot read one session and write another.
    pub storage: StateDir,
    /// The `always_*` knobs. A headless run has no toggle UI, so config is the
    /// whole answer. The model gate stays in `RequestOptions::clamped`.
    pub defaults: SessionDefaults,
    pub model_policy: Arc<ModelPolicy>,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub project_config: ProjectConfig,
    /// How long the process waits for background runs after the last turn
    /// before it stops them. Only used when runs are enabled.
    pub join_timeout: Duration,
}

pub struct HeadlessHandle {
    pub tool_names: Vec<String>,
    pub session_id: SessionRef,
    pub cwd: String,
    pub task: smol::Task<()>,
}

/// Read once at startup. A headless host has no `/cd` and its slots come from
/// the caller, so nothing here changes mid-session.
struct ContextSpec {
    vars: template::Vars,
    instructions: String,
    slots: Arc<ResolvedPrompts>,
    config: AgentConfig,
    excluded_tools: Vec<&'static str>,
    mcp: bool,
    system_override: Option<String>,
    append_system: Option<String>,
}

impl ContextSpec {
    /// Takes the handle rather than a `bool`. Two flags side by side are one
    /// silent swap away from a session that describes tools it cannot call.
    fn new(
        vars: template::Vars,
        instructions: &agent::Instructions,
        slots: Arc<ResolvedPrompts>,
        config: AgentConfig,
        excluded_tools: Vec<&'static str>,
        mcp: Option<&McpHandle>,
    ) -> Self {
        Self {
            vars,
            instructions: instructions.text.clone(),
            slots,
            config,
            excluded_tools,
            mcp: mcp.is_some(),
            system_override: None,
            append_system: None,
        }
    }

    fn render(&self, model: &Model, workflow: bool) -> RunContext {
        let tools = RequestTools::build(
            ToolRegistry::global(),
            &self.vars,
            model,
            &self.config,
            &self.excluded_tools,
            workflow,
            self.mcp,
        );
        let mut context = match &self.system_override {
            Some(system) => {
                RunContext::fixed(system.clone(), tools).with_prompt_policy(&self.slots)
            }
            None => RunContext::render(&self.vars, &self.instructions, &self.slots, model, tools),
        };
        if let Some(append) = &self.append_system {
            for text in [&mut context.system, &mut context.authored] {
                text.push('\n');
                text.push_str(append);
            }
        }
        context
    }

    fn builder(self) -> RunContextBuilder {
        Arc::new(move |model, workflow| self.render(model, workflow))
    }
}

/// Names advertised to SDK clients: base tools plus what the first request
/// would put in the model's context from MCP (always-load definitions and
/// `tool_search`). Probed as [`ToolDeferral::Client`] because the native
/// array lists every deferred definition too.
fn advertised_tool_names(tools: &Value, mcp: Option<&McpSession>) -> Vec<String> {
    let mut probe = tools.clone();
    if let Some(mcp) = mcp {
        mcp.extend_tools(&mut probe, ToolDeferral::Client);
    }
    extract_tool_names(&probe)
}

pub fn spawn(params: HeadlessParams) -> (HeadlessHandle, SessionEvents) {
    let working_dir = params.initial_wd.to_string_lossy().into_owned();
    let mode = AgentMode::Build;
    let vars = template::env_vars();
    let instructions = agent::load_instructions(&vars.apply("{cwd}"));
    let prompt_slots = Arc::new(params.prompt_slots);
    let spec = ContextSpec::new(
        vars,
        &instructions,
        Arc::clone(&prompt_slots),
        params.config.clone(),
        params.excluded_tools,
        params.mcp_handle.as_ref(),
    );
    let initial = spec.render(&params.model, params.defaults.workflow);
    let context = spec.builder();

    let mcp = params
        .mcp_handle
        .clone()
        .map(|h| McpSession::new(h, &params.resumed.history));
    let tool_names = advertised_tool_names(initial.tools.definitions(), mcp.as_ref());

    let (guard, events) = event_stream();
    let event_tx = guard.sender(0);

    let runs = crate::runs::start_session(
        &params.config.runs,
        event_tx.clone(),
        params.resumed.id.id(),
    );
    let session_ref = params.resumed.id.clone();
    let mailbox = SessionMailbox::register(session_ref.id());
    let run_session_ref = session_ref.clone();
    let defaults = params.defaults;
    let working_dir_path = params.initial_wd.clone();
    let task_working_dir = working_dir.clone();
    let join_timeout = params.join_timeout;
    let task = smol::spawn(run_session(guard, params.mcp_handle.clone(), async move {
        let mut model = params.model;
        let Some(provider) = connect(&mut model, params.timeouts, &event_tx).await else {
            return;
        };
        let mut track = SessionTrack::open(
            params.resumed,
            params.claim,
            params.storage,
            &task_working_dir,
        );

        let error_tx = event_tx.clone();
        // Outlives `agent`, so dropping it writes the transcript once the agent
        // that produced it is gone, without this body saying so.
        let mut turn = track.turn(model.spec());
        let mut agent = Agent::new(
            AgentParams {
                provider,
                model: model.clone(),
                config: params.config,
                tool_output_lines: ToolOutputLines::default(),
                permissions: Arc::new(PermissionManager::new(
                    params.permissions_config,
                    working_dir_path,
                    params.project_config,
                    params.plugin_rules,
                )),
                session_id: Some(run_session_ref),
                background_for: None,
                task_id: None,
                mailbox: Some(mailbox),
                timeouts: params.timeouts,
                file_access: FileAccess::fresh(),
                prompt_slots: Arc::clone(&prompt_slots),
                subagent_cancels: Arc::new(CancelMap::new()),
                ledger: Arc::new(RunLedger::default()),
                registry: Arc::clone(ToolRegistry::global_arc()),
                audience: ToolAudience::MAIN,
                model_policy: Arc::clone(&params.model_policy),
                runs: runs.clone(),
            },
            turn.run_params(event_tx, context),
        )
        .with_loaded_instructions(instructions.loaded)
        .with_mcp(mcp);

        let follow = runs.as_ref().map(|ctx| {
            let (wake_tx, wake_rx) = flume::unbounded();
            let hub = Arc::clone(&ctx.hub);
            // The first turn is the caller's; nothing wakes `main` before it ends.
            hub.main_turn_begins();
            let driver = smol::spawn(Arc::clone(&hub).drive_main(ChannelWake(wake_tx)));
            (hub, wake_rx, driver)
        });
        let mut result = agent
            .run(AgentInput::from_defaults(
                params.prompt,
                mode,
                params.images,
                defaults,
                InputSource::Headless,
            ))
            .await;
        if let Some((hub, wake_rx, driver)) = follow {
            if matches!(result, Ok(reason) if reason != DoneReason::Cancelled) {
                result = follow_up(&mut agent, &hub, &wake_rx, defaults, join_timeout).await;
            }
            driver.cancel().await;
            hub.shutdown();
        }
        drop(agent);

        if let Err(e) = result {
            error!(error = %e, "agent error");
            let _ = error_tx.send(AgentEvent::error(&e));
        }
    }));
    (
        HeadlessHandle {
            tool_names,
            session_id: session_ref,
            cwd: working_dir,
            task,
        },
        events,
    )
}

pub struct InteractiveParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub prompt_slots: Arc<ResolvedPrompts>,
    pub excluded_tools: Vec<&'static str>,
    pub mcp_handle: Option<McpHandle>,
    pub initial_wd: PathBuf,
    pub resumed: Resumed,
    /// See [`HeadlessParams::claim`].
    pub claim: SessionClaim,
    /// See [`HeadlessParams::storage`].
    pub storage: StateDir,
    pub yolo: bool,
    pub system_prompt_override: Option<String>,
    pub append_system_prompt: Option<String>,
    /// The `always_*` knobs. `workflow` picks the tool catalog here; the rest
    /// are what a host without toggles puts on every [`AgentInput`] it sends.
    pub defaults: SessionDefaults,
    pub model_policy: Arc<ModelPolicy>,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub project_config: ProjectConfig,
    /// Host-side overrides that shadow a registered tool's execution while
    /// keeping its advertised schema (e.g. ACP answers `question` via elicitation).
    pub local_tools: LocalTools,
    /// Start a turn of `main` when a waking run notice arrives while it is
    /// idle. The turn ends with its own `Done`, which a client that pairs
    /// `Done` with its own prompt (ACP) would take for the answer to a prompt
    /// it sent meanwhile, so such hosts leave this off.
    pub wake_main: bool,
}

pub struct InteractiveHandle {
    pub tool_names: Vec<String>,
    pub input_tx: flume::Sender<AgentInput>,
    pub answer_tx: flume::Sender<String>,
    pub cancel_tx: flume::Sender<()>,
    pub model_tx: flume::Sender<Model>,
    pub session_id: SessionRef,
    pub permissions: Arc<PermissionManager>,
    pub task: smol::Task<()>,
}

pub fn spawn_interactive(params: InteractiveParams) -> (InteractiveHandle, SessionEvents) {
    let vars = template::env_vars();
    let instructions = agent::load_instructions(&vars.apply("{cwd}"));
    let spec = ContextSpec {
        system_override: params.system_prompt_override,
        append_system: params.append_system_prompt,
        ..ContextSpec::new(
            vars,
            &instructions,
            Arc::clone(&params.prompt_slots),
            params.config.clone(),
            params.excluded_tools,
            params.mcp_handle.as_ref(),
        )
    };
    let initial = spec.render(&params.model, params.defaults.workflow);
    let context = spec.builder();

    let mcp = params
        .mcp_handle
        .clone()
        .map(|h| McpSession::new(h, &params.resumed.history));
    let tool_names = advertised_tool_names(initial.tools.definitions(), mcp.as_ref());

    let (guard, events) = event_stream();
    let base_tx = guard.sender(0);
    let (input_tx, input_rx) = flume::unbounded::<AgentInput>();
    let (answer_tx, answer_rx) = flume::unbounded::<String>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (model_tx, model_rx) = flume::unbounded::<Model>();

    let runs =
        crate::runs::start_session(&params.config.runs, base_tx.clone(), params.resumed.id.id());
    let session_ref = params.resumed.id.clone();
    let mailbox = SessionMailbox::register(session_ref.id());

    let working_dir = params.initial_wd.to_string_lossy().into_owned();
    let mut permissions_config = params.permissions_config;
    permissions_config.yolo |= params.yolo;
    let permissions = Arc::new(PermissionManager::new(
        permissions_config,
        params.initial_wd,
        params.project_config,
        Arc::clone(&params.plugin_rules),
    ));

    let answer_rx = Arc::new(Mutex::new(answer_rx));
    let file_access = FileAccess::fresh();

    let session_ref_clone = session_ref.clone();
    let task_permissions = Arc::clone(&permissions);
    let task = smol::spawn(run_session(guard, params.mcp_handle.clone(), async move {
        let mut model = params.model;
        let Some(mut provider) = connect(&mut model, params.timeouts, &base_tx).await else {
            return;
        };
        let mut track =
            SessionTrack::open(params.resumed, params.claim, params.storage, &working_dir);
        let mut run_id: u64 = 0;

        let waker = runs.as_ref().filter(|_| params.wake_main).map(|ctx| {
            let (wake_tx, wake_rx) = flume::unbounded();
            let hub = Arc::clone(&ctx.hub);
            let driver = smol::spawn(Arc::clone(&hub).drive_main(ChannelWake(wake_tx)));
            (hub, wake_rx, driver)
        });
        let mut last_mode = AgentMode::Build;
        loop {
            enum Next {
                Input(AgentInput),
                Wake,
            }
            let next = match &waker {
                Some((_, wake_rx, _)) => {
                    futures_lite::future::or(
                        async { input_rx.recv_async().await.ok().map(Next::Input) },
                        async { wake_rx.recv_async().await.ok().map(|()| Next::Wake) },
                    )
                    .await
                }
                None => input_rx.recv_async().await.ok().map(Next::Input),
            };
            let input = match next {
                None => break,
                Some(Next::Input(input)) => {
                    last_mode = input.mode.clone();
                    input
                }
                Some(Next::Wake) => {
                    // A user turn that ran first may have drained what the
                    // wake was for.
                    if let Some((hub, ..)) = &waker
                        && !hub.main_has_unread()
                    {
                        hub.main_turn_ends();
                        continue;
                    }
                    blank_turn(params.defaults, last_mode.clone())
                }
            };
            if let Some((hub, ..)) = &waker {
                hub.main_turn_begins();
            }
            let (trigger, cancel) = CancelToken::new();
            let cancel_task = smol::spawn({
                let cancel_rx = cancel_rx.clone();
                async move {
                    if cancel_rx.recv_async().await.is_ok() {
                        trigger.cancel();
                    }
                }
            });

            // MCP connects in the background, so a prompt that beats it waits
            // here instead of shipping a turn without the MCP tools. The wait
            // is racing cancel: a slow server must not pin the whole session.
            if let Some(mcp) = &mcp {
                let _ = cancel.race(mcp.ready()).await;
            }

            let event_tx = base_tx.with_run_id(run_id);
            let error_tx = event_tx.clone();

            if let Some(mut new_model) = model_rx
                .try_iter()
                .last()
                .filter(|candidate| params.model_policy.allows(&candidate.spec()))
                && new_model.spec() != model.spec()
            {
                let Some(switched) = connect(&mut new_model, params.timeouts, &error_tx).await
                else {
                    if let Some((hub, ..)) = &waker {
                        hub.main_turn_ends();
                    }
                    run_id += 1;
                    continue;
                };
                provider = switched;
                model = new_model;
            }

            while answer_rx.lock().await.try_recv().is_ok() {}

            // Outlives `agent`, so this turn is on disk before the next prompt
            // is read, without the loop body remembering to write it.
            let mut turn = track.turn(model.spec());
            let mut agent = Agent::new(
                AgentParams {
                    provider: Arc::clone(&provider),
                    model: model.clone(),
                    config: params.config.clone(),
                    tool_output_lines: ToolOutputLines::default(),
                    permissions: Arc::clone(&task_permissions),
                    session_id: Some(session_ref_clone.clone()),
                    background_for: None,
                    task_id: None,
                    mailbox: Some(mailbox.clone()),
                    timeouts: params.timeouts,
                    file_access: Arc::clone(&file_access),
                    prompt_slots: Arc::clone(&params.prompt_slots),
                    subagent_cancels: Arc::new(CancelMap::new()),
                    ledger: Arc::new(RunLedger::default()),
                    registry: Arc::clone(ToolRegistry::global_arc()),
                    audience: ToolAudience::MAIN,
                    model_policy: Arc::clone(&params.model_policy),
                    runs: runs.clone(),
                },
                turn.run_params(event_tx, Arc::clone(&context)),
            )
            .with_loaded_instructions(instructions.loaded.clone())
            .with_user_response_rx(Arc::clone(&answer_rx))
            .with_cancel(cancel)
            .with_local_tools(Arc::clone(&params.local_tools))
            .with_mcp(mcp.clone());

            let result = agent.run(input).await;
            drop(agent);
            cancel_task.cancel().await;

            if let Err(ref e) = result {
                error!(error = %e, "agent error");
                let _ = error_tx.send(AgentEvent::error(e));
            }

            if let Some((hub, ..)) = &waker {
                hub.main_turn_ends();
            }
            run_id += 1;
        }
        if let Some((hub, _, driver)) = waker {
            driver.cancel().await;
            hub.shutdown();
        }
    }));

    (
        InteractiveHandle {
            tool_names,
            input_tx,
            answer_tx,
            cancel_tx,
            model_tx,
            session_id: session_ref,
            permissions,
            task,
        },
        events,
    )
}

/// Waits for a session task that has nothing left to do but tear MCP down,
/// dropping it if that wedges. Safe to bound: the event stream already ended
/// (see [`run_session`]), and dropping the task cannot resurrect it.
pub async fn await_shutdown(task: smol::Task<()>) {
    futures_lite::future::or(task, async {
        smol::Timer::after(SESSION_SHUTDOWN_TIMEOUT).await;
    })
    .await;
}

/// Runs a session body, ends its event stream, then tears MCP down. The stream
/// ends with the run and not with teardown: a wedged shutdown must not keep a
/// consumer waiting for events that can no longer come.
async fn run_session(
    guard: EventStreamGuard,
    mcp_handle: Option<McpHandle>,
    body: impl Future<Output = ()>,
) {
    body.await;
    drop(guard);
    if let Some(handle) = mcp_handle {
        handle.shutdown().await;
    }
}

fn extract_tool_names(tools: &Value) -> Vec<String> {
    tools
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures_lite::future::poll_once;
    use test_case::test_case;

    use super::*;
    use crate::mcp::McpCommand;
    use crate::prompt::{PromptId, Slot, SlotEntry};

    const RUN_ID: u64 = 7;
    const STREAM_ENDED: &str = "the stream must end with the run, not with teardown";
    const SHUTDOWN_WEDGED: &str = "the MCP shutdown must still be waiting for its ack";
    const PROVIDER_ERROR: &str = "provider error";
    const BODY_EVENT: &str = "the body's event must be delivered before the close";
    const STILL_WORKING: &str = "await_shutdown must not return while the task still works";
    const TASK_DROPPED: &str = "await_shutdown dropped a task that had work left";

    /// A host-written prompt never had maki's facts in it. If the frame
    /// claimed it did, a later edit would be announced to a model that never
    /// saw the original.
    #[test_case(None, true ; "rendered_prompt_states_them")]
    #[test_case(Some("custom"), false ; "override_leaves_them_out")]
    fn override_states_no_facts(system_override: Option<&str>, tracked: bool) {
        let mut slots = ResolvedPrompts::default();
        slots.insert(
            PromptId::System,
            Slot::AfterInstructions,
            SlotEntry {
                plugin: Arc::from("memory"),
                content: "tags: a".into(),
            },
        );
        let instructions = agent::Instructions {
            text: "\n\nProject instructions:\nbe nice".into(),
            ..Default::default()
        };
        let spec = ContextSpec {
            system_override: system_override.map(String::from),
            ..ContextSpec::new(
                template::Vars::new(),
                &instructions,
                Arc::new(slots),
                AgentConfig::default(),
                Vec::new(),
                None,
            )
        };
        let model = Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap();
        let facts = spec.render(&model, false).facts;
        assert_eq!(
            facts.is_some_and(|f| !f.instructions.is_empty() && !f.hints.is_empty()),
            tracked
        );
    }

    #[test_case(None, "You analyse information." ; "plugin_template")]
    #[test_case(Some("Explicit SDK policy."), "Explicit SDK policy." ; "sdk_precedence")]
    fn headless_prompt_replacement_keeps_explicit_sdk_precedence(
        explicit: Option<&str>,
        expected: &str,
    ) {
        const PLUGIN_SYSTEM: &str = "You analyse information.";
        const OMITTED_INSTRUCTIONS: &str = "Project instructions must not be appended.";
        let mut prompts = ResolvedPrompts::default();
        prompts.set_template(PromptId::System, PLUGIN_SYSTEM.into());
        let spec = ContextSpec {
            system_override: explicit.map(String::from),
            ..ContextSpec::new(
                template::Vars::new(),
                &agent::Instructions {
                    text: OMITTED_INSTRUCTIONS.into(),
                    ..Default::default()
                },
                Arc::new(prompts),
                AgentConfig::default(),
                Vec::new(),
                None,
            )
        };
        let model = Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap();
        let context = spec.render(&model, false);
        assert_eq!(context.system, expected);
        assert_eq!(context.facts.is_none(), explicit.is_some());
        assert!(!context.system.contains(OMITTED_INSTRUCTIONS));
    }

    #[test]
    fn extract_tool_names_filters_valid_entries() {
        let tools = serde_json::json!([{"name": "read"}, {"type": "function"}, {"name": "bash"}]);
        assert_eq!(extract_tool_names(&tools), vec!["read", "bash"]);
    }

    #[test]
    fn advertised_names_show_tool_search_not_deferred_tools() {
        let base = serde_json::json!([{"name": "read"}]);
        let mcp =
            crate::mcp::test_support::stub_session(&[("srv.fetch_issue", "Fetch a GitHub issue")]);
        let names = advertised_tool_names(&base, Some(&mcp));
        assert_eq!(
            names,
            vec!["read", crate::mcp::TOOL_SEARCH_TOOL_NAME],
            "clients must see the search tool, not deferred definitions"
        );
        assert_eq!(
            base,
            serde_json::json!([{"name": "read"}]),
            "probing must not bake MCP entries into the base tools"
        );
        assert_eq!(advertised_tool_names(&base, None), vec!["read"]);
    }

    /// The ordering the SDK exit rests on: the stream ends when the run ends,
    /// not when MCP teardown does. The handle here takes the `Shutdown` and
    /// never acks it, so the session future is still parked on its own timeout
    /// while the consumer already has the body's error and the end of the
    /// stream. The retained sender is the Lua tool context an idle VM never
    /// collects.
    #[test]
    fn stream_ends_with_the_run_not_with_mcp_teardown() {
        let (guard, mut events) = event_stream();
        let retained = guard.sender(RUN_ID);
        let event_tx = retained.clone();
        let (cmd_tx, cmd_rx) = flume::unbounded();
        smol::block_on(async {
            let mut session = pin!(run_session(
                guard,
                Some(McpHandle::for_test(cmd_tx)),
                async move {
                    let _ = event_tx.send(AgentEvent::Error {
                        message: PROVIDER_ERROR.into(),
                        auth: false,
                    });
                }
            ));
            assert!(
                poll_once(session.as_mut()).await.is_none(),
                "{SHUTDOWN_WEDGED}"
            );
            assert!(
                matches!(cmd_rx.try_recv(), Ok(McpCommand::Shutdown { .. })),
                "{SHUTDOWN_WEDGED}"
            );
            let envelope = events.next().await.expect(BODY_EVENT);
            assert!(matches!(
                envelope.event,
                AgentEvent::Error { message, .. } if message == PROVIDER_ERROR
            ));
            assert!(
                matches!(poll_once(events.next()).await, Some(None)),
                "{STREAM_ENDED}"
            );
        });
        assert!(retained.send(AgentEvent::Nudge).is_ok());
    }

    /// `await_shutdown` bounds teardown, not the session: a task with work left
    /// runs to completion. Dropping it here would cancel prompts stdin already
    /// queued.
    #[test]
    fn await_shutdown_waits_for_a_task_that_still_works() {
        let (release_tx, release_rx) = flume::bounded::<()>(1);
        let finished = Arc::new(AtomicBool::new(false));
        let task = smol::spawn({
            let finished = Arc::clone(&finished);
            async move {
                let _ = release_rx.recv_async().await;
                finished.store(true, Ordering::SeqCst);
            }
        });

        smol::block_on(async {
            let mut shutdown = pin!(await_shutdown(task));
            assert!(
                poll_once(shutdown.as_mut()).await.is_none(),
                "{STILL_WORKING}"
            );
            release_tx.send(()).unwrap();
            shutdown.await;
        });
        assert!(finished.load(Ordering::SeqCst), "{TASK_DROPPED}");
    }
}
