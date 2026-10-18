//! Native agent runs. Each background subagent owns a driver task that holds
//! its history for the life of the run, so it outlives the turn that spawned
//! it: the cancel token is a child of the hub's session-lifetime root, not of
//! anything the parent's turn cancels.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use futures_lite::future::{or, pending};
use maki_config::ModelPolicy;
use maki_providers::model::ModelTier;
use maki_providers::provider::{self, Provider};
use maki_providers::{
    AgentError, ContentBlock, ContextGauge, EMPTY_RESPONSE_MARKER, Message, Model, RequestOptions,
    Role, ThinkingConfig,
};
use maki_storage::sessions::StoredThinking;
use serde_json::{Value, json};

use super::inbox::{self, Inbox, InboxItem, Phase};
use super::notice::BODY_FILE_SUFFIX;
use super::{
    AgentSpec, BodyRef, Clock, DoneReason as UnitDone, Terminal, UnitHub, UnitId, UnitResult,
};
use crate::agent::{LoadedInstructions, UNAVAILABLE_RESULT, load_instruction_text};
use crate::cancel::{CancelMap, CancelToken, CancelTrigger};
use crate::mcp::McpSession;
use crate::prompt::PromptId;
use crate::tools::schema::sanitize_tool_input_schema;
use crate::tools::{
    DescriptionContext, LocalTool, LocalTools, RequestTools, ToolAudience, ToolContext, ToolFilter,
    capability_exclusions, local_tool,
};
use crate::{
    Agent, AgentEvent, AgentInput, AgentMode, AgentParams, AgentRunParams, DoneReason, Envelope,
    EventSender, History, InputSource, RunContext, RunContextBuilder, RunLedger, SubagentInfo,
    template,
};

/// Bounded result text: the notice keeps the head and the tail of anything
/// longer and the whole text goes to disk.
pub(crate) const RESULT_LIMIT: usize = 4096;
const RESULT_HEAD: usize = 3072;
const RESULT_TAIL: usize = 768;
/// A run that ignores its cancel this long is force-settled.
pub(super) const FORCE_SETTLE_AFTER: Duration = Duration::from_secs(5);
const MAX_NUDGES: u32 = 2;
const MAX_SCHEMA_ERRORS: usize = 3;
const DETAIL_LIMIT: usize = 200;
const STRUCTURED_OUTPUT_NAME: &str = "structured_output";
const STRUCTURED_OUTPUT_DESCRIPTION: &str =
    "Report your final result. Call it exactly once when your task is complete.";
const STRUCTURED_OUTPUT_ACK: &str = "Output recorded.";
const STRUCTURED_OUTPUT_PROMPT_SUFFIX: &str =
    "\n\nWhen finished, call the structured_output tool with your final result.";
const NUDGE_MISSING: &str = "You did not call the structured_output tool. Call it now with your final result matching its input schema.";
const NUDGE_SUMMARY: &str = "You finished your work but did not provide a summary. Reply with a concise summary of what you did and found.";
const INVALID_INPUT_PREFIX: &str =
    "Input does not match the required schema. Fix the errors and call structured_output again:\n";
const MISSING_STRUCTURED: &str = "finished without calling structured_output";
const INVALID_STRUCTURED: &str = "result does not match output_schema";
const MISSING_SUMMARY: &str = "finished without providing a summary";
const DROPPED: &str = "the prompt was dropped by a plugin";
const SCHEMA_ROOT_ERROR: &str = "output_schema must have type object";
const CANCEL_NOTE_MARKER: &str = "[Response cut off by user cancel]";

/// The model choice that `maki.agent.resolve_model` and `spawn` share: no tier
/// keeps the parent's model, otherwise the requested tier clamped to the
/// parent's own.
pub fn resolve_model(
    parent: &Model,
    policy: &ModelPolicy,
    tier: Option<&str>,
) -> Result<Model, String> {
    let Some(tier_str) = tier else {
        return Ok(parent.clone());
    };
    let requested = tier_str.parse::<ModelTier>().map_err(|e| e.to_string())?;
    let effective = requested.capped_at(parent.tier);
    if effective == parent.tier {
        return Ok(parent.clone());
    }
    let slug = &parent.provider;
    maki_providers::model_registry::spec_for_tier(slug, effective)
        .or_else(|| maki_providers::model_registry::spec_for_tier_any(effective))
        .filter(|spec| policy.allows(spec))
        .and_then(|s| Model::from_spec(&s).ok())
        .map(Ok)
        .unwrap_or_else(|| {
            Model::from_tier_with_policy(slug, effective, policy).map_err(|e| e.to_string())
        })
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a `structured_output` call captured, shared with the tool closure.
#[derive(Default)]
struct Captured {
    value: Mutex<Option<Value>>,
    last_errors: Mutex<Option<String>>,
}

/// Everything an agent run needs that is decided on the caller's side of
/// `spawn`: model, provider, prompt, tool list and the pieces of the session
/// it shares.
pub struct AgentLaunch {
    params: AgentParams,
    context: RunContextBuilder,
    opts: RequestOptions,
    mcp: Option<McpSession>,
    reauth: bool,
    local_tools: LocalTools,
    structured: Option<Arc<Captured>>,
    mode: AgentMode,
    pub(super) model_spec: String,
    pub(super) fork: Option<Arc<Vec<Message>>>,
}

impl AgentLaunch {
    pub(super) fn request_options(&self) -> RequestOptions {
        self.opts
    }

    /// Resolves model and provider, renders the prompt and builds the tool
    /// list for an agent of `spec`'s kind, from the caller's context. Fails
    /// before anything starts, so a bad schema or tier costs no tokens.
    pub async fn prepare(ctx: &ToolContext, spec: &AgentSpec) -> Result<Self, String> {
        let (audience, prompt_id) = match spec.subagent_type.as_str() {
            "research" => (ToolAudience::RESEARCH_SUB, PromptId::Research),
            "general" => (ToolAudience::GENERAL_SUB, PromptId::General),
            other => return Err(format!("unknown subagent type: {other}")),
        };
        let structured = match &spec.output_schema {
            Some(schema) => Some(check_schema(schema)?),
            None => None,
        };

        let mut model = resolve_model(&ctx.model, &ctx.model_policy, spec.model_tier.as_deref())?;
        let provider: Arc<dyn Provider> = if model.spec() == ctx.model.spec() {
            Arc::clone(&ctx.provider)
        } else {
            Arc::from(
                provider::from_model_async(&mut model, ctx.timeouts)
                    .await
                    .map_err(|e| e.to_string())?,
            )
        };

        let requested_thinking = match &spec.thinking {
            None => None,
            Some(setting) => match StoredThinking::parse_setting(setting) {
                Ok(stored) => Some(ThinkingConfig::from(stored)),
                Err(e) => return Err(format!("invalid thinking: {e}")),
            },
        };
        let opts = RequestOptions {
            thinking: requested_thinking
                .map_or(ctx.opts.thinking, |t| t.clamp_to(ctx.opts.thinking)),
            fast: ctx.opts.fast,
        }
        .clamped(&model);

        let vars = template::env_vars();
        let cwd = vars.apply("{cwd}").into_owned();
        let instructions = smol::unblock(move || load_instruction_text(&cwd)).await;
        let system = ctx.prompt_slots.render(prompt_id, &vars, &instructions);

        let disabled: Vec<&str> = ctx
            .config
            .disabled_tools
            .iter()
            .map(String::as_str)
            .collect();
        let filter = ToolFilter::All
            .excluding(&disabled)
            .excluding(capability_exclusions(&model));
        let description = DescriptionContext {
            filter: &filter,
            audience,
            workflow: false,
            mcp: ctx.mcp.is_some(),
        };
        let mut defs =
            ctx.registry
                .definitions(&vars, &description, model.supports_tool_examples());
        let mut local: HashMap<String, LocalTool> = HashMap::new();
        if let (Some(captured), Some(schema)) = (&structured, &spec.output_schema) {
            let schema = with_object_root(schema.clone());
            if let Some(list) = defs.as_array_mut() {
                list.push(json!({
                    "name": STRUCTURED_OUTPUT_NAME,
                    "description": STRUCTURED_OUTPUT_DESCRIPTION,
                    "input_schema": sanitize_tool_input_schema(schema.clone()),
                }));
            }
            let captured = Arc::clone(captured);
            let validator = Arc::new(
                jsonschema::validator_for(&schema)
                    .map_err(|e| format!("invalid output_schema: {e}"))?,
            );
            local.insert(
                STRUCTURED_OUTPUT_NAME.to_owned(),
                local_tool(audience, move |input, _| {
                    let errors = schema_errors(&validator, &input);
                    let result = if errors.is_empty() {
                        *lock(&captured.value) = Some(input);
                        Ok(STRUCTURED_OUTPUT_ACK.to_owned())
                    } else {
                        let bounded = errors
                            .into_iter()
                            .take(MAX_SCHEMA_ERRORS)
                            .collect::<Vec<_>>()
                            .join("\n");
                        *lock(&captured.last_errors) = Some(bounded.clone());
                        Err(format!("{INVALID_INPUT_PREFIX}{bounded}"))
                    };
                    Box::pin(async move { result })
                }),
            );
        }
        let tools = RequestTools::assembled(defs, &ctx.config, &model);
        let context: RunContextBuilder =
            Arc::new(move |_, _| RunContext::fixed(system.clone(), tools.clone()));

        let model_spec = model.spec();
        let params = AgentParams {
            provider,
            model,
            config: ctx.config.clone(),
            tool_output_lines: ctx.tool_output_lines,
            permissions: Arc::clone(&ctx.permissions),
            session_id: ctx.session_id.clone(),
            background_for: ctx.background_for,
            task_id: None,
            mailbox: None,
            timeouts: ctx.timeouts,
            file_access: Arc::clone(&ctx.file_access),
            prompt_slots: Arc::clone(&ctx.prompt_slots),
            subagent_cancels: Arc::new(CancelMap::new()),
            ledger: RunLedger::child(&ctx.ledger),
            registry: Arc::clone(&ctx.registry),
            audience,
            model_policy: Arc::clone(&ctx.model_policy),
            runs: None,
        };
        Ok(Self {
            params,
            context,
            opts,
            mcp: ctx.mcp.as_ref().map(McpSession::fresh),
            reauth: ctx.reauth,
            local_tools: Arc::new(local),
            structured,
            mode: AgentMode::Build,
            model_spec,
            fork: None,
        })
    }
}

fn with_object_root(mut schema: Value) -> Value {
    if let Some(obj) = schema.as_object_mut() {
        obj.entry("type").or_insert_with(|| json!("object"));
    }
    schema
}

fn schema_errors(validator: &jsonschema::Validator, value: &Value) -> Vec<String> {
    validator
        .iter_errors(value)
        .map(|e| {
            let path = e.instance_path().to_string();
            if path.is_empty() {
                e.to_string()
            } else {
                format!("at {path}: {e}")
            }
        })
        .collect()
}

/// Compiles early: a schema that cannot be used is refused at `spawn`.
fn check_schema(schema: &Value) -> Result<Arc<Captured>, String> {
    if schema
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("object")
        != "object"
    {
        return Err(SCHEMA_ROOT_ERROR.to_owned());
    }
    jsonschema::validator_for(&with_object_root(schema.clone()))
        .map_err(|e| format!("invalid output_schema: {e}"))?;
    Ok(Arc::default())
}

/// The control half of a run, shared between the hub (which stops it) and
/// its driver (which obeys).
pub(super) struct AgentRun {
    token: CancelToken,
    cancel: Mutex<Option<CancelTrigger>>,
    /// The current turn's own token, so a soft stop ends the turn only.
    turn: Mutex<Option<CancelTrigger>>,
    soft: AtomicBool,
    timed_out: AtomicBool,
    pub(super) info: SubagentInfo,
    /// What other runs and the user send to this agent.
    pub(super) inbox: Arc<Inbox>,
    /// Test hooks: a named point of the driver parks until released.
    #[cfg(test)]
    pub(super) pauses: Mutex<std::collections::HashMap<&'static str, PauseHook>>,
    task: Mutex<Option<smol::Task<()>>>,
}

/// Reached signal out, release signal in.
#[cfg(test)]
pub(super) type PauseHook = (flume::Sender<()>, flume::Receiver<()>);

impl AgentRun {
    pub(super) fn new(root: &CancelToken, info: SubagentInfo) -> Arc<Self> {
        let (trigger, token) = root.child();
        Arc::new(Self {
            token,
            cancel: Mutex::new(Some(trigger)),
            turn: Mutex::new(None),
            soft: AtomicBool::new(false),
            timed_out: AtomicBool::new(false),
            info,
            inbox: Arc::new(Inbox::new(Phase::Running)),
            #[cfg(test)]
            pauses: Mutex::default(),
            task: Mutex::new(None),
        })
    }

    /// Hard stop: the run's token, which every turn token descends from.
    pub(super) fn cancel_run(&self) -> Vec<InboxItem> {
        let left = self.inbox.close();
        let trigger = lock(&self.cancel).take();
        if let Some(trigger) = trigger {
            trigger.cancel();
        }
        left
    }

    /// Soft stop: abandon the current turn and stay available.
    pub(super) fn interrupt_turn(&self) {
        self.soft.store(true, Ordering::Release);
        let trigger = lock(&self.turn).take();
        if let Some(trigger) = trigger {
            trigger.cancel();
        }
    }

    /// Drops the driver task, which cancels it at its next await.
    pub(super) fn abort(&self) {
        let task = lock(&self.task).take();
        drop(task);
    }

    fn begin_turn(&self) -> CancelToken {
        let (trigger, token) = self.token.child();
        *lock(&self.turn) = Some(trigger);
        self.soft.store(false, Ordering::Release);
        token
    }

    fn end_turn(&self) {
        let _ = lock(&self.turn).take();
    }
}

/// Starts the driver; the run keeps the task so the hub can drop it.
pub(super) fn start(
    hub: Weak<UnitHub>,
    id: UnitId,
    run: Arc<AgentRun>,
    spec: AgentSpec,
    mut launch: AgentLaunch,
    clock: Arc<dyn Clock>,
) {
    let handle = run.info.parent_tool_use_id.clone();
    launch.params.task_id = Some(Arc::from(handle.as_str()));
    launch.params.runs = hub
        .upgrade()
        .map(|hub| super::RunsCtx { hub, self_run: id });
    let driver = Driver {
        hub,
        id,
        run: Arc::clone(&run),
        clock,
        started: Instant::now(),
        turns: AtomicU32::new(0),
        staged: Mutex::default(),
    };
    let task = smol::spawn(driver.drive(spec, launch));
    *lock(&run.task) = Some(task);
}

struct Driver {
    hub: Weak<UnitHub>,
    id: UnitId,
    run: Arc<AgentRun>,
    clock: Arc<dyn Clock>,
    started: Instant,
    turns: AtomicU32,
    /// Taken from the inbox to start or restart a turn, not yet in history.
    staged: Mutex<Vec<InboxItem>>,
}

/// How one `Agent::run` ended, with what the model said.
struct TurnEnd {
    result: Result<DoneReason, AgentError>,
    text: String,
}

enum Settled {
    Restart(String),
    Idle,
    Stopped,
}

enum Verdict {
    Idle { interrupted: bool },
    Done(UnitDone, Option<String>),
}

enum Judged {
    Final(Verdict),
    Nudge(String),
}

impl Driver {
    /// One-shot test hook; a no-op outside tests.
    #[cfg(test)]
    async fn pause(&self, at: &'static str) {
        let hook = lock(&self.run.pauses).remove(at);
        if let Some((reached, go)) = hook {
            let _ = reached.send(());
            let _ = go.recv_async().await;
        }
    }

    #[cfg(not(test))]
    async fn pause(&self, _at: &'static str) {}

    fn hub(&self) -> Option<Arc<UnitHub>> {
        self.hub.upgrade()
    }

    /// Holds an agent slot for the turn about to start.
    async fn slot(&self) -> bool {
        match self.hub() {
            Some(hub) => hub.acquire(self.id, &self.run.token, false).await,
            None => false,
        }
    }

    async fn drive(self, spec: AgentSpec, launch: AgentLaunch) {
        let mut history = History::new(
            launch
                .fork
                .as_ref()
                .map_or_else(Vec::new, |snapshot| snapshot.as_ref().clone()),
        );
        self.drive_loop(spec, launch, &mut history).await;
        // Taken into a transcript that no request will ever carry now.
        let mut left: Vec<InboxItem> = std::mem::take(&mut *lock(&self.staged));
        left.extend(history.take_landed().into_iter().map(|(item, _)| item));
        if let Some(hub) = self.hub() {
            hub.messages_dropped(&left, "recipient_stopped");
        }
    }

    async fn drive_loop(&self, spec: AgentSpec, launch: AgentLaunch, history: &mut History) {
        // The clock starts when the run gets to work, not while it queues.
        if !self.slot().await {
            self.finish(UnitDone::Killed, None, UnitResult::default())
                .await;
            return;
        }
        let deadline = self.clock.now() + Duration::from_secs(spec.timeout_secs);
        let mut gauge = ContextGauge::default();
        let (tx, rx) = flume::unbounded::<Envelope>();
        let events = EventSender::new(tx, 0);

        let mut message = spec.prompt.clone();
        if launch.structured.is_some() {
            message.push_str(STRUCTURED_OUTPUT_PROMPT_SUFFIX);
        }
        let first = self.run.inbox.take_start();
        self.delivered(&first);
        if !first.is_empty() {
            message.push_str("\n\n");
            message.push_str(&inbox::frame(&first, None));
        }
        loop {
            // A no-op while the slot is held; after an idle wake it queues.
            if !self.slot().await {
                self.finish(UnitDone::Killed, None, UnitResult::default())
                    .await;
                return;
            }
            let mut nudges = 0;
            let (verdict, mut result) = loop {
                let end = self
                    .turn(
                        &launch, history, &mut gauge, &events, &rx, message, deadline,
                    )
                    .await;
                let (judged, result) = self.judge(&launch, end);
                match judged {
                    Judged::Nudge(next) if nudges < MAX_NUDGES => {
                        nudges += 1;
                        message = next;
                    }
                    Judged::Nudge(_) => {
                        let detail = match &launch.structured {
                            Some(c) => match lock(&c.last_errors).clone() {
                                Some(errors) => format!("{INVALID_STRUCTURED}:\n{errors}"),
                                None => MISSING_STRUCTURED.to_owned(),
                            },
                            None => MISSING_SUMMARY.to_owned(),
                        };
                        break (Verdict::Done(UnitDone::Failed, Some(detail)), result);
                    }
                    Judged::Final(v) => break (v, result),
                }
            };

            // Before the state change, so whoever sees the state also finds the
            // transcript.
            self.send_history(history);
            let ledger = launch.params.ledger.totals();
            result.turns = self.turns.load(Ordering::Relaxed);
            result.elapsed_ms = elapsed_ms(self.started);
            result.usage = ledger.usage;
            result.cost = ledger.cost.unwrap_or(0.0);
            self.spill(&mut result).await;

            self.pause("before_settle").await;
            match verdict {
                Verdict::Idle { interrupted } if !spec.one_shot => {
                    match self.settle(result, !interrupted) {
                        Settled::Restart(text) => message = text,
                        Settled::Idle => match self.idle_until_woken().await {
                            Some(text) => message = text,
                            None => {
                                self.finish(UnitDone::Killed, None, UnitResult::default())
                                    .await;
                                return;
                            }
                        },
                        Settled::Stopped => {
                            self.finish(UnitDone::Killed, None, UnitResult::default())
                                .await;
                            return;
                        }
                    }
                }
                Verdict::Idle { .. } => {
                    self.finish(UnitDone::Completed, None, result).await;
                    return;
                }
                Verdict::Done(reason, detail) => {
                    self.finish(reason, detail, result).await;
                    return;
                }
            }
        }
    }

    /// The turn is over. Under the inbox lock: anything that arrived while
    /// the run was returning starts the next turn, otherwise the run goes
    /// `Idle`, so a send either lands before this check or sees `Idle` and
    /// wakes the run. Nothing is lost between the two.
    fn settle(&self, result: UnitResult, notify: bool) -> Settled {
        let mut guard = self.run.inbox.lock();
        if self.run.token.is_cancelled() {
            return Settled::Stopped;
        }
        if guard.has_wakeable() {
            let items = guard.take_start();
            drop(guard);
            self.delivered(&items);
            return Settled::Restart(inbox::frame(&items, None));
        }
        guard.set_phase(Phase::Idle);
        let idle = self
            .hub()
            .is_some_and(|hub| hub.settle_idle(self.id, result, notify));
        if idle {
            Settled::Idle
        } else {
            let left = guard.close();
            drop(guard);
            if let Some(hub) = self.hub() {
                hub.messages_dropped(&left, "recipient_stopped");
            }
            Settled::Stopped
        }
    }

    /// Items that become the next turn's user message. They are delivered
    /// once that turn's first request returned (the step is unknown), so they
    /// wait in `staged` and move into the history's landed list.
    fn delivered(&self, items: &[InboxItem]) {
        lock(&self.staged).extend(items.iter().cloned());
    }

    async fn idle_until_woken(&self) -> Option<String> {
        self.pause("after_idle").await;
        self.wait_for_wake().await
    }

    /// Parks an `Idle` agent until something wakes it (the text to start the
    /// next turn with) or its stop arrives (`None`).
    async fn wait_for_wake(&self) -> Option<String> {
        enum Woke {
            Message,
            Cancel,
        }
        loop {
            let woke = or(
                async {
                    self.run.inbox.woken().await;
                    Woke::Message
                },
                async {
                    self.run.token.cancelled().await;
                    Woke::Cancel
                },
            )
            .await;
            if matches!(woke, Woke::Cancel) {
                return None;
            }
            self.pause("woken").await;
            let mut guard = self.run.inbox.lock();
            match guard.phase() {
                Phase::Closed => return None,
                Phase::Running => {
                    let items = guard.take_start();
                    drop(guard);
                    self.delivered(&items);
                    return Some(inbox::frame(&items, None));
                }
                Phase::Idle => {}
            }
        }
    }

    /// Children go first, then the parent settles: a notice never reports a
    /// finished parent with live children.
    async fn finish(&self, reason: UnitDone, detail: Option<String>, result: UnitResult) {
        let left = self.run.inbox.close();
        let Some(hub) = self.hub() else { return };
        hub.messages_dropped(&left, "recipient_stopped");
        hub.stop_children(self.id);
        hub.children_settled(self.id).await;
        let terminal = Terminal {
            reason,
            exit_code: None,
            detail,
            result,
        };
        hub.settle(self.id, terminal, 0);
    }

    #[allow(clippy::too_many_arguments)]
    async fn turn(
        &self,
        launch: &AgentLaunch,
        history: &mut History,
        gauge: &mut ContextGauge,
        events: &EventSender,
        rx: &flume::Receiver<Envelope>,
        message: String,
        deadline: Instant,
    ) -> TurnEnd {
        history.close_dangling_tool_calls(UNAVAILABLE_RESULT);
        history.note_landed(&std::mem::take(&mut *lock(&self.staged)), None);
        let before = history.len();
        let token = self.run.begin_turn();
        let input = AgentInput {
            message,
            mode: launch.mode.clone(),
            images: Vec::new(),
            preamble: Vec::new(),
            earlier: Vec::new(),
            thinking: launch.opts.thinking,
            fast: launch.opts.fast,
            workflow: false,
            prompt: None,
            source: InputSource::Plugin,
        };
        let result = {
            let mut agent = Agent::new(
                launch.params.clone(),
                AgentRunParams {
                    history: &mut *history,
                    gauge,
                    event_tx: events.clone(),
                    context: Arc::clone(&launch.context),
                },
            )
            .with_reauth(launch.reauth)
            .with_loaded_instructions(LoadedInstructions::new())
            .with_cancel(token)
            .with_mcp(launch.mcp.clone())
            .with_local_tools(Arc::clone(&launch.local_tools));
            let run = agent.run(input);
            let forward = async {
                loop {
                    match rx.recv_async().await {
                        Ok(envelope) => self.forward(envelope),
                        Err(_) => pending::<()>().await,
                    }
                }
            };
            let timeout = async {
                self.clock.sleep_until(deadline).await;
                self.run.timed_out.store(true, Ordering::Release);
                self.run.interrupt_turn();
                pending::<()>().await
            };
            let side = async {
                or(forward, timeout).await;
                unreachable!("the side tasks never end")
            };
            or(run, side).await
        };
        self.run.end_turn();
        while let Ok(envelope) = rx.try_recv() {
            self.forward(envelope);
        }
        if let Err(e) = &result
            && !matches!(e, AgentError::Cancelled)
        {
            self.emit(AgentEvent::error(e));
        }
        let text = reply_since(history.as_slice(), before);
        TurnEnd { result, text }
    }

    fn judge(&self, launch: &AgentLaunch, end: TurnEnd) -> (Judged, UnitResult) {
        let mut result = UnitResult::default();
        let verdict = match end.result {
            Err(AgentError::Cancelled) | Ok(DoneReason::Cancelled) => {
                result.summary = end.text;
                if self.run.token.is_cancelled() {
                    Judged::Final(Verdict::Done(UnitDone::Killed, None))
                } else if self.run.timed_out.load(Ordering::Acquire) {
                    Judged::Final(Verdict::Done(UnitDone::TimedOut, None))
                } else if self.run.soft.load(Ordering::Acquire) {
                    Judged::Final(Verdict::Idle { interrupted: true })
                } else {
                    Judged::Final(Verdict::Done(UnitDone::Killed, None))
                }
            }
            Err(e) => {
                result.summary = end.text;
                Judged::Final(Verdict::Done(
                    UnitDone::Failed,
                    Some(clip(&e.user_message(), DETAIL_LIMIT)),
                ))
            }
            Ok(DoneReason::Dropped) => {
                Judged::Final(Verdict::Done(UnitDone::Failed, Some(DROPPED.to_owned())))
            }
            Ok(DoneReason::MaxTurns) => {
                result.summary = end.text;
                Judged::Final(Verdict::Done(UnitDone::MaxTurns, None))
            }
            Ok(DoneReason::MaxTokens) => {
                result.summary = end.text;
                Judged::Final(Verdict::Done(UnitDone::MaxTokens, None))
            }
            Ok(DoneReason::EndTurn | DoneReason::Compact) => match &launch.structured {
                Some(captured) => match lock(&captured.value).clone() {
                    Some(value) => {
                        result.summary = value.to_string();
                        result.structured = Some(value);
                        Judged::Final(Verdict::Idle { interrupted: false })
                    }
                    None => {
                        result.summary = end.text;
                        Judged::Nudge(NUDGE_MISSING.to_owned())
                    }
                },
                None if end.text.trim().is_empty() => Judged::Nudge(NUDGE_SUMMARY.to_owned()),
                None => {
                    result.summary = end.text;
                    Judged::Final(Verdict::Idle { interrupted: false })
                }
            },
        };
        (verdict, result)
    }

    fn forward(&self, mut envelope: Envelope) {
        let Some(hub) = self.hub() else { return };
        match &envelope.event {
            AgentEvent::TurnComplete(turn) if envelope.subagent.is_none() => {
                self.turns.fetch_add(1, Ordering::Relaxed);
                hub.record_turn(self.id, turn.usage, turn.cost);
            }
            AgentEvent::ToolStart(start) if envelope.subagent.is_none() => {
                hub.set_progress(self.id, Some(format!("{} {}", start.tool, start.summary)));
            }
            AgentEvent::ToolOutput { .. }
            | AgentEvent::ToolPending { .. }
            | AgentEvent::SubagentHistory { .. }
            | AgentEvent::StreamClosed => return,
            _ => {}
        }
        envelope.run_id = crate::BACKGROUND_RUN_ID;
        envelope
            .subagent
            .get_or_insert_with(|| self.run.info.clone());
        let _ = hub.events.send_envelope(envelope);
    }

    fn emit(&self, event: AgentEvent) {
        if let Some(hub) = self.hub() {
            let _ = hub.events.send_envelope(Envelope {
                event,
                subagent: Some(self.run.info.clone()),
                run_id: crate::BACKGROUND_RUN_ID,
            });
        }
    }

    fn send_history(&self, history: &History) {
        self.emit(AgentEvent::SubagentHistory {
            tool_use_id: self.run.info.parent_tool_use_id.clone(),
            messages: history.as_slice().to_vec(),
        });
    }

    /// Writes the whole text to `<handle>.result.txt` and keeps head and tail
    /// when it exceeds the budget.
    async fn spill(&self, result: &mut UnitResult) {
        if result.summary.len() <= RESULT_LIMIT {
            return;
        }
        let handle = self.run.info.parent_tool_use_id.clone();
        let full = std::mem::take(&mut result.summary);
        let dir = self.hub().and_then(|hub| hub.shell.dir.clone());
        result.truncated = true;
        result.summary = bound(&full, &handle);
        if let Some(dir) = dir {
            let path = dir.join(format!("{handle}{BODY_FILE_SUFFIX}"));
            let target = path.clone();
            let written = smol::unblock(move || write_private(&target, full.as_bytes())).await;
            match written {
                Ok(()) => result.body_ref = Some(BodyRef(path)),
                Err(e) => tracing::warn!(%e, "could not spill the run result"),
            }
        }
    }
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn clip(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

fn tail_of(text: &str, limit: usize) -> &str {
    let mut start = text.len().saturating_sub(limit);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

/// First 3072 and last 768 bytes joined by the omission marker.
pub(crate) fn bound(text: &str, handle: &str) -> String {
    if text.len() <= RESULT_LIMIT {
        return text.to_owned();
    }
    let head = clip(text, RESULT_HEAD);
    let tail = tail_of(text, RESULT_TAIL);
    let omitted = text.len() - head.len() - tail.len();
    format!(
        "{head}\n[... {omitted} bytes omitted; full text: runs(id=\"{handle}\", full=true) ...]\n{tail}"
    )
}

fn write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?.write_all(bytes)
}

/// The assistant text this call added: the last assistant message that says
/// something, without the loop's placeholders.
fn reply_since(messages: &[Message], from: usize) -> String {
    messages[from.min(messages.len())..]
        .iter()
        .rev()
        .filter(|m| matches!(m.role, Role::Assistant))
        .map(|m| {
            m.content
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Text { text } if text != EMPTY_RESPONSE_MARKER => {
                        Some(text.as_str())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .find(|text| !text.trim().is_empty())
        .map(|text| text.replace(CANCEL_NOTE_MARKER, "").trim_end().to_owned())
        .unwrap_or_default()
}
