//! Every request in a session starts with the same `system` and `tools`. If
//! one byte of that prefix changes, the cache misses from there on, and on
//! Anthropic the model also loses every thinking block it wrote under it (or
//! the request fails with a 400). So we render the prefix once and only ever
//! add to its end. When the date, the model or plan mode changes later, we
//! tell the model in a context update message instead.
//!
//! A new frame is only built when there is nothing left to lose: a new
//! session, a compaction, or a change no message can describe, like other
//! tools or a prompt the host wrote itself.
//!
//! Each update carries the facts it told, so replaying them over the frame's
//! facts, in transcript order, gives what the model holds true, the way it
//! read them. That stays true after a resume, a rewind or a rebuilt frame,
//! and there is no side state to drift out of sync.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use maki_providers::{Message, Model};
use maki_storage::frame::{
    FactsUpdate, LiveFacts, PlanChange, PromptFacts, StoredFrame, TopologyFacts,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use strum::IntoEnumIterator;

use super::history::History;
use super::instructions::build_system_prompt;
use crate::mcp::{McpSession, ToolDeferral};
use crate::prompt::{PromptId, ResolvedPrompts, Slot, SlotKind};
use crate::template::Vars;
use crate::tools::RequestTools;

const UPDATE_OPEN: &str = "<context-update>\nThis changed since the conversation started and replaces what the system prompt says about it.\n\n";
const UPDATE_CLOSE: &str = "\n</context-update>";
pub(crate) const PLAN_ENDED: &str = "Plan mode ended. You may edit files again.";
const INSTRUCTIONS_GONE: &str = "No project instructions apply anymore.";
const FINGERPRINT_BYTES: usize = 16;
pub(crate) const FRAME_REBUILT: &str =
    "The tools or the system prompt changed, so the prompt was rebuilt and its cache starts over.";

/// What a session would start with if it started now, for one model.
pub struct RunContext {
    pub system: String,
    pub tools: RequestTools,
    /// What `system` states. `None` when the host wrote it: we don't know
    /// which facts it mentions, so we never send updates about any.
    pub facts: Option<PromptFacts>,
    /// Prompt text the host wrote itself. We can't describe a change to it in
    /// an update, so a frame is only reused while it stays the same.
    pub authored: String,
}

impl RunContext {
    pub fn render(
        vars: &Vars,
        instructions: &str,
        slots: &ResolvedPrompts,
        model: &Model,
        tools: RequestTools,
    ) -> Self {
        let system = build_system_prompt(vars, instructions, slots, model);
        Self {
            system,
            tools,
            facts: Some(PromptFacts {
                date: vars.apply("{date}").into_owned(),
                cwd: vars.apply("{cwd}").into_owned(),
                model: model.spec(),
                instructions: if slots.renders_instructions(PromptId::System) {
                    instructions.to_owned()
                } else {
                    String::new()
                },
                hints: hint_facts(slots),
            }),
            authored: String::new(),
        }
        .with_prompt_policy(slots)
    }

    /// For a prompt the host wrote itself.
    pub fn fixed(system: String, tools: RequestTools) -> Self {
        Self {
            authored: system.clone(),
            system,
            tools,
            facts: None,
        }
    }

    pub fn with_prompt_policy(mut self, prompts: &ResolvedPrompts) -> Self {
        let policy = prompts.policy_key();
        if !policy.is_empty() {
            self.authored = json!([self.authored, policy]).to_string();
        }
        self
    }
}

/// Each frontend brings its own, because only it knows how its prompt and
/// tools are put together.
pub type RunContextBuilder = Arc<dyn Fn(&Model, bool) -> RunContext + Send + Sync>;

fn hint_facts(slots: &ResolvedPrompts) -> BTreeMap<String, String> {
    let mut hints: BTreeMap<String, String> = BTreeMap::new();
    for slot in Slot::iter() {
        if slot.kind() == SlotKind::Singleton {
            let content = slots.section(PromptId::System, slot);
            if content != PromptId::System.default_section(slot) {
                hints.insert(format!("prompt/{slot}"), content);
            }
            continue;
        }
        for entry in slots.get(PromptId::System, slot) {
            hints
                .entry(format!("{}/{slot}", entry.plugin))
                .and_modify(|content| {
                    content.push('\n');
                    content.push_str(&entry.content);
                })
                .or_insert_with(|| entry.content.clone());
        }
    }
    hints
}

/// Hashes what no update can describe: the base tools, the MCP deferral mode
/// (it shapes every MCP entry) and the host's own prompt text. The hash is the
/// same in every process, so a resumed session can tell if its frame still fits.
pub(crate) fn fingerprint(context: &RunContext, mcp: Option<&McpSession>, model: &Model) -> String {
    let mut hasher = Sha256::new();
    hasher.update(context.tools.definitions().to_string());
    hasher.update([0]);
    hasher.update(&context.authored);
    if mcp.is_some() {
        hasher.update([0]);
        hasher.update(format!("{:?}", ToolDeferral::for_model(model)));
    }
    hasher.finalize()[..FINGERPRINT_BYTES]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The in-memory half of a frame. The first run of a process rebuilds it from
/// the stored half.
pub(crate) struct LiveFrame {
    /// Kept for its filter, which dispatch checks tool names against.
    pub(crate) tools: RequestTools,
    /// What requests carry: the base tools, then the MCP part. It only grows.
    /// A tool whose server went away stays listed and fails at dispatch.
    pub(crate) wire: Value,
}

impl LiveFrame {
    pub(crate) fn build(tools: RequestTools, mcp: Option<&McpSession>, model: &Model) -> Self {
        let mut wire = tools.definitions().clone();
        if let Some(mcp) = mcp {
            mcp.extend_tools(&mut wire, ToolDeferral::for_model(model));
        }
        Self { tools, wire }
    }

    /// Takes the MCP part from the stored frame, not the live servers. Which
    /// servers are up by now is just timing, and the model already saw the old
    /// list.
    pub(crate) fn restore(tools: RequestTools, stored: &StoredFrame) -> Self {
        let mut wire = tools.definitions().clone();
        if let Some(entries) = wire.as_array_mut() {
            entries.extend(stored.mcp_tools.iter().cloned());
        }
        Self { tools, wire }
    }

    pub(crate) fn mcp_tools(&self) -> &[Value] {
        let base = self.tools.definitions().as_array().map_or(0, Vec::len);
        self.wire.as_array().map_or(&[], |wire| &wire[base..])
    }

    /// Any model that reuses this frame defers the same way, because the
    /// fingerprint covers `deferral`. So taking it from the current model is
    /// safe.
    pub(crate) fn append_late_tools(
        &mut self,
        mcp: &McpSession,
        deferral: ToolDeferral,
    ) -> &[Value] {
        let before = self.wire.as_array().map_or(0, Vec::len);
        mcp.append_late_tools(&mut self.wire, deferral);
        self.wire.as_array().map_or(&[], |wire| &wire[before..])
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FrameFit {
    Kept,
    /// Nothing was lost: there was no frame, or only the facts of an imported
    /// one, which has no prefix and so no cache.
    Built,
    /// An existing frame was replaced, losing the cache built under it.
    Rebuilt,
}

/// The one place a frame is made. Keeps `history`'s frame while `context` still
/// fits it, else builds a new one from `context`.
///
/// A replaced frame takes facts with it that no update ever stated, like the
/// cwd of the machine a session moved from. The transcript is full of them,
/// and the model reads its tail more than the new prompt, so the new frame
/// inherits them and the next update corrects them.
///
/// It inherits what the old frame stated at its start, before any update.
/// [`History::told`] replays every update in the transcript on top anyway,
/// so this tells the same thing today. A later rewind can cut those updates,
/// and then the frame must not claim the model still heard them.
pub(crate) fn fit_frame(
    history: &mut History,
    context: RunContext,
    mcp: Option<&McpSession>,
    model: &Model,
) -> FrameFit {
    let fingerprint = fingerprint(&context, mcp, model);
    match history
        .frame()
        .filter(|frame| frame.prefix().is_some())
        .cloned()
    {
        Some(stored) if stored.fingerprint == fingerprint => {
            if history.live_frame().is_none() {
                history.adopt_frame(LiveFrame::restore(context.tools, &stored));
            }
            FrameFit::Kept
        }
        stored => {
            let inherited = history
                .frame()
                .map(|frame| inherited_facts(&frame.facts_at_start(), context.facts.as_ref()))
                .unwrap_or_default();
            let live = LiveFrame::build(context.tools, mcp, model);
            let mut frame = StoredFrame::new(
                context.system,
                fingerprint,
                context.facts,
                live.mcp_tools().to_vec(),
            );
            frame.inherited = inherited;
            history.set_frame(frame, live);
            if stored.is_some() {
                FrameFit::Rebuilt
            } else {
                FrameFit::Built
            }
        }
    }
}

/// What the old frame stated at its start that `facts` states otherwise. Instructions and hints
/// stay out: the new prompt states them in full, nothing in the transcript
/// points back at them, and they are large. Plan mode is in no prompt, so
/// updates carry it under any frame.
fn inherited_facts(start: &LiveFacts, facts: Option<&PromptFacts>) -> FactsUpdate {
    let (Some(held), Some(stated)) = (&start.prompt, facts) else {
        return FactsUpdate::default();
    };
    let differs = |held: &String, stated: &String| (held != stated).then(|| held.clone());
    FactsUpdate {
        date: differs(&held.date, &stated.date),
        model: differs(&held.model, &stated.model),
        cwd: differs(&held.cwd, &stated.cwd),
        ..FactsUpdate::default()
    }
}

fn wrap_update(parts: &[String]) -> String {
    format!("{UPDATE_OPEN}{}{UPDATE_CLOSE}", parts.join("\n\n"))
}

fn plan_text(plan_path: &Path, prompts: &ResolvedPrompts) -> String {
    let vars = Vars::new().set("{plan_path}", plan_path.display().to_string());
    prompts.render(PromptId::Plan, &vars, "").trim().to_owned()
}

/// The update entering plan mode sends, exactly as the model reads it.
pub fn plan_mode_update(plan_path: &Path, prompts: &ResolvedPrompts) -> String {
    wrap_update(&[plan_text(plan_path, prompts)])
}

const MAIN_NAME: &str = "main";

fn child_list(children: &[(String, String, u8)]) -> String {
    children
        .iter()
        .map(|(handle, name, kind)| {
            let kind = if *kind == 0 { "agent" } else { "shell" };
            format!("\"{name}\" ({handle}, {kind})")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// What the model reads about the runs around it. Plain sentences: it is
/// told again only when the parent or the children change.
pub(crate) fn topology_text(topology: &TopologyFacts) -> String {
    let (handle, name, depth) = &topology.me;
    let (max_depth, siblings) = topology.limits;
    let at_cap = *depth >= max_depth;
    let Some((parent_handle, parent_name)) = &topology.parent else {
        let runs = if topology.children.is_empty() {
            "You have no unfinished runs.".to_owned()
        } else {
            format!("Runs you started: {}.", child_list(&topology.children))
        };
        return format!(
            "Run topology changed. You are \"{MAIN_NAME}\". {runs} Runs finish with a notice; do not poll. Maximum depth {max_depth}."
        );
    };
    let parent = if parent_handle == MAIN_NAME {
        format!("\"{MAIN_NAME}\"")
    } else {
        format!("\"{parent_name}\" ({parent_handle})")
    };
    let children = if topology.children.is_empty() {
        "You have no children.".to_owned()
    } else {
        format!("Your children: {}.", child_list(&topology.children))
    };
    let cap = if at_cap {
        " You are at the maximum depth: spawn(kind=\"agent\") will fail with DEPTH_LIMIT; do the work directly."
    } else {
        ""
    };
    let siblings = if siblings {
        "You may message siblings by name or handle with send; runs(scope=\"all\") lists them."
    } else {
        "Messages to siblings are not enabled."
    };
    format!(
        "Run topology changed. You are agent \"{name}\" ({handle}), depth {depth} of {max_depth}. Your parent is {parent}. {children} Use send(to=\"parent\") for a question or progress while you work; your result is delivered to your parent automatically when you finish, so do not send it. Sending does not end your turn. {siblings}{cap}"
    )
}

/// The update that brings the model from `told` to `now`, if anything
/// changed. It says only what changed, and stores only that.
pub(crate) fn context_update(
    told: &LiveFacts,
    now: &LiveFacts,
    prompts: &ResolvedPrompts,
) -> Option<Message> {
    let update = told.changes_to(now);
    if update.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    if let Some(date) = &update.date {
        parts.push(format!("Date is now {date}."));
    }
    if let Some(model) = &update.model {
        parts.push(format!("You are now running as {model}."));
    }
    if let Some(cwd) = &update.cwd {
        parts.push(format!("Working directory is now {cwd}."));
    }
    if let Some(instructions) = &update.instructions {
        parts.push(if instructions.trim().is_empty() {
            INSTRUCTIONS_GONE.to_owned()
        } else {
            format!("Project instructions now read:{instructions}")
        });
    }
    match &update.plan {
        Some(PlanChange::Entered(path)) => parts.push(plan_text(path, prompts)),
        Some(PlanChange::Ended) => parts.push(PLAN_ENDED.to_owned()),
        None => {}
    }
    if let Some(topology) = &update.topology {
        parts.push(topology_text(topology));
    }
    for (key, content) in &update.hints {
        parts.push(match content {
            Some(content) => format!("Updated `{key}`:\n{}", content.trim()),
            None => format!("`{key}` no longer applies."),
        });
    }
    Some(Message::context_update(wrap_update(&parts), update))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::AgentConfig;
    use crate::mcp::test_support::stub_session;
    use serde_json::json;
    use test_case::test_case;

    const DATE: &str = "2026-10-02";
    const NEXT_DATE: &str = "2026-10-03";
    const HINT_KEY: &str = "memory/after_instructions";
    const NATIVE_MODEL: &str = "anthropic/claude-opus-5";
    const CLIENT_MODEL: &str = "anthropic/claude-sonnet-4-20250514";

    fn facts() -> LiveFacts {
        LiveFacts {
            plan: None,
            prompt: Some(PromptFacts {
                date: DATE.into(),
                cwd: "/repo".into(),
                model: "anthropic/claude-opus-5-5".into(),
                instructions: "\n\nProject instructions:\nbe nice".into(),
                hints: BTreeMap::from([(HINT_KEY.into(), "tags: a, b".into())]),
            }),
            topology: None,
        }
    }

    fn prompt(f: &mut LiveFacts) -> &mut PromptFacts {
        f.prompt.as_mut().unwrap()
    }

    #[test]
    fn unchanged_facts_tell_nothing() {
        assert!(context_update(&facts(), &facts(), &ResolvedPrompts::default()).is_none());
    }

    #[test_case(|f: &mut LiveFacts| prompt(f).date = NEXT_DATE.into(), NEXT_DATE ; "date")]
    #[test_case(|f: &mut LiveFacts| prompt(f).model = "openai/gpt-6".into(), "openai/gpt-6" ; "model")]
    #[test_case(|f: &mut LiveFacts| prompt(f).cwd = "/other".into(), "/other" ; "cwd")]
    #[test_case(|f: &mut LiveFacts| f.plan = Some(PathBuf::from("/plans/p.md")), "/plans/p.md" ; "plan_on")]
    #[test_case(|f: &mut LiveFacts| { prompt(f).hints.insert(HINT_KEY.into(), "tags: c".into()); }, "tags: c" ; "hint_changed")]
    #[test_case(|f: &mut LiveFacts| prompt(f).hints.clear(), HINT_KEY ; "hint_removed")]
    #[test_case(|f: &mut LiveFacts| prompt(f).instructions.clear(), INSTRUCTIONS_GONE ; "instructions_gone")]
    fn changed_fact_is_told(change: fn(&mut LiveFacts), says: &str) {
        let mut now = facts();
        change(&mut now);
        let update = context_update(&facts(), &now, &ResolvedPrompts::default()).unwrap();
        let mut heard = facts();
        heard.apply(update.facts_update().unwrap());
        assert_eq!(heard, now);
        assert!(update.first_text_content().unwrap().contains(says));
    }

    fn model(spec: &str) -> Model {
        Model::from_spec(spec).unwrap()
    }

    fn context(tools: &str, authored: &str) -> RunContext {
        RunContext::fixed(
            authored.into(),
            RequestTools::assembled(
                json!([{ "name": tools }]),
                &AgentConfig::default(),
                &model(NATIVE_MODEL),
            ),
        )
    }

    #[test]
    fn fingerprint_tracks_tools_authored_text_and_deferral() {
        let native = model(NATIVE_MODEL);
        let base = fingerprint(&context("a", ""), None, &native);
        assert_eq!(base, fingerprint(&context("a", ""), None, &native));
        assert_ne!(base, fingerprint(&context("b", ""), None, &native));
        assert_ne!(
            base,
            fingerprint(&context("a", "custom prompt"), None, &native)
        );
        let mcp = stub_session(&[]);
        assert_ne!(
            fingerprint(&context("a", ""), Some(&mcp), &native),
            fingerprint(&context("a", ""), Some(&mcp), &model(CLIENT_MODEL)),
            "a model without tool search shapes the MCP entries another way"
        );
    }

    fn main_topology() -> TopologyFacts {
        TopologyFacts {
            me: ("main".into(), "main".into(), 0),
            parent: None,
            children: vec![
                ("ag-k3x9ab".into(), "auditor".into(), 0),
                ("sh-4m2q7z".into(), "build".into(), 1),
            ],
            limits: (2, false),
        }
    }

    fn sub_topology() -> TopologyFacts {
        TopologyFacts {
            me: ("ag-k3x9ab".into(), "auditor".into(), 1),
            parent: Some(("main".into(), "main".into())),
            children: Vec::new(),
            limits: (2, false),
        }
    }

    #[test]
    fn rendering_snapshot_main_and_sub() {
        assert_eq!(
            topology_text(&main_topology()),
            "Run topology changed. You are \"main\". Runs you started: \"auditor\" (ag-k3x9ab, agent), \"build\" (sh-4m2q7z, shell). Runs finish with a notice; do not poll. Maximum depth 2."
        );
        assert_eq!(
            topology_text(&sub_topology()),
            "Run topology changed. You are agent \"auditor\" (ag-k3x9ab), depth 1 of 2. Your parent is \"main\". You have no children. Use send(to=\"parent\") for a question or progress while you work; your result is delivered to your parent automatically when you finish, so do not send it. Sending does not end your turn. Messages to siblings are not enabled."
        );
        let mut capped = sub_topology();
        capped.me.2 = 2;
        capped.parent = Some(("ag-aaaaaa".into(), "lead".into()));
        capped.limits.1 = true;
        let text = topology_text(&capped);
        assert!(text.contains("Your parent is \"lead\" (ag-aaaaaa)."));
        assert!(text.contains("You may message siblings by name or handle"));
        assert!(text.ends_with("do the work directly."), "{text}");
    }

    #[test]
    fn fact_changes_only_when_children_change() {
        let at = |topology: Option<TopologyFacts>| LiveFacts {
            topology,
            ..facts()
        };
        let told = at(Some(main_topology()));
        assert!(context_update(&told, &told.clone(), &ResolvedPrompts::default()).is_none());
        let mut grown = main_topology();
        grown.children.push(("ag-zzzzzz".into(), "zed".into(), 0));
        let update =
            context_update(&told, &at(Some(grown.clone())), &ResolvedPrompts::default()).unwrap();
        assert!(update.first_text_content().unwrap().contains("\"zed\""));
        let mut heard = told;
        heard.apply(update.facts_update().unwrap());
        assert_eq!(heard.topology, Some(grown));
    }

    #[test]
    fn retell_coalesced_per_drain() {
        let mut now = facts();
        let mut topology = main_topology();
        topology.children.clear();
        let mut told = facts();
        for n in 0..5 {
            topology
                .children
                .push((format!("ag-{n}"), format!("kid{n}"), 0));
        }
        now.topology = Some(topology);
        // Five children appeared since the last drain: one update names all.
        let update = context_update(&told, &now, &ResolvedPrompts::default()).unwrap();
        let text = update.first_text_content().unwrap();
        assert_eq!(text.matches("Run topology changed.").count(), 1);
        assert!((0..5).all(|n| text.contains(&format!("kid{n}"))));
        told.apply(update.facts_update().unwrap());
        assert!(context_update(&told, &now, &ResolvedPrompts::default()).is_none());
    }

    /// The cache key of a session is its system prompt and its tools. Telling
    /// the topology appends a message and touches neither.
    #[test]
    fn topology_update_leaves_the_prefix_and_fingerprint_alone() {
        let native = model(NATIVE_MODEL);
        let mut history = History::new(Vec::new());
        assert_eq!(
            fit_frame(&mut history, context("a", "sys"), None, &native),
            FrameFit::Built
        );
        let (system, tools) = history.request_prefix().unwrap();
        let (system, tools) = (system.to_owned(), tools.to_string());
        let fingerprint_before = history.frame().unwrap().fingerprint.clone();

        let told = history.told().unwrap();
        let now = LiveFacts {
            topology: Some(sub_topology()),
            ..told.clone()
        };
        history.push(context_update(&told, &now, &ResolvedPrompts::default()).unwrap());
        assert_eq!(history.told().unwrap(), now);

        assert_eq!(
            fit_frame(&mut history, context("a", "sys"), None, &native),
            FrameFit::Kept
        );
        let (system_after, tools_after) = history.request_prefix().unwrap();
        assert_eq!(system_after, system);
        assert_eq!(tools_after.to_string(), tools);
        assert_eq!(history.frame().unwrap().fingerprint, fingerprint_before);
    }
}
