//! `maki.run`: the native run hub (`spawn`, `send`, `runs`, `wait`, `stop`)
//! for plugins. Each function takes the handler's ctx, validates its input
//! against the same schema as the model-facing tool of that name and goes
//! through the same hub, so a plugin and a model see one set of runs.

use maki_agent::runs::{UnitState, UnitView};
use maki_agent::tools::runs as native;
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, Table, Value as LuaValue};
use serde_json::{Value as JsonValue, json};

use crate::api::util::convert::{json_to_lua, lua_to_json};
use crate::api::util::ctx::{AgentContext, LuaCtx};
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::plugin_permissions::PluginPermissions;

fn dispatch_ctx<'a>(ctx: &'a LuaCtx, method: &str) -> Result<&'a AgentContext, String> {
    ctx.agent()
        .ok_or_else(|| ctx.cap_err(&format!("maki.run.{method}")))
}

fn input_of(lua: &Lua, opts: Option<LuaValue>) -> LuaResult<JsonValue> {
    match opts {
        Some(v) => lua_to_json(lua, &v),
        None => Ok(json!({})),
    }
}

fn view_to_lua(lua: &Lua, view: &UnitView) -> LuaResult<Table> {
    let t = lua.create_table()?;
    t.set("handle", view.handle.to_string())?;
    t.set("name", view.name.to_string())?;
    t.set("kind", view.kind.as_str())?;
    t.set("state", view.state_label())?;
    t.set("description", view.description.clone())?;
    let settled = matches!(view.state, UnitState::Done(_) | UnitState::Idle);
    t.set("settled", settled)?;
    if let UnitState::Done(term) = &view.state {
        t.set("done", true)?;
        t.set("reason", term.reason.as_str())?;
        if let Some(detail) = &term.detail {
            t.set("detail", detail.clone())?;
        }
        if let Some(code) = term.exit_code {
            t.set("exit_code", code)?;
        }
    } else {
        t.set("done", false)?;
    }
    if let Some(result) = &view.result {
        t.set("summary", result.summary.clone())?;
        t.set("truncated", result.truncated)?;
        if let Some(structured) = &result.structured {
            t.set("structured", json_to_lua(lua, structured)?)?;
        }
        if let Some(body) = &result.body_ref {
            t.set("full_text_path", body.0.display().to_string())?;
        }
    }
    Ok(t)
}

/// Whether native runs are available to this tool call. They are when
/// `[agent.runs] enabled` is true; a plugin that wants to fall back to its own
/// way of running work (the bundled `task` plugin does) asks this first. True
/// means `spawn`, `wait` and the rest can be used.
///
/// @param ctx LuaCtx Agent context.
/// @return boolean
/// @example
/// if maki.run.enabled(ctx) then
///   local run = maki.run.spawn(ctx, { kind = "shell", description = "tests", command = "make test" })
/// end
#[lua_fn]
async fn enabled(_lua: Lua, ctx: mlua::UserDataRef<LuaCtx>) -> LuaResult<bool> {
    Ok(ctx
        .agent()
        .is_some_and(|agent| native::host_enabled(&agent.to_tool_context())))
}

/// Starts background work and returns at once, like the `spawn` tool. The
/// fields are the tool's: `kind` (`"agent"` or `"shell"`) and `description` are
/// required; `name`, `prompt`, `subagent_type`, `model_tier`, `thinking`,
/// `output_schema`, `context`, `one_shot`, `command`, `cwd`, `timeout_secs`,
/// `watch` and `notify` are optional. A run that cannot start now queues.
/// `bind_to_caller` is this function's own: with it, the run belongs to the
/// call and is stopped when the call is cancelled, so a caller that walked
/// away from its sub-agent leaves no run behind. Needs the `run` permission.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table The spawn input.
/// @return (table?, string?) `{ handle, name, kind, state, text }` for the new run, or
///   `(nil, err)` (limits, bad input, runs not enabled).
/// @example
/// local run, err = maki.run.spawn(ctx, {
///   kind = "agent",
///   description = "Find auth middleware",
///   prompt = "Search the codebase for auth middleware.",
///   subagent_type = "research",
///   one_shot = true,
///   bind_to_caller = true,
/// })
/// if err then error(err) end
/// print(run.handle, run.state)
#[lua_fn(guard = Run)]
async fn spawn(lua: Lua, ctx: mlua::UserDataRef<LuaCtx>, opts: LuaValue) -> LuaResult<Pair<Table>> {
    let mut input = lua_to_json(&lua, &opts)?;
    // `bind_to_caller` is the plugin's own field, not one of the tool's: it
    // is read here and taken out before the input is validated.
    let bound = input
        .get("bind_to_caller")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    if let Some(obj) = input.as_object_mut() {
        obj.remove("bind_to_caller");
    }
    let agent = try_pair!(dispatch_ctx(&ctx, "spawn"));
    let tctx = agent.to_tool_context();
    drop(ctx);
    let run = try_pair!(if bound {
        native::host_spawn_bound(&tctx, input).await
    } else {
        native::host_spawn(&tctx, input).await
    });
    let t = lua.create_table()?;
    t.set("handle", run.handle)?;
    t.set("name", run.name)?;
    t.set("kind", run.kind)?;
    t.set("state", run.state)?;
    t.set("text", run.text)?;
    Ok((Some(t), None))
}

/// Sends a message to a run (or `"parent"` / `"main"`), like the `send` tool.
/// Fields: `to` and `message` (required), `mode` (`"steer"`, `"followup"` or
/// `"quiet"`). Reaching a finished agent revives it. Needs the `agent` permission.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table The send input.
/// @return (string?, string?) The delivery report, or `(nil, err)`.
/// @example
/// local out, err = maki.run.send(ctx, { to = "worker", message = "also check the tests" })
#[lua_fn(guard = Agent)]
async fn send(lua: Lua, ctx: mlua::UserDataRef<LuaCtx>, opts: LuaValue) -> LuaResult<Pair<String>> {
    call(lua, ctx, "send", Some(opts)).await
}

/// Lists runs or shows one, like the `runs` tool. Fields (all optional): `id`,
/// `scope` (`"children"`, `"descendants"`, `"all"`), `full`, `tail`,
/// `from_offset`, `include_finished`. Needs the `agent` permission.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table? The runs input.
/// @return (string?, string?) The listing as the tool prints it, or `(nil, err)`.
/// @example
/// print(maki.run.runs(ctx, { scope = "descendants" }))
#[lua_fn(guard = Agent)]
async fn runs(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    opts: Option<LuaValue>,
) -> LuaResult<Pair<String>> {
    call(lua, ctx, "runs", opts).await
}

/// Stops runs and what they started, like the `stop` tool. Fields: `ids`
/// (required), `reason`, `detach`, `soft`, `wait_secs`. Needs the `agent`
/// permission.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table The stop input.
/// @return (string?, string?) The outcome per run, or `(nil, err)`.
/// @example
/// maki.run.stop(ctx, { ids = { run.handle } })
#[lua_fn(guard = Agent)]
async fn stop(lua: Lua, ctx: mlua::UserDataRef<LuaCtx>, opts: LuaValue) -> LuaResult<Pair<String>> {
    call(lua, ctx, "stop", Some(opts)).await
}

async fn call(
    lua: Lua,
    ctx: mlua::UserDataRef<LuaCtx>,
    name: &'static str,
    opts: Option<LuaValue>,
) -> LuaResult<Pair<String>> {
    let input = input_of(&lua, opts)?;
    let agent = try_pair!(dispatch_ctx(&ctx, name));
    let tctx = agent.to_tool_context();
    drop(ctx);
    Ok(match native::host_call(&tctx, name, input).await {
        Ok(text) => (Some(text), None),
        Err(e) => err_pair(e),
    })
}

/// Waits for runs to settle, like the `wait` tool, and returns one table per
/// run: `{ handle, name, kind, state, description, settled, done, reason?,
/// detail?, exit_code?, summary?, truncated?, structured?, full_text_path? }`.
/// `summary` is an agent's result or a shell's last output. Fields: `ids`
/// (required), `mode` (`"all"` or `"any"`), `timeout_secs` (1 to 600). A wait
/// that times out returns the runs as they stand (`settled` is false for those
/// still going); a cancel of the calling task returns `(nil, err)` and leaves
/// the runs running. A plugin waits for whatever it started; the model's
/// `wait` tool is the one that keeps to commands, since an agent reports with
/// send and its end sends a notice. Needs the `agent` permission.
///
/// @param ctx LuaCtx Agent context.
/// @param opts table The wait input.
/// @return (table?, string?) Array of run tables, or `(nil, err)`.
/// @example
/// local runs, err = maki.run.wait(ctx, { ids = { run.handle }, timeout_secs = 120 })
/// if err then error(err) end
/// print(runs[1].reason, runs[1].summary)
#[lua_fn(guard = Agent)]
async fn wait(lua: Lua, ctx: mlua::UserDataRef<LuaCtx>, opts: LuaValue) -> LuaResult<Pair<Table>> {
    let input = lua_to_json(&lua, &opts)?;
    let agent = try_pair!(dispatch_ctx(&ctx, "wait"));
    let tctx = agent.to_tool_context();
    drop(ctx);
    let (views, _satisfied) = try_pair!(native::host_wait(&tctx, input).await);
    let out = lua.create_table()?;
    for (i, view) in views.iter().enumerate() {
        out.set(i + 1, view_to_lua(&lua, view)?)?;
    }
    Ok((Some(out), None))
}

lua_table! {
    /// The native run hub: background agents and shell commands that outlive a
    /// turn, the same runs the model's `spawn`, `send`, `runs`, `wait` and
    /// `stop` tools work on. Every function takes the tool handler's ctx and
    /// answers `(value, err)`. Runs are available when `[agent.runs] enabled`
    /// is true; without it every function but `enabled` returns an error.
    "maki.run" => pub(crate) fn create_run_table(perms: &PluginPermissions),
    DOCS [enabled, spawn(perms), send(perms), runs(perms), wait(perms), stop(perms)]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::plugin_permissions::Permission;
    use maki_agent::runs::{RunsCtx, ShellOptions, SystemClock, UnitHub, UnitLimits};
    use maki_agent::tools::test_support::stub_ctx_with;
    use maki_agent::{AgentMode, EventSender};

    use super::*;

    struct Fixture {
        lua: Lua,
        hub: Option<Arc<UnitHub>>,
        _dir: tempfile::TempDir,
    }

    fn fixture(with_runs: bool, perms: PluginPermissions) -> Fixture {
        fixture_with_cancel(with_runs, perms, maki_agent::cancel::CancelToken::none())
    }

    /// A fixture whose tool call carries {cancel}, so a test can fire it.
    fn fixture_with_cancel(
        with_runs: bool,
        perms: PluginPermissions,
        cancel: maki_agent::cancel::CancelToken,
    ) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut ctx = stub_ctx_with(&AgentMode::Build, None, Some("tu-1"));
        ctx.cancel = cancel;
        let hub = with_runs.then(|| {
            let (tx, _rx) = flume::unbounded();
            let hub = UnitHub::with_shell_options(
                UnitLimits::default(),
                EventSender::new(tx, 0),
                Arc::new(SystemClock),
                ShellOptions {
                    dir: Some(dir.path().to_path_buf()),
                    ..ShellOptions::default()
                },
            );
            ctx.runs = Some(RunsCtx {
                hub: Arc::clone(&hub),
                self_run: hub.main(),
            });
            hub
        });
        let lua = Lua::new();
        lua.globals()
            .set("run", create_run_table(&lua, &perms).unwrap())
            .unwrap();
        lua.globals()
            .set("ctx", lua.create_userdata(LuaCtx::handler(&ctx)).unwrap())
            .unwrap();
        Fixture {
            lua,
            hub,
            _dir: dir,
        }
    }

    fn eval<T: mlua::FromLuaMulti>(f: &Fixture, code: &str) -> T {
        smol::block_on(f.lua.load(code).eval_async()).unwrap()
    }

    #[test]
    fn run_table_has_the_five_calls_and_enabled() {
        let f = fixture(true, PluginPermissions::trusted());
        for name in ["spawn", "send", "runs", "wait", "stop", "enabled"] {
            let kind: String = eval(&f, &format!("return type(run.{name})"));
            assert_eq!(kind, "function", "{name}");
        }
    }

    #[test]
    fn spawn_wait_runs_and_stop_work_over_the_hub() {
        let f = fixture(true, PluginPermissions::trusted());
        let enabled: bool = eval(&f, "return run.enabled(ctx)");
        assert!(enabled);
        let (handle, kind, state): (String, String, String) = eval(
            &f,
            r#"
            local r, err = run.spawn(ctx, { kind = "shell", description = "echo", command = "echo hello-run", name = "echoer" })
            assert(not err, err)
            return r.handle, r.kind, r.state
            "#,
        );
        assert!(handle.starts_with("sh-"), "{handle}");
        assert_eq!(kind, "shell");
        assert!(!state.is_empty());
        let (reason, summary, settled): (String, String, bool) = eval(
            &f,
            r#"
            local views, err = run.wait(ctx, { ids = { "echoer" }, timeout_secs = 30 })
            assert(not err, err)
            return views[1].reason, views[1].summary, views[1].settled
            "#,
        );
        assert!(settled);
        assert_eq!(reason, "completed");
        assert!(summary.contains("hello-run"), "{summary}");
        let listing: String = eval(&f, "return (run.runs(ctx, { include_finished = true }))");
        assert!(listing.contains("echoer"), "{listing}");
        let (text, err): (Option<String>, Option<String>) = eval(
            &f,
            r#"return run.send(ctx, { to = "echoer", message = "hi" })"#,
        );
        // A finished shell cannot be messaged; the failure is an error pair.
        assert!(text.is_none() && err.is_some(), "{text:?} {err:?}");
        let stopped: Option<String> = eval(&f, r#"return (run.stop(ctx, { ids = { "echoer" } }))"#);
        assert!(stopped.is_some());
    }

    #[test]
    fn long_shell_is_stopped_through_the_wrapper() {
        let f = fixture(true, PluginPermissions::trusted());
        let state: String = eval(
            &f,
            r#"
            local r = assert(run.spawn(ctx, { kind = "shell", description = "sleeper", command = "sleep 60", name = "sleeper" }))
            assert(run.stop(ctx, { ids = { r.handle } }))
            local views = assert(run.wait(ctx, { ids = { r.handle }, timeout_secs = 30 }))
            return views[1].state
            "#,
        );
        assert_eq!(state, "killed");
    }

    /// A run spawned with `bind_to_caller` belongs to the call: it is
    /// stopped when the call's token fires, so a caller that walked away
    /// from its sub-agent leaves no run behind holding an agent slot. A run
    /// without it outlives the call, as a background task does.
    #[test]
    fn a_bound_run_is_stopped_with_its_caller() {
        let (trigger, token) = maki_agent::cancel::CancelToken::new();
        let f = fixture_with_cancel(true, PluginPermissions::trusted(), token);
        eval::<()>(
            &f,
            r#"
            local r = assert(run.spawn(ctx, { kind = "shell", description = "sleeper", command = "sleep 60", name = "bound", bind_to_caller = true }))
            local u = assert(run.spawn(ctx, { kind = "shell", description = "sleeper", command = "sleep 60", name = "free" }))
            return
            "#,
        );
        trigger.cancel();
        let hub = f.hub.as_ref().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let state_of = |name: &str| {
            hub.list(hub.main(), maki_agent::runs::Scope::All)
                .into_iter()
                .find(|v| v.name.as_str() == name)
                .map(|v| v.state_label().to_owned())
                .unwrap_or_default()
        };
        loop {
            let bound = state_of("bound");
            if bound == "killed" {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the bound run was not stopped with its caller, it is {bound}"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_ne!(
            state_of("free"),
            "killed",
            "a run that was not bound must outlive the call"
        );
    }

    #[test]
    fn without_runs_everything_but_enabled_is_an_error_pair() {
        let f = fixture(false, PluginPermissions::trusted());
        let enabled: bool = eval(&f, "return run.enabled(ctx)");
        assert!(!enabled);
        let (val, err): (Option<mlua::Value>, Option<String>) = eval(
            &f,
            r#"return run.spawn(ctx, { kind = "shell", description = "x", command = "true" })"#,
        );
        assert!(val.is_none());
        assert!(err.unwrap().contains("not available"));
        let (val, err): (Option<mlua::Value>, Option<String>) = eval(&f, "return run.runs(ctx)");
        assert!(val.is_none() && err.is_some());
    }

    #[test]
    fn spawn_needs_the_run_permission() {
        let mut perms = PluginPermissions::trusted();
        perms.set(Permission::Run, false);
        let f = fixture(true, perms);
        let res: mlua::Result<mlua::Value> = smol::block_on(
            f.lua
                .load(r#"return run.spawn(ctx, { kind = "shell", description = "x", command = "true" })"#)
                .eval_async(),
        );
        assert!(res.is_err());
    }
}
