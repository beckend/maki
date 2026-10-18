use std::collections::HashMap;

use maki_storage::frame::TopologyFacts;

use super::{RunKind, UnitId, UnitLimits, UnitTable};

const MAIN: &str = "main";

/// The kind tag of a child in the fact.
fn kind_tag(kind: RunKind) -> u8 {
    match kind {
        RunKind::Agent => 0,
        RunKind::Shell => 1,
    }
}

/// `who`'s own place, parent and unfinished children. Siblings stay out: they
/// would re-tell every run on each spawn, and `runs scope=all` lists them.
/// `None` for the main agent while it has no unfinished run.
pub(super) fn facts(
    table: &UnitTable,
    main: UnitId,
    who: UnitId,
    limits: &UnitLimits,
) -> Option<TopologyFacts> {
    let mut children: Vec<(String, String, u8)> = table
        .children
        .get(&who)
        .into_iter()
        .flatten()
        .filter_map(|id| table.units.get(id))
        .filter(|u| !u.state.is_done())
        .map(|u| {
            (
                u.handle.to_string(),
                u.name.to_string(),
                kind_tag(u.kind.kind()),
            )
        })
        .collect();
    children.sort_by(|a, b| a.1.cmp(&b.1));
    let (me, parent) = if who == main {
        ((MAIN.to_owned(), MAIN.to_owned(), 0), None)
    } else {
        let unit = table.units.get(&who)?;
        let parent = if unit.parent == main {
            (MAIN.to_owned(), MAIN.to_owned())
        } else {
            let p = table.units.get(&unit.parent)?;
            (p.handle.to_string(), p.name.to_string())
        };
        (
            (unit.handle.to_string(), unit.name.to_string(), unit.depth),
            Some(parent),
        )
    };
    TopologyFacts {
        me,
        parent,
        children,
        limits: (limits.max_depth, limits.siblings),
    }
    .normalized()
}

/// Everything below `root`, parents before their children.
pub(super) fn descendants(children: &HashMap<UnitId, Vec<UnitId>>, root: UnitId) -> Vec<UnitId> {
    let mut out = Vec::new();
    let mut stack: Vec<UnitId> = children
        .get(&root)
        .into_iter()
        .flatten()
        .rev()
        .copied()
        .collect();
    while let Some(id) = stack.pop() {
        out.push(id);
        stack.extend(children.get(&id).into_iter().flatten().rev());
    }
    out
}
