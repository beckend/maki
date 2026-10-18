use std::collections::HashMap;

use super::UnitId;

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
