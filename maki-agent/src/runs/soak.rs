//! Soak tests: many real shell runs spawned, stopped and left to finish, then
//! checks that nothing leaks (no process group, no queued notice, no slot)
//! and that the bounded structures stayed inside their bounds.

use std::sync::Arc;
use std::time::{Duration, Instant};

use maki_storage::runs::{JournalOptions, Replay, RunJournal};
use serde_json::json;

use super::inbox::MAX_PENDING;
use super::*;
use crate::Envelope;

const SPAWNS: usize = 150;
const MAX_SHELLS: u8 = 6;
const MAX_RETAINED: u16 = 16;

fn hub_in(dir: &std::path::Path) -> (Arc<UnitHub>, Arc<RunJournal>) {
    let (tx, rx) = flume::unbounded::<Envelope>();
    std::mem::forget(rx);
    let hub = UnitHub::with_shell_options(
        UnitLimits {
            max_shells: MAX_SHELLS,
            max_shells_per_owner: MAX_SHELLS,
            spawn_per_minute: u8::MAX,
            session_max_runs: u16::MAX,
            max_retained: MAX_RETAINED,
            ..UnitLimits::default()
        },
        EventSender::new(tx, 0),
        Arc::new(ManualClock::default()),
        ShellOptions {
            grace: Duration::from_millis(100),
            ..ShellOptions::default()
        },
    );
    let journal = Arc::new(RunJournal::open_in(dir.to_owned(), JournalOptions::default()).unwrap());
    hub.attach_journal(Arc::clone(&journal));
    (hub, journal)
}

fn shell(command: &str) -> SpawnRequest {
    serde_json::from_value(json!({
        "kind": "shell", "description": "soak", "command": command
    }))
    .unwrap()
}

fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn live(hub: &UnitHub) -> Vec<UnitView> {
    hub.list(hub.main(), Scope::All)
        .into_iter()
        .filter(|u| !u.state.is_done())
        .collect()
}

fn pgids(journal: &RunJournal) -> Vec<u32> {
    journal
        .replay()
        .runs
        .iter()
        .filter_map(|r| r.proc.as_ref().map(|p| p.pgid))
        .collect()
}

#[test]
fn many_spawns_and_stops_leak_nothing_and_stay_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let (hub, journal) = hub_in(dir.path());
    let mut spawned = 0;
    for n in 0..SPAWNS {
        // A third finish on their own, the rest are long sleepers to stop.
        let command = if n % 3 == 0 { "echo hi" } else { "sleep 300" };
        let ack = loop {
            match hub.spawn(hub.main(), shell(command)) {
                Ok(ack) => break ack,
                Err(_) => {
                    // At the shell cap: stop what is running and retry.
                    let ids: Vec<_> = live(&hub).iter().map(|u| u.id).collect();
                    hub.stop(hub.main(), &ids, StopOpts::default());
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        };
        spawned += 1;
        assert!(live(&hub).len() <= MAX_SHELLS as usize, "cap exceeded");
        if n % 3 == 1 {
            hub.stop(hub.main(), &[ack.id], StopOpts::default());
        }
        let queued = hub.pending_notices(hub.main()).len();
        assert!(queued <= MAX_PENDING, "main inbox grew to {queued}");
    }
    assert_eq!(spawned, SPAWNS);
    let ids: Vec<_> = live(&hub).iter().map(|u| u.id).collect();
    hub.stop(hub.main(), &ids, StopOpts::default());
    until("every run settled", || live(&hub).is_empty());

    let groups = pgids(&journal);
    assert!(groups.len() >= SPAWNS / 2, "{} proc records", groups.len());
    until("no process group left", || {
        groups.iter().all(|&g| !shell::group_exists(g))
    });
    assert!(hub.table().pending_notices.is_empty());
    assert_eq!(hub.notices_in_flight.load(Ordering::SeqCst), 0);
    let shown = hub.list(hub.main(), Scope::All).len();
    assert!(
        shown <= SPAWNS,
        "listing holds {shown} rows for {SPAWNS} runs"
    );
    // Finished runs past `max_retained` become one-line tombstones. Full
    // units are bounded by retention, units plus tombstones by the session
    // run cap (`session_max_runs`), which counts every spawn of this process.
    let (full, tombs) = {
        let table = hub.table();
        (table.units.len(), table.tombstones.len())
    };
    assert!(
        full <= MAX_RETAINED as usize,
        "{full} full units kept, cap {MAX_RETAINED}"
    );
    assert_eq!(full + tombs, SPAWNS, "every run is a unit or a tombstone");
    assert!(hub.pending_notices(hub.main()).len() <= MAX_PENDING);

    hub.shutdown();
    assert!(Replay::read(journal.dir()).clean_mark);
}

#[test]
fn dropping_the_hub_leaves_no_live_process_group() {
    let dir = tempfile::tempdir().unwrap();
    let (hub, journal) = hub_in(dir.path());
    for _ in 0..MAX_SHELLS {
        hub.spawn(hub.main(), shell("sleep 300")).unwrap();
    }
    until("every proc record", || {
        pgids(&journal).len() == MAX_SHELLS as usize
    });
    let groups = pgids(&journal);
    assert!(groups.iter().all(|&g| shell::group_exists(g)));
    drop(hub);
    until("no process group left", || {
        groups.iter().all(|&g| !shell::group_exists(g))
    });
}
