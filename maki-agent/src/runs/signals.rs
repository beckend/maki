//! SIGTERM, SIGHUP and SIGINT tear down running shells before maki dies.

use std::os::fd::RawFd;
use std::sync::atomic::{AtomicI32, Ordering};

const SIGNALS: [libc::c_int; 3] = [libc::SIGTERM, libc::SIGHUP, libc::SIGINT];
static WRITE_FD: AtomicI32 = AtomicI32::new(-1);

extern "C" fn on_signal(sig: libc::c_int) {
    let fd = WRITE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let byte = sig as u8;
        // SAFETY: write(2) is async-signal-safe; a full pipe just drops it.
        unsafe { libc::write(fd, (&raw const byte).cast(), 1) };
    }
}

pub(super) fn install() {
    if WRITE_FD.load(Ordering::SeqCst) >= 0 {
        return;
    }
    let mut fds: [RawFd; 2] = [0; 2];
    // SAFETY: fds is a valid two-element array.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } != 0 {
        return;
    }
    let [read_fd, write_fd] = fds;
    // The reader is blocking; only the write end must not block the handler.
    // SAFETY: read_fd is the fd we just created.
    unsafe {
        let flags = libc::fcntl(read_fd, libc::F_GETFL);
        libc::fcntl(read_fd, libc::F_SETFL, flags & !libc::O_NONBLOCK);
    }
    let spawned = std::thread::Builder::new()
        .name("maki-signals".into())
        .spawn(move || {
            let mut byte = 0u8;
            loop {
                // SAFETY: reads one byte into a valid buffer.
                let n = unsafe { libc::read(read_fd, (&raw mut byte).cast(), 1) };
                if n == 1 {
                    break;
                }
                if n == 0
                    || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
                {
                    return;
                }
            }
            super::shutdown_all();
            let sig = byte as libc::c_int;
            // SAFETY: restore the default action and die of the same signal.
            unsafe {
                libc::signal(sig, libc::SIG_DFL);
                libc::raise(sig);
            }
        });
    if spawned.is_err() {
        return;
    }
    WRITE_FD.store(write_fd, Ordering::SeqCst);
    for sig in SIGNALS {
        // SAFETY: the handler only calls async-signal-safe functions.
        unsafe { libc::signal(sig, on_signal as *const () as libc::sighandler_t) };
    }
}
