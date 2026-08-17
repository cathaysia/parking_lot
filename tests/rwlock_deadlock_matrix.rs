use parking_lot::{RwLock, RwLockUpgradableReadGuard};
#[cfg(feature = "deadlock_detection")]
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

const LOCK_TIMEOUT: Duration = Duration::from_millis(20);
const CASE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Acquired,
    TimedOut,
}

type Case = fn(Arc<RwLock<()>>) -> Outcome;

fn outcome<T>(result: Option<T>) -> Outcome {
    if result.is_some() {
        Outcome::Acquired
    } else {
        Outcome::TimedOut
    }
}

fn run_case(case: Case) -> Outcome {
    let lock = Arc::new(RwLock::new(()));
    let (sender, receiver) = mpsc::channel();

    thread::spawn(move || {
        sender.send(case(lock)).unwrap();
    });

    receiver
        .recv_timeout(CASE_TIMEOUT)
        .expect("lock matrix case did not finish")
}

#[cfg(feature = "deadlock_detection")]
fn run_case_expect_panic(case: Case) -> bool {
    let lock = Arc::new(RwLock::new(()));
    let (sender, receiver) = mpsc::channel();

    thread::spawn(move || {
        let panicked = catch_unwind(AssertUnwindSafe(|| case(lock))).is_err();
        sender.send(panicked).unwrap();
    });

    receiver
        .recv_timeout(CASE_TIMEOUT)
        .expect("panic matrix case did not finish")
}

macro_rules! rwlock_matrix {
    (
        $(
            $case:ident: $label:literal {
                timeout: $timeout:ident,
                panic: $panic:literal,
                run: |$lock:ident| $body:block
            }
        ),+ $(,)?
    ) => {
        $(
            fn $case($lock: Arc<RwLock<()>>) -> Outcome $body
        )+

        #[test]
        fn test1_rwlock_matrix_expect_timeout() {
            let cases: &[(&str, Case, Outcome)] = &[
                $(($label, $case, Outcome::$timeout),)+
            ];

            for &(name, case, expected) in cases {
                let actual = run_case(case);
                assert_eq!(actual, expected, "unexpected result for {name}");
            }
        }

        #[cfg(feature = "deadlock_detection")]
        #[test]
        #[ignore = "the recursive-lock panic hook is not implemented yet"]
        fn test2_rwlock_matrix_expect_panic() {
            let cases: &[(&str, Case, bool)] = &[
                $(($label, $case, $panic),)+
            ];

            for &(name, case, expected) in cases {
                let actual = run_case_expect_panic(case);
                assert_eq!(actual, expected, "unexpected panic result for {name}");
            }
        }
    };
}

rwlock_matrix! {
    read_read: "read -> read" {
        timeout: Acquired,
        panic: true,
        run: |lock| {
            let _read = lock.read();
            outcome(lock.try_read_for(LOCK_TIMEOUT))
        }
    },
    read_read_with_waiting_writer: "read -> read (writer waiting)" {
        timeout: TimedOut,
        panic: true,
        run: |lock| {
            let read = lock.read();
            let (started_sender, started_receiver) = mpsc::channel();
            let writer_lock = lock.clone();
            let writer = thread::spawn(move || {
                started_sender.send(()).unwrap();
                let _write = writer_lock.write();
            });

            started_receiver.recv().unwrap();
            thread::sleep(Duration::from_millis(10));
            let result = outcome(lock.try_read_for(LOCK_TIMEOUT));
            drop(read);
            writer.join().unwrap();
            result
        }
    },
    read_read_recursive: "read -> read_recursive" {
        timeout: Acquired,
        panic: false,
        run: |lock| {
            let _read = lock.read();
            outcome(lock.try_read_recursive_for(LOCK_TIMEOUT))
        }
    },
    read_write: "read -> write" {
        timeout: TimedOut,
        panic: true,
        run: |lock| {
            let _read = lock.read();
            outcome(lock.try_write_for(LOCK_TIMEOUT))
        }
    },
    read_upgradable: "read -> upgradable" {
        timeout: Acquired,
        panic: false,
        run: |lock| {
            let _read = lock.read();
            outcome(lock.try_upgradable_read_for(LOCK_TIMEOUT))
        }
    },
    write_read: "write -> read" {
        timeout: TimedOut,
        panic: true,
        run: |lock| {
            let _write = lock.write();
            outcome(lock.try_read_for(LOCK_TIMEOUT))
        }
    },
    write_write: "write -> write" {
        timeout: TimedOut,
        panic: true,
        run: |lock| {
            let _write = lock.write();
            outcome(lock.try_write_for(LOCK_TIMEOUT))
        }
    },
    upgradable_read: "upgradable -> read" {
        timeout: Acquired,
        panic: false,
        run: |lock| {
            let _upgradable = lock.upgradable_read();
            outcome(lock.try_read_for(LOCK_TIMEOUT))
        }
    },
    upgradable_write: "upgradable -> write" {
        timeout: TimedOut,
        panic: true,
        run: |lock| {
            let _upgradable = lock.upgradable_read();
            outcome(lock.try_write_for(LOCK_TIMEOUT))
        }
    },
    upgradable_upgradable: "upgradable -> upgradable" {
        timeout: TimedOut,
        panic: true,
        run: |lock| {
            let _upgradable = lock.upgradable_read();
            outcome(lock.try_upgradable_read_for(LOCK_TIMEOUT))
        }
    },
    upgradable_upgrade: "upgradable -> upgrade" {
        timeout: Acquired,
        panic: false,
        run: |lock| {
            let upgradable = lock.upgradable_read();
            if RwLockUpgradableReadGuard::try_upgrade_for(upgradable, LOCK_TIMEOUT).is_ok() {
                Outcome::Acquired
            } else {
                Outcome::TimedOut
            }
        }
    }
}
