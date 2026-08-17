use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
#[cfg(feature = "deadlock_detection")]
use std::sync::Once;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

const LOCK_TIMEOUT: Duration = Duration::from_millis(20);

#[cfg(feature = "deadlock_detection")]
fn enable_panic_on_deadlock() {
    static ENABLE: Once = Once::new();
    ENABLE.call_once(|| std::env::set_var("PARKING_LOT_PANIC_ON_DEADLOCK", "1"));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Acquired,
    TimedOut,
}

fn outcome<T>(result: Option<T>) -> Outcome {
    if result.is_some() {
        Outcome::Acquired
    } else {
        Outcome::TimedOut
    }
}

macro_rules! rwlock_panic_test {
    (true, $label:literal, $timeout:ident, |$lock:ident| $body:block) => {
        #[cfg(feature = "deadlock_detection")]
        #[test]
        #[should_panic(expected = "parking_lot: possible recursive RwLock deadlock")]
        fn expect_panic() {
            enable_panic_on_deadlock();
            let $lock = Arc::new(RwLock::new(()));
            let _ = $body;
        }
    };
    (false, $label:literal, $timeout:ident, |$lock:ident| $body:block) => {
        #[cfg(feature = "deadlock_detection")]
        #[test]
        fn expect_no_panic() {
            enable_panic_on_deadlock();
            let $lock = Arc::new(RwLock::new(()));
            let actual = $body;
            assert_eq!(
                actual,
                Outcome::$timeout,
                "unexpected result for {}",
                $label
            );
        }
    };
}

macro_rules! rwlock_matrix {
    (
        $(
            $case:ident: $label:literal {
                timeout: $timeout:ident,
                panic: $panic:tt,
                run: |$lock:ident| $body:block
            }
        ),+ $(,)?
    ) => {
        $(
            mod $case {
                use super::*;

                #[cfg(not(feature = "deadlock_detection"))]
                #[test]
                fn expect_timeout() {
                    let $lock = Arc::new(RwLock::new(()));
                    let actual = $body;
                    assert_eq!(
                        actual,
                        Outcome::$timeout,
                        "unexpected result for {}",
                        $label
                    );
                }

                rwlock_panic_test!($panic, $label, $timeout, |$lock| $body);
            }
        )+
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
    },
    upgrade_drop_upgradable: "upgrade -> drop -> upgradable" {
        timeout: Acquired,
        panic: false,
        run: |lock| {
            let upgradable = lock.upgradable_read();
            let write = RwLockUpgradableReadGuard::try_upgrade_for(upgradable, LOCK_TIMEOUT)
                .ok()
                .unwrap();
            drop(write);
            outcome(lock.try_upgradable_read_for(LOCK_TIMEOUT))
        }
    },
    upgrade_downgrade_read: "upgrade -> downgrade -> read" {
        timeout: Acquired,
        panic: true,
        run: |lock| {
            let upgradable = lock.upgradable_read();
            let write = RwLockUpgradableReadGuard::try_upgrade_for(upgradable, LOCK_TIMEOUT)
                .ok()
                .unwrap();
            let _read = RwLockWriteGuard::downgrade(write);
            outcome(lock.try_read_for(LOCK_TIMEOUT))
        }
    },
    downgrade_read_upgradable: "write -> downgrade -> upgradable" {
        timeout: Acquired,
        panic: false,
        run: |lock| {
            let write = lock.write();
            let _read = RwLockWriteGuard::downgrade(write);
            outcome(lock.try_upgradable_read_for(LOCK_TIMEOUT))
        }
    },
    downgrade_upgradable_read: "write -> downgrade_to_upgradable -> read" {
        timeout: Acquired,
        panic: false,
        run: |lock| {
            let write = lock.write();
            let _upgradable = RwLockWriteGuard::downgrade_to_upgradable(write);
            outcome(lock.try_read_for(LOCK_TIMEOUT))
        }
    },
    downgrade_read_to_upgradable: "upgradable -> downgrade -> upgradable" {
        timeout: Acquired,
        panic: false,
        run: |lock| {
            let upgradable = lock.upgradable_read();
            let _read = RwLockUpgradableReadGuard::downgrade(upgradable);
            outcome(lock.try_upgradable_read_for(LOCK_TIMEOUT))
        }
    }
}
