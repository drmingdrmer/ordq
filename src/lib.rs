//! Order keeping job processing queue.
//!
//! This module provides a multithreaded worker pool
//! that processes tasks while keeping the order of outputs.
//!
//! Example:
//! ```rust
//! struct Add;
//!
//! impl ordq::Work for Add {
//!     type I = (i32, i32);
//!     type O = i32;
//!
//!     fn run(&mut self, x: Self::I) -> Self::O {
//!         x.0 + x.1
//!     }
//! }
//!
//! # fn test_jobq() {
//! let (tx, rx) = ordq::new(1024, vec![Add, Add]);
//!
//! tx.send((1, 2));
//! tx.send((3, 4));
//! tx.send((5, 6));
//!
//! tx.close();
//!
//! assert_eq!(rx.recv(), Some(Ok(3)));
//! assert_eq!(rx.recv(), Some(Ok(7)));
//! assert_eq!(rx.recv(), Some(Ok(11)));
//! assert_eq!(rx.recv(), None);
//! assert_eq!(rx.recv(), None);
//! # }
//! ```

use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;

/// `Work` can do some work with input `I`, and output `O`.
pub trait Work: Send + 'static {
    type I: Send;
    type O: Send;
    fn name(&self) -> Option<String> {
        None
    }
    fn run(&mut self, x: Self::I) -> Self::O;
}

/// A job to do by a `Work`.
struct Job<W: Work> {
    input: W::I,
    /// A sender sends the output to a pre-allocated position in the output channel.
    tx: mpsc::Sender<W::O>,
}

/// A `Worker` exhausts jobs from the input channel and sends the output to the output channel.
struct Worker<W: Work> {
    input: crossbeam_channel::Receiver<Job<W>>,

    work: W,

    /// Worker stat
    stats: Arc<Stats>,
}

impl<W: Work> Worker<W> {
    fn worker_loop(mut self) {
        while let Ok(job) = self.input.recv() {
            let output = self.work.run(job.input);
            if self.stats.inject_send_failure.load(Ordering::Relaxed) {
                continue;
            }
            let _ = job.tx.send(output);
            self.stats.processed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Error when a sender failed to send the input because of receiving end is dropped
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("failed to send, receiver dropped")]
pub struct SendError<I>(pub I);

/// Error when a worker failed to send the output.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("failed to receive from a worker")]
pub struct RecvError;

/// The input sender that sends job input to the workers.
#[derive(Clone)]
pub struct Sender<W: Work> {
    input_tx: crossbeam_channel::Sender<Job<W>>,
    output_tx: mpsc::Sender<mpsc::Receiver<W::O>>,
}

impl<W: Work> Sender<W> {
    /// Push a job into the queue.
    pub fn send(&self, input: W::I) -> Result<(), SendError<W::I>> {
        let (tx, rx) = mpsc::channel();

        // Push to output to keep order.
        if let Err(_e) = self.output_tx.send(rx) {
            return Err(SendError(input));
        }

        let job = Job { input, tx };
        let _ = self.input_tx.send(job);
        Ok(())
    }

    pub fn close(self) {}
}

/// Receives job output from the thread pool.
pub struct Receiver<W: Work> {
    output_rx: mpsc::Receiver<mpsc::Receiver<W::O>>,
    worker_handles: Vec<(Arc<Stats>, JoinHandle<()>)>,
}

impl<W: Work> Receiver<W> {
    /// Receive an output.
    ///
    /// If the sending end is dropped and no more pending job to run, return `None`.
    ///
    /// If a worker fails to send the output, return `Some(RecvError)`.
    /// In this case, the caller can still receive the output from other workers.
    pub fn recv(&self) -> Option<Result<W::O, RecvError>> {
        // No more output if all workers are dropped.
        let rx = self.output_rx.recv().ok()?;
        Some(rx.recv().map_err(|_e| RecvError))
    }

    /// Returns the number of jobs processed by each worker.
    pub fn stats(&self) -> Vec<usize> {
        self.worker_handles
            .iter()
            .map(|(a, _)| a.processed.load(Ordering::Relaxed))
            .collect()
    }
}

/// Worker stats
#[derive(Default)]
pub struct Stats {
    /// Number of job processed by this worker.
    pub processed: AtomicUsize,

    /// Inject send failure for testing.
    inject_send_failure: AtomicBool,
}

/// Create a worker thread pool with the given queue capacity and workers.
///
/// The workers blocks on the input channel, until all of the senders are dropped.
pub fn new<W: Work>(
    capacity: usize,
    workers: impl IntoIterator<Item = W>,
) -> (Sender<W>, Receiver<W>) {
    let (input_tx, input_rx) = crossbeam_channel::bounded(capacity);
    let mut handles = Vec::new();

    for work in workers.into_iter() {
        let stat = Arc::new(Stats::default());

        let worker = Worker {
            input: input_rx.clone(),
            work,
            stats: stat.clone(),
        };

        let h = std::thread::spawn(move || {
            worker.worker_loop();
        });

        handles.push((stat, h));
    }

    let (output_tx, output_rx) = mpsc::channel();

    let tx = Sender {
        input_tx,
        output_tx,
    };

    let rx = Receiver {
        output_rx,
        worker_handles: handles,
    };

    (tx, rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Add {
        i: u64,
    }

    impl Work for Add {
        type I = (i32, i32);
        type O = i32;

        fn run(&mut self, x: Self::I) -> Self::O {
            std::thread::sleep(std::time::Duration::from_micros(self.i));
            x.0 + x.1
        }
    }

    #[test]
    fn test_ordq() {
        let (tx, rx) = new(1024, vec![Add { i: 3 }, Add { i: 0 }, Add { i: 0 }]);

        tx.send((1, 2)).unwrap();
        tx.send((3, 4)).unwrap();
        tx.send((5, 6)).unwrap();

        tx.close();

        assert_eq!(rx.recv(), Some(Ok(3)));
        assert_eq!(rx.recv(), Some(Ok(7)));
        assert_eq!(rx.recv(), Some(Ok(11)));
        assert_eq!(rx.recv(), None);
        assert_eq!(rx.recv(), None);
    }

    #[test]
    fn test_ordq_order_is_kept() {
        let (tx, rx) = new(1024, vec![Add { i: 3 }, Add { i: 2 }, Add { i: 0 }]);

        let n = 1_000_000;

        let _h = std::thread::spawn(move || {
            for i in 0..n {
                tx.send((0, i)).unwrap();
            }
        });

        for i in 0..n {
            assert_eq!(rx.recv(), Some(Ok(i)));
        }

        assert_eq!(rx.recv(), None);
        println!("{:?}", rx.stats());
    }

    #[test]
    fn test_ordq_receiver_drooped() {
        let (tx, rx) = new(1024, vec![Add { i: 3 }, Add { i: 0 }, Add { i: 0 }]);

        tx.send((0, 2)).unwrap();
        tx.send((0, 4)).unwrap();

        assert_eq!(rx.recv(), Some(Ok(2)));
        assert_eq!(rx.recv(), Some(Ok(4)));

        drop(rx);
        assert!(tx.send((0, 6)).is_err());
    }

    struct Foo;

    impl Work for Foo {
        type I = i32;
        type O = i32;

        fn run(&mut self, x: Self::I) -> Self::O {
            x
        }
    }

    #[test]
    fn test_ordq_worker_not_send() {
        let (tx, rx) = new(1024, vec![Foo, Foo]);

        tx.send(2).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5));

        // Inject send failure, not output will be sent.
        for h in rx.worker_handles.iter() {
            h.0.inject_send_failure.store(true, Ordering::Relaxed);
        }

        tx.send(3).unwrap();

        assert_eq!(rx.recv(), Some(Ok(2)));
        assert_eq!(rx.recv(), Some(Err(RecvError)));

        // Reset the failure, the output will be sent.

        for h in rx.worker_handles.iter() {
            h.0.inject_send_failure.store(false, Ordering::Relaxed);
        }

        tx.send(4).unwrap();
        assert_eq!(rx.recv(), Some(Ok(4)));
    }
}
