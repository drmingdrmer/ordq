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
//! assert_eq!(rx.recv(), Some(3));
//! assert_eq!(rx.recv(), Some(7));
//! assert_eq!(rx.recv(), Some(11));
//! assert_eq!(rx.recv(), None);
//! assert_eq!(rx.recv(), None);
//! # }
//! ```

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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

    /// Stat
    processed: Arc<AtomicUsize>,
}

impl<W: Work> Worker<W> {
    fn worker_loop(mut self) {
        while let Ok(job) = self.input.recv() {
            let res = self.work.run(job.input);
            let _ = job.tx.send(res);
            self.processed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The input sender that sends job input to the workers.
#[derive(Clone)]
pub struct Sender<W: Work> {
    input_tx: crossbeam_channel::Sender<Job<W>>,
    output_tx: mpsc::Sender<mpsc::Receiver<W::O>>,
}

impl<W: Work> Sender<W> {
    /// Push a job into the queue.
    pub fn send(&self, input: W::I) {
        let (tx, rx) = mpsc::channel();

        // Push to output to keep order.
        self.output_tx.send(rx).unwrap();

        let job = Job { input, tx };
        self.input_tx.send(job).unwrap();
    }

    pub fn close(self) {}
}

/// A coordinator runs jobs in a thread pool.
pub struct Receiver<W: Work> {
    output_rx: mpsc::Receiver<mpsc::Receiver<W::O>>,
    worker_handles: Vec<(Arc<AtomicUsize>, JoinHandle<()>)>,
}

impl<W: Work> Receiver<W> {
    /// Receive an output.
    ///
    /// If all workers are dropped, return None.
    pub fn recv(&self) -> Option<W::O> {
        // No more output if all workers are dropped.
        let rx = self.output_rx.recv().ok()?;
        let got = rx.recv().ok()?;
        Some(got)
    }

    /// Returns the number of jobs processed by each worker.
    pub fn stats(&self) -> Vec<usize> {
        self.worker_handles
            .iter()
            .map(|(a, _)| a.load(Ordering::Relaxed))
            .collect()
    }
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
        let stat = Arc::new(AtomicUsize::new(0));

        let worker = Worker {
            input: input_rx.clone(),
            work,
            processed: stat.clone(),
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
    fn test_jobq() {
        let (tx, rx) = new(1024, vec![Add { i: 3 }, Add { i: 0 }, Add { i: 0 }]);

        tx.send((1, 2));
        tx.send((3, 4));
        tx.send((5, 6));

        tx.close();

        assert_eq!(rx.recv(), Some(3));
        assert_eq!(rx.recv(), Some(7));
        assert_eq!(rx.recv(), Some(11));
        assert_eq!(rx.recv(), None);
        assert_eq!(rx.recv(), None);
    }

    #[test]
    fn test_jobq_order_is_kept() {
        let (tx, rx) = new(1024, vec![Add { i: 3 }, Add { i: 2 }, Add { i: 0 }]);

        let n = 1_000_000;

        let _h = std::thread::spawn(move || {
            for i in 0..n {
                tx.send((0, i));
            }
        });

        for i in 0..n {
            assert_eq!(rx.recv(), Some(i));
        }

        assert_eq!(rx.recv(), None);
        println!("{:?}", rx.stats());
    }
}
