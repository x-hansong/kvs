use std::thread;
use crate::thread_pool::ThreadPool;
use crate::Result;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use log::{info, error};
use std::panic;
use std::panic::AssertUnwindSafe;

type Job = Box<dyn FnOnce() + Send + 'static>;


pub struct SharedQueueThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>
}

enum Message {
    NewJob(Job),
    Terminate
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();
            match message {
                Message::NewJob(job) => {
                    info!("Worker {} receive new job", id);
                    let result = panic::catch_unwind(AssertUnwindSafe(|| {
                        job()
                    }));
                    if result.is_err() {
                        error!("Worker {} execute error", id);
                    }
                },
                Message::Terminate => {
                    info!("Worker {} was told to terminate", id);
                    break
                }
            }
        });

        Worker {
            id, thread: Some(thread)
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> where
        Self: Sized {
        let (sender, receiver) = mpsc::channel::<Message>();
        let mut workers = Vec::with_capacity(threads as usize);
        let receiver = Arc::new(Mutex::new(receiver));
        for i in 0..threads {
            workers.push(Worker::new(i as usize, Arc::clone(&receiver)));
        }

        Ok(SharedQueueThreadPool {
            workers,
            sender
        })

    }

    fn spawn<F>(&self, job: F) where
        F: FnOnce() + Send + 'static {
        let job = Box::new(job);
        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        info!("Sending terminate message to all workers.");
        for _ in &mut self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }
        info!("Shutting down all workers.");

        for worker in &mut self.workers {
            info!("Shutting down worker {}", worker.id);
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

