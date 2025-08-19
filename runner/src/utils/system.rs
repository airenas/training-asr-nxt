use anyhow::Error;
use crossbeam_channel::{bounded, select, Sender};
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    iterator::Signals,
};
use std::thread;

use crate::data::structs::FileMeta;

pub fn setup_signal_handlers() -> crossbeam_channel::Receiver<()> {
    tracing::debug!("Setting up signal handlers");
    let (cancel_tx, cancel_rx) = bounded::<()>(2);

    thread::spawn(move || {
        let mut signals = Signals::new([SIGINT, SIGTERM]).unwrap();
        for signal in &mut signals {
            match signal {
                SIGINT => {
                    tracing::info!("Exit event SIGINT");
                    break;
                }
                SIGTERM => {
                    tracing::info!("Exit event SIGTERM");
                    break;
                }
                _ => {}
            }
        }
        tracing::debug!("sending exit event");
        if let Err(e) = cancel_tx.send(()) {
            tracing::error!("Failed to send cancel signal: {}", e);
        }
    });
    cancel_rx
}

pub fn join_threads(handles: Vec<thread::JoinHandle<anyhow::Result<u16>>>) -> anyhow::Result<()> {
    tracing::debug!("Joining threads");
    let mut err: Option<anyhow::Error> = None;
    for h in handles {
        let join_res = h.join();
        match join_res {
            Ok(res) => match res {
                Ok(index) => tracing::debug!("Worker {} finished", index),
                Err(e) => {
                    err = Some(e);
                }
            },
            Err(e) => {
                err = Some(join_err_to_anyhow(e));
            }
        }
    }
    if let Some(err) = err {
        Err(err)
    } else {
        Ok(())
    }
}

pub fn setup_send_files(
    files: Vec<FileMeta>,
    tx: Sender<FileMeta>,
    cancel_rx: crossbeam_channel::Receiver<()>,
) -> anyhow::Result<()> {
    // producer
    thread::spawn({
        let cancel_rx = cancel_rx.clone();
        move || {
            for f in files {
                select! {
                        send(tx, f) -> res => {
                    if res.is_err() {
                        break;
                    }
                }
                        recv(cancel_rx) -> _ => {
                        break;
                    }
                    }
            }
            tracing::trace!("sender exit");
        }
    });
    Ok(())
}

fn join_err_to_anyhow(e: Box<dyn std::any::Any + Send>) -> Error {
    if let Some(s) = e.downcast_ref::<&str>() {
        anyhow::anyhow!("Thread panicked: {}", s)
    } else if let Some(s) = e.downcast_ref::<String>() {
        anyhow::anyhow!("Thread panicked: {}", s)
    } else {
        anyhow::anyhow!("Thread panicked with unknown type")
    }
}
