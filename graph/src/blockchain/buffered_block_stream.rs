use crate::prelude::{Pin};
use crate::prelude::tokio::sync::mpsc;
use crate::blockchain::{Blockchain, BlockStream};
use crate::blockchain::block_stream::BlockStreamEvent;
use futures::Stream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::{Stream as TokioStream};
use std::task::{Context, Poll};
use futures03::stream::{StreamExt};
use futures03::compat::{Future01CompatExt, Sink01CompatExt, Stream01CompatExt, Compat01As03};
use std::sync::Arc;
use crate::task_spawn;


pub struct BufferedBlockStream<C: Blockchain> {
    source: Arc<Compat01As03<BlockStream<C>>>,
    sender: Sender<BlockStreamEvent<C>>,
    receiver: Receiver<BlockStreamEvent<C>>,
    started: bool,
}

impl<C> BufferedBlockStream<C>  where C: Blockchain {
    pub fn new(source: BlockStream<C>) -> Self {
        let (tx, rx) = mpsc::channel(4);
        BufferedBlockStream {
            source: Arc::new(source.compat()),
            sender: tx,
            receiver: rx,
            started: false,
        }
    }

    fn start(&mut self) {
        let mut tx = self.sender.clone();
        // println!("starting with channel cap {}", tx.capacity());
        self.started = true;
        let mut s = self.source.clone();
        // task_spawn::spawn(async {
        //     while let Some(Ok(block)) = s.next().await {
        //         if let Err(e) = tx.send(block).await {
        //             println!("error: {}", e);
        //             return;
        //         }
        //     }
        // });
    }
}

impl<C> TokioStream for BufferedBlockStream<C> where C: Blockchain {
    type Item = BlockStreamEvent<C>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<BlockStreamEvent<C>>> {
        if !self.started {
            self.start();
        }
        println!("Polling next");
        let ret = self.receiver.poll_recv(cx);
        println!("Polling done");
        ret
    }
}

