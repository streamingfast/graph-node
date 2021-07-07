use super::Blockchain;
use crate::blockchain::block_stream::{
    BlockStreamContext, BlockStreamEvent, BlockWithTriggers, NextBlocks,
};
use crate::blockchain::ChainHeadUpdateStream;
use anyhow::Error;
use futures03::{
    stream::{Stream, StreamExt},
    Future, FutureExt,
};
use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::fs::read;

pub enum BlockStreamState<C>
where
    C: Blockchain,
{
    /// Starting or restarting reconciliation.
    ///
    /// Valid next states: Reconciliation
    BeginReconciliation,

    /// The BlockStream is reconciling the subgraph store state with the chain store state.
    ///
    /// Valid next states: YieldingBlocks, Idle, BeginReconciliation (in case of revert)
    Reconciliation(Pin<Box<dyn Future<Output = Result<NextBlocks<C>, Error>> + Send>>),

    /// The BlockStream is emitting blocks that must be processed in order to bring the subgraph
    /// store up to date with the chain store.
    ///
    /// Valid next states: BeginReconciliation
    YieldingBlocks(Box<VecDeque<BlockWithTriggers<C>>>),

    /// The BlockStream experienced an error and is pausing before attempting to produce
    /// blocks again.
    ///
    /// Valid next states: BeginReconciliation
    RetryAfterDelay(Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>),

    /// The BlockStream has reconciled the subgraph store and chain store states.
    /// No more work is needed until a chain head update.
    ///
    /// Valid next states: BeginReconciliation
    Idle,

    /// Not a real state, only used when going from one state to another.
    Transition,
}

pub struct BlockStream<C: Blockchain> {
    state: BlockStreamState<C>,
    // consecutive_err_count: u32,
    // chain_head_update_stream: ChainHeadUpdateStream,
    ctx: BlockStreamContext<C>,
}

impl<C> Stream for BlockStream<C>
where
    C: Blockchain,
{
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {

        let result = loop {
            match &mut self.state {
                BlockStreamState::BeginReconciliation => {
                    // Start the reconciliation process by asking for blocks
                    let ctx = self.ctx.clone();
                    let fut = async move { ctx.next_blocks().await };
                    // let fut = async move { next_blocks().await };
                    self.state = BlockStreamState::Reconciliation(fut.boxed());
                }
                BlockStreamState::Reconciliation(next_blocks_future) => {
                    match next_blocks_future.poll_unpin(cx) {
                        Poll::Ready(Ok(NextBlocks::Blocks(next_blocks, block_range_size))) => {}
                        Poll::Ready(Ok(NextBlocks::Done)) => {}
                        Poll::Ready(Ok(NextBlocks::Revert(block))) => {}
                        Poll::Ready(Err(e)) => {}
                        Poll::Pending => {}
                    }
                }
                BlockStreamState::Idle => {}
                BlockStreamState::RetryAfterDelay(_) => {}
                BlockStreamState::Transition => unreachable!(),
                _ => {}
            }
        };
    }
}
