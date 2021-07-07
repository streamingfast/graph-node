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
    Reconciliation(Box<dyn Future<Output = Result<NextBlocks<C>, Error>> + Send>),

    /// The BlockStream is emitting blocks that must be processed in order to bring the subgraph
    /// store up to date with the chain store.
    ///
    /// Valid next states: BeginReconciliation
    YieldingBlocks(VecDeque<BlockWithTriggers<C>>),

    /// The BlockStream experienced an error and is pausing before attempting to produce
    /// blocks again.
    ///
    /// Valid next states: BeginReconciliation
    RetryAfterDelay(Box<dyn Future<Output = Result<(), Error>> + Send>),

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
    consecutive_err_count: u32,
    chain_head_update_stream: ChainHeadUpdateStream,
    ctx: BlockStreamContext<C>,
}

impl<C> Stream for BlockStream<C>
where
    C: Blockchain,
{
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = BlockStreamState::Transition;
        mem::swap(&mut self.state, &mut state);

        let result = loop {
            match state {
                BlockStreamState::BeginReconciliation => {
                    // Start the reconciliation process by asking for blocks
                    let ctx = self.ctx.clone();
                    let fut = async move { ctx.next_blocks().await };
                    state = BlockStreamState::Reconciliation(Box::new(fut.boxed()));
                }
                BlockStreamState::Reconciliation(mut next_blocks_future) => {
                    match next_blocks_future.poll(cx.clone()) {
                        Poll::Ready(Ok(NextBlocks::Blocks(next_blocks, block_range_size))) => {}
                        Poll::Ready(Ok(NextBlocks::Done)) => {}
                        Poll::Ready(Ok(NextBlocks::Revert(block))) => {}
                        Poll::Ready(Err(e)) => {}
                        Poll::Pending => {}
                    }
                }

                // BlockStreamState::YieldingBlocks(mut next_blocks) => {}
                //
                // BlockStreamState::RetryAfterDelay(mut delay) => {}

                // Waiting for a chain head update
                BlockStreamState::Idle => {}

                // This will only happen if this poll function fails to complete normally then is
                // called again.
                BlockStreamState::Transition => unreachable!(),
            }
        };
    }
}
