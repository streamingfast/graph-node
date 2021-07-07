use super::Blockchain;
use crate::blockchain::block_stream::{
    BlockStreamContext, BlockStreamEvent, BlockWithTriggers, NextBlocks,
};
use crate::blockchain::ChainHeadUpdateStream;
use anyhow::Error;
use futures03::{
    stream::{Stream},
    Future, FutureExt,
};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use slog::debug;

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
    consecutive_err_count: u32,
    chain_head_update_stream: ChainHeadUpdateStream,
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
                // Waiting for the reconciliation to complete or yield blocks
                BlockStreamState::Reconciliation(next_blocks_future) => {
                    match next_blocks_future.poll_unpin(cx) {
                        Poll::Ready(Ok(NextBlocks::Blocks(next_blocks, block_range_size))) => {
                            // We had only one error, so we infer that reducing the range size is
                            // what fixed it. Reduce the max range size to prevent future errors.
                            // See: 018c6df4-132f-4acc-8697-a2d64e83a9f0
                            if self.consecutive_err_count == 1 {
                                // Reduce the max range size by 10%, but to no less than 10.
                                self.ctx.max_block_range_size =
                                    (self.ctx.max_block_range_size * 9 / 10).max(10);
                            }
                            self.consecutive_err_count = 0;

                            let total_triggers =
                                next_blocks.iter().map(|b| b.trigger_count()).sum::<usize>();
                            self.ctx.previous_triggers_per_block =
                                total_triggers as f64 / block_range_size as f64;
                            self.ctx.previous_block_range_size = block_range_size;
                            if total_triggers > 0 {
                                debug!(self.ctx.logger, "Processing {} triggers", total_triggers);
                            }

                            // Switch to yielding state until next_blocks is depleted
                            self.state = BlockStreamState::YieldingBlocks(Box::new(next_blocks));

                            // Yield the first block in next_blocks
                            continue;
                        }
                        // Reconciliation completed. We're caught up to chain head.
                        Poll::Ready(Ok(NextBlocks::Done)) => {
                            // Reset error count
                            self.consecutive_err_count = 0;

                            // Switch to idle
                            self.state = BlockStreamState::Idle;

                            // Poll for chain head update
                            continue;
                        }
                        Poll::Ready(Ok(NextBlocks::Revert(block))) => {
                            self.state = BlockStreamState::BeginReconciliation;
                            break Ok(Poll::Ready(Some(BlockStreamEvent::Revert(block))));
                        }
                        Poll::Pending => {
                            // Nothing to change or yield yet.
                            self.state = BlockStreamState::Reconciliation(Box::pin(next_blocks_future));
                            break Ok(Poll::Pending); //todo: is this ok

                        }
                        Poll::Ready(Err(e)) => {
                            //todo: to be implemented ...
                            break Err(e);

                        }
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
