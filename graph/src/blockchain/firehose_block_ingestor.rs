use std::{marker::PhantomData, sync::Arc, time::Duration};
use std::future::Future;

use crate::{
    blockchain::Block as BlockchainBlock,
    components::store::ChainStore,
    firehose::{bstream, decode_firehose_block, endpoints::FirehoseEndpoint},
    prelude::{error, info, Logger},
    util::backoff::ExponentialBackoff,
};
use anyhow::{Context, Error};
use futures03::StreamExt;
use slog::trace;
use tonic::Streaming;
use crate::blockchain::Block;

pub struct FirehoseBlockIngestor<M>
where
    M: prost::Message + BlockchainBlock + Default + 'static,
{
    ancestor_count: i32,
    chain_store: Arc<dyn ChainStore>,
    endpoint: Arc<FirehoseEndpoint>,
    logger: Logger,

    phantom: PhantomData<M>,
}

impl<M> FirehoseBlockIngestor<M>
where
    M: prost::Message + BlockchainBlock + Default + 'static,
{
    pub fn new(
        ancestor_count: i32,
        chain_store: Arc<dyn ChainStore>,
        endpoint: Arc<FirehoseEndpoint>,
        logger: Logger,
    ) -> FirehoseBlockIngestor<M> {
        FirehoseBlockIngestor {
            ancestor_count,
            chain_store,
            endpoint,
            logger,
            phantom: PhantomData {},
        }
    }

    pub async fn run(self) {
        let mut latest_cursor = self.fetch_head_cursor().await;
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));

        loop {
            info!(
                self.logger,
                "Blockstream disconnected, connecting"; "endpoint uri" => format_args!("{}", self.endpoint), "cursor" => format_args!("{}", latest_cursor),
            );

            let result = self
                .endpoint
                .clone()
                .stream_blocks(bstream::BlocksRequestV2 {
                    start_block_num: -1,
                    start_cursor: latest_cursor.clone(),
                    fork_steps: vec![
                        bstream::ForkStep::StepNew as i32,
                        bstream::ForkStep::StepUndo as i32,
                    ],
                    ..Default::default()
                })
                .await;

            match result {
                Ok(stream) => latest_cursor = self.process_blocks(latest_cursor, stream).await,
                Err(e) => {
                    error!(self.logger, "Unable to connect to endpoint: {:?}", e);
                }
            }

            // If we reach this point, we must wait a bit before retrying
            backoff.sleep_async().await;
        }
    }

    pub async fn run_backfill(self) {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));

        loop {
            let mut backfill_cursor = self.fetch_backfill_cursor().await;
            let backfill_target = self.fetch_backfill_target_block_num().await;

            if backfill_target == 0 {
                backoff.sleep_async().await;
                continue
            }

            let result = self.endpoint.clone().stream_blocks(bstream::BlocksRequestV2{
                start_block_num: 0,
                stop_block_num: backfill_target as u64,
                start_cursor: backfill_cursor.clone(),
                fork_steps: vec![
                    bstream::ForkStep::StepIrreversible as i32, //TODO: only irreversible, right?
                ],
                ..Default::default()
            }).await;

            match result {
                Ok(stream) => {
                    backfill_cursor = self.process_backfill_blocks(backfill_cursor, stream).await
                },
                Err(e) => {
                    error!(self.logger, "Unable to connect to backfill endpoint: {:?}", e)
                }
            }

            // If we reach this point, we must wait a bit before retrying
            backoff.sleep_async().await;
        }
    }


    async fn fetch_head_cursor(&self) -> String {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().chain_head_cursor() {
                Ok(cursor) => return cursor.unwrap_or_else(|| "".to_string()),
                Err(e) => {
                    error!(self.logger, "Fetching chain head cursor failed: {:?}", e);

                    backoff.sleep_async().await;
                }
            }
        }
    }

    async fn fetch_backfill_cursor(&self) -> String {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().chain_backfill_cursor() {
                Ok(cursor) => return cursor.unwrap_or_else(|| "".to_string()),
                Err(e) => {
                    error!(self.logger, "Fetching chain backfill cursor failed: {:?}", e);

                    backoff.sleep_async().await;
                }
            }
        }
    }

    async fn fetch_backfill_target_block_num(&self) -> i64 {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().chain_backfill_target_block_num() {
                Ok(ptr_opt) => {
                    match ptr_opt {
                        None => { return 0 }
                        Some(block_num) => {
                            return block_num
                        }
                    }
                },
                Err(e) => {
                    error!(self.logger, "Fetching chain backfill target failed: {:?}", e);

                    backoff.sleep_async().await;
                }
            }
        }
    }

    async fn set_backfill_target_block_num(&self, block_num: u64) {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(30));
        loop {
            match self.chain_store.clone().set_chain_backfill_target_block_num(block_num as i64) {
                Ok(()) => {
                    return
                },
                Err(e) => {
                    error!(self.logger, "Setting chain backfill target failed: {:?}", e);

                    backoff.sleep_async().await;
                }
            }
        }
    }

    async fn process_blocks(
        &self,
        cursor: String,
        mut stream: Streaming<bstream::BlockResponseV2>,
    ) -> String {
        let mut latest_cursor = cursor;

        //TODO: do this in the backfill methods, and check blocks table for oldest block
        let mut update_backfill_target_block = false;
        let backfill_target_res = self.chain_store.clone().chain_backfill_target_block_num();
        match backfill_target_res {
            Ok(v) => {
                match v {
                    None => {
                        update_backfill_target_block = true;
                    }
                    Some(_) => {}
                }
            }
            Err(_) => {}
        }

        while let Some(message) = stream.next().await {
            match message {
                Ok(v) => {
                    match self.process_block(&v).await {
                        Ok(v) => {
                            match v {
                                None => {}
                                Some(block) => {
                                    if update_backfill_target_block {
                                        self.set_backfill_target_block_num(block.number() as u64);
                                        update_backfill_target_block = false;
                                    }

                                }
                            }
                        }
                        Err(e) => {
                            error!(self.logger, "Process block failed: {:?}", e);
                            break;
                        }
                    }

                    latest_cursor = v.cursor;
                }
                Err(e) => {
                    info!(
                        self.logger,
                        "An error occurred while streaming blocks: {}", e
                    );
                    break;
                }
            }
        }

        error!(
            self.logger,
            "Stream blocks complete unexpectedly, expecting stream to always stream blocks"
        );
        latest_cursor
    }

    async fn process_block(&self, response: &bstream::BlockResponseV2) -> Result<Option<Arc<dyn Block>>, Error> {
        let block = decode_firehose_block::<M>(response)
            .context("Mapping firehose block to blockchain::Block")?;

        trace!(self.logger, "Received block to ingest {}", block.ptr());

        self.chain_store
            .clone()
            .upsert_block(block.clone())
            .await
            .context("Inserting blockchain::Block in chain store")?;

        self.chain_store
            .clone()
            .attempt_chain_head_update(self.ancestor_count)
            .await
            .context("Updating chain head update")?;

        self.chain_store
            .clone()
            .set_chain_head_cursor(response.cursor.clone())
            .await
            .context("Updating chain head cursor")?;

        Ok(Some(block))
    }

    async fn process_backfill_blocks(
        &self,
        cursor: String,
        mut stream: Streaming<bstream::BlockResponseV2>,
    ) -> String {
        let mut latest_cursor = cursor;

        while let Some(message) = stream.next().await {
            match message {
                Ok(v) => {
                    if let Err(e) = self.process_backfill_block(&v).await {
                        error!(self.logger, "Process block failed: {:?}", e);
                        break;
                    }

                    latest_cursor = v.cursor;
                }
                Err(e) => {
                    info!(
                        self.logger,
                        "An error occurred while streaming blocks: {}", e
                    );
                    break;
                }
            }
        }

        error!(
            self.logger,
            "Stream blocks complete unexpectedly, expecting stream to always stream blocks"
        );
        latest_cursor
    }

    async fn process_backfill_block(&self, response: &bstream::BlockResponseV2) -> Result<(), Error> {
        let block = decode_firehose_block::<M>(response)
            .context("Mapping firehose block to blockchain::Block")?;

        trace!(self.logger, "Received block to ingest in backfill {}", block.ptr());

        self.chain_store
            .clone()
            .upsert_block(block.clone())
            .await
            .context("Inserting blockchain::Block in chain store")?;

        self.chain_store
            .clone()
            .set_chain_backfill_cursor(response.cursor.clone())
            .await
            .context("Updating chain backfill cursor")?;

        Ok(())
    }
}
