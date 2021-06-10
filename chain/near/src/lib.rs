use std::path::PathBuf;
use tokio::sync::mpsc;

//use configs::{init_logging, Opts, SubCommand};
use near_indexer;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn init_logging() {
    let env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,near=error,stats=info,telemetry=info,indexer_example=info,indexer=info,near-performance-metrics=info",
    );
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

async fn listen_blocks(mut stream: mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        info!(
            target: "indexer_example",
            "#{} {} Shards: {}, Transactions: {}, Receipts: {}, ExecutionOutcomes: {}",
            streamer_message.block.header.height,
            streamer_message.block.header.hash,
            streamer_message.shards.len(),
            streamer_message.shards.iter().map(|shard| if let Some(chunk) = &shard.chunk { chunk.transactions.len() } else { 0usize }).sum::<usize>(),
            streamer_message.shards.iter().map(|shard| if let Some(chunk) = &shard.chunk { chunk.receipts.len() } else { 0usize }).sum::<usize>(),
            streamer_message.shards.iter().map(|shard| shard.receipt_execution_outcomes.len()).sum::<usize>(),
        );
    }
}

pub struct NearIndexer {
    homedir: PathBuf,
}

impl NearIndexer {
    pub fn new(homedir: PathBuf) -> Self {
        openssl_probe::init_ssl_cert_env_vars();
        init_logging();
        Self { homedir }
    }

    pub fn init(&self) {
        let config = near_indexer::InitConfigArgs {
            chain_id: Some("testnet".to_string()),
            account_id: None,
            test_seed: None,
            num_shards: 1,
            fast: false,
            genesis: None,
            download: true,
            download_genesis_url: None,
        };
        near_indexer::indexer_init_configs(&self.homedir, config.into())
    }

    pub fn run(&self) {
        let indexer_config = near_indexer::IndexerConfig {
            home_dir: self.homedir.clone(),
            sync_mode: near_indexer::SyncModeEnum::FromInterruption,
            await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
        };
        let system = actix::System::new();
        system.block_on(async move {
            let indexer = near_indexer::Indexer::new(indexer_config);
            let stream = indexer.streamer();
            actix::spawn(listen_blocks(stream));
        });
        system.run().unwrap();
    }
}
