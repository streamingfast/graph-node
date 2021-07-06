use graph_node::manager::commands::chain::info;
use futures::future::join_all;
use std::iter::FromIterator;
use graph::log::factory::LoggerFactory;
use graph::slog::Logger;
use std::sync::Arc;
use graph_chain_ethereum::{EthereumNetworks, ProviderEthRpcMetrics, Transport};
use graph::prelude::{anyhow, Duration, DeploymentHash};
use graph::components::metrics::Registry;
use graph::components::store::{BlockStore, DeploymentLocator, DeploymentId};
use graph_core::{
    three_box::ThreeBoxAdapter, LinkResolver, MetricsRegistry,
    SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider, SubgraphInstanceManager,
    SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph::components::ethereum::{NodeCapabilities, EthereumNetworkIdentifier};
use graph_node::opt::Opt;
use std::collections::{BTreeSet, BTreeMap, HashMap};
use graph::data::store::NodeId;
use lazy_static::lazy_static;
use std::str::FromStr;
use graph::cheap_clone::CheapClone;
use std::ops::Deref;
use graph::blockchain::block_stream::{BlockStreamMetrics, BlockStreamEvent};
use graph::components::metrics::stopwatch::StopwatchMetrics;
use graph::data::subgraph::{Source, UnifiedMappingApiVersion, Mapping, Link, MappingABI};
use graph::ext::futures::{CancelableError, CancelGuard};
use graph::semver::Version;
use graph::blockchain::TriggerFilter;
use graph::blockchain::Blockchain;
use graph::prelude::*;
use graph::prelude::ethabi::{Address, Contract};

const ETH_NET_VERSION_WAIT_TIME: Duration = Duration::from_secs(30);

pub type BlockNumber = i32;


lazy_static! {
    // Default to an Ethereum reorg threshold to 50 blocks
    static ref REORG_THRESHOLD: BlockNumber = std::env::var("ETHEREUM_REORG_THRESHOLD")
        .ok()
        .map(|s| BlockNumber::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_REORG_THRESHOLD")))
        .unwrap_or(50);

    // Default to an ancestor count of 50 blocks
    static ref ANCESTOR_COUNT: BlockNumber = std::env::var("ETHEREUM_ANCESTOR_COUNT")
        .ok()
        .map(|s| BlockNumber::from_str(&s)
             .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_ANCESTOR_COUNT")))
        .unwrap_or(50);
}


#[tokio::main]
async fn main() {
    println!("node test example");

    let opt = graph_node::opt::Opt {
        config: None,
        check_config: false,
        subgraph: None,
        postgres_url: Some("postgresql://postgres:empty@localhost:5432/graph-node?options=-c%20enable_incremental_sort%3Doff".to_owned()),
        postgres_secondary_hosts: vec![],
        postgres_host_weights: vec![],
        ethereum_rpc: vec!["bsc:http://localhost:8545".to_owned()],
        ethereum_ws: vec![],
        ethereum_ipc: vec![],
        ipfs: vec![],
        http_port: 0,
        index_node_port: 0,
        ws_port: 0,
        admin_port: 0,
        metrics_port: 0,
        node_id: "a1b2c3".to_string(),
        debug: true,
        elasticsearch_url: None,
        elasticsearch_user: None,
        elasticsearch_password: None,
        ethereum_polling_interval: 0,
        disable_block_ingestor: false,
        store_connection_pool_size: 2,
        network_subgraphs: vec![],
        arweave_api: "".to_string(),
        three_box_api: "".to_string(),
    };

    // Set up logger
    let logger = graph::log::logger(opt.debug);

    info!(logger, "Setting up example");

    let config = match graph_node::config::Config::load(&logger, &opt.clone().into()) {
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
        Ok(config) => config,
    };

    let node_id =
        NodeId::new(opt.node_id.clone()).expect("Node ID must contain only a-z, A-Z, 0-9, and '_'");


    // Create a component and subgraph logger factory
    let logger_factory = LoggerFactory::new(logger.clone(), None);

    // Set up Prometheus registry
    let prometheus_registry = Arc::new(Registry::new());
    let metrics_registry = Arc::new(MetricsRegistry::new(
        logger.clone(),
        prometheus_registry.clone(),
    ));


    let eth_networks = create_ethereum_networks(logger.clone(), metrics_registry.clone(), config.clone())
        .await
        .expect("Failed to parse Ethereum networks");

    let store_builder =
        graph_node::store_builder::StoreBuilder::new(&logger, &node_id, &config, metrics_registry.cheap_clone()).await;

    let (eth_networks, idents) = connect_networks(&logger, eth_networks).await;

    let chain_head_update_listener = store_builder.chain_head_update_listener();
    let network_store = store_builder.network_store(idents);

    let chains = Arc::new(networks_as_chains(
        &logger,
        node_id.clone(),
        metrics_registry.clone(),
        &eth_networks,
        network_store.as_ref(),
        chain_head_update_listener.clone(),
        &logger_factory,
    ));
    let network = "bsc".to_string();

    let chain = chains.get("bsc")
        .with_context(|| format!("no chain configured for network {}", network))
        .unwrap();

    println!("test should be running");
    println!("found chain configuration {:?}", chain);

    let deployment_locator = DeploymentLocator {
        id: DeploymentId(2),
        hash: DeploymentHash::new("QmekP583qkqbkhx54kyZC3pviSqoAdjcx94sme1mZ9shv1".to_string()).unwrap(),
    };

    let datasources = vec![mock_data_source("/Users/julien/codebase/graphprotocol/graph-node/runtime/test/wasm_test/string_to_number.wasm")];
    let filter = graph_chain_ethereum::TriggerFilter::from_data_sources(datasources.iter());
    let stopwatch_metrics = StopwatchMetrics::new(logger.clone(), deployment_locator.hash.clone(), metrics_registry.clone());
    let block_stream_metrics = Arc::new(BlockStreamMetrics::new(
        metrics_registry.clone(),
        &deployment_locator.hash,
        "bsc".to_string(),
        stopwatch_metrics.clone(),
    ));
    let version_vec = vec![Version::parse("0.1.0").unwrap()];
    let unified_api_version = UnifiedMappingApiVersion::try_from_versions(version_vec.iter()).unwrap();

    let block_stream_canceler = CancelGuard::new();
    let mut block_stream = chain.new_block_stream(
        deployment_locator.clone(),
        vec![10].clone(),
        filter.clone(),
        block_stream_metrics.clone(),
        unified_api_version.clone(),
    ).unwrap()
        .map_err(CancelableError::Error)
        .cancelable(&block_stream_canceler, || CancelableError::Cancel)
        .compat();

    let subgraph_store = network_store.subgraph_store();
    let writable_subgraph_store = subgraph_store.writable(&deployment_locator).unwrap();
    println!("starting created block stream ");
    loop {
        let block = match block_stream.next().await {
            Some(Ok(BlockStreamEvent::ProcessBlock(block))) => block,
            Some(Ok(BlockStreamEvent::Revert(subgraph_ptr))) => {
                info!(
                        logger,
                        "Reverting block to get back to main chain";
                        "block_number" => format!("{}", subgraph_ptr.number),
                        "block_hash" => format!("{}", subgraph_ptr.hash)
                    );
                continue;
            }
            Some(Err(e)) => {
                debug!(
                        &logger,
                        "Block stream produced a non-fatal error";
                        "error" => format!("{}", e),
                    );
                continue;
            }
            None => unreachable!("The block stream stopped producing blocks"),
        };
        let block_ptr = block.ptr();
        info!(
            logger,
            "Processing Block";
            "block_number" => format!("{}", block_ptr.number),
            "block_hash" => format!("{}", block_ptr.hash),
            "trigger_count" => format!("{}", block_ptr.hash)
        );


        let res = writable_subgraph_store.transact_block_operations(block_ptr, vec![], stopwatch_metrics.clone(), vec![], vec![]);
        match res {
            Ok(_) => (),
            Err(e) => {
                print!("Error writting to store {:?}", e)
            }
        }

    }
}

async fn create_ethereum_networks(
    logger: Logger,
    registry: Arc<MetricsRegistry>,
    config: graph_node::config::Config,
) -> Result<EthereumNetworks, anyhow::Error> {
    let eth_rpc_metrics = Arc::new(ProviderEthRpcMetrics::new(registry));
    let mut parsed_networks = EthereumNetworks::new();
    for (name, chain) in config.chains.chains {
        for provider in chain.providers {
            let capabilities = provider.node_capabilities();

            let logger = logger.new(o!("provider" => provider.label.clone()));
            info!(
                logger,
                "Creating transport";
                "url" => &provider.url,
                "capabilities" => capabilities
            );


            let (transport_event_loop, transport) = match provider.transport {
                Rpc => graph_chain_ethereum::Transport::new_rpc(&provider.url, provider.headers),
                Ipc => graph_chain_ethereum::Transport::new_ipc(&provider.url),
                Ws => graph_chain_ethereum::Transport::new_ws(&provider.url),
            };

            // If we drop the event loop the transport will stop working.
            // For now it's fine to just leak it.
            std::mem::forget(transport_event_loop);

            let supports_eip_1898 = !provider.features.contains("no_eip1898");

            parsed_networks.insert(
                name.to_string(),
                capabilities,
                Arc::new(
                    graph_chain_ethereum::EthereumAdapter::new(
                        logger,
                        provider.label,
                        &provider.url,
                        transport,
                        eth_rpc_metrics.clone(),
                        supports_eip_1898,
                    )
                        .await,
                ),
            );
        }
    }
    parsed_networks.sort();
    Ok(parsed_networks)
}


async fn connect_networks(
    logger: &Logger,
    mut eth_networks: EthereumNetworks,
) -> (
    EthereumNetworks,
    Vec<(String, Vec<EthereumNetworkIdentifier>)>,
) {
    // The status of a provider that we learned from connecting to it
    #[derive(PartialEq)]
    enum Status {
        Broken {
            network: String,
            provider: String,
        },
        Version {
            network: String,
            ident: EthereumNetworkIdentifier,
        },
    }

    // This has one entry for each provider, and therefore multiple entries
    // for each network
    let statuses = join_all(
        eth_networks
            .flatten()
            .into_iter()
            .map(|(network_name, capabilities, eth_adapter)| {
                (network_name, capabilities, eth_adapter, logger.clone())
            })
            .map(|(network, capabilities, eth_adapter, logger)| async move {
                info!(
                    logger, "Connecting to Ethereum to get network identifier";
                    "capabilities" => &capabilities
                );
                use graph_chain_ethereum::EthereumAdapterTrait;
                match tokio::time::timeout(ETH_NET_VERSION_WAIT_TIME, eth_adapter.net_identifiers())
                    .await
                    .map_err(anyhow::Error::from)
                {
                    // An `Err` means a timeout, an `Ok(Err)` means some other error (maybe a typo
                    // on the URL)
                    Ok(Err(e)) | Err(e) => {
                        Status::Broken {
                            network,
                            provider: eth_adapter.provider().to_string(),
                        }
                    }
                    Ok(Ok(ident)) => {
                        info!(
                            logger,
                            "Connected to Ethereum";
                            "network_version" => &ident.net_version,
                            "capabilities" => &capabilities
                        );
                        Status::Version { network, ident }
                    }
                }
            }),
    )
        .await;

    // Group identifiers by network name
    let idents: HashMap<String, Vec<EthereumNetworkIdentifier>> =
        statuses
            .into_iter()
            .fold(HashMap::new(), |mut networks, status| {
                match status {
                    Status::Broken { network, provider } => {
                        eth_networks.remove(&network, &provider)
                    }
                    Status::Version { network, ident } => {
                        networks.entry(network.to_string()).or_default().push(ident)
                    }
                }
                networks
            });
    let idents: Vec<_> = idents.into_iter().collect();
    (eth_networks, idents)
}

fn networks_as_chains(
    logger: &Logger,
    node_id: NodeId,
    registry: Arc<MetricsRegistry>,
    eth_networks: &EthereumNetworks,
    store: &graph_store_postgres::Store,
    chain_head_update_listener: Arc<graph_store_postgres::ChainHeadUpdateListener>,
    logger_factory: &LoggerFactory,
) -> HashMap<String, Arc<graph_chain_ethereum::Chain>> {
    let chains = eth_networks
        .networks
        .iter()
        .filter_map(|(network_name, eth_adapters)| {
            store
                .block_store()
                .chain_store(network_name)
                .map(|chain_store| {
                    let is_ingestible = chain_store.is_ingestible();
                    (network_name, eth_adapters, chain_store, is_ingestible)
                })
                .or_else(|| {
                    error!(
                        logger,
                        "No store configured for chain {}; ignoring this chain", network_name
                    );
                    None
                })
        })
        .map(|(network_name, eth_adapters, chain_store, is_ingestible)| {
            let chain = graph_chain_ethereum::Chain::new(
                logger_factory.clone(),
                network_name.clone(),
                node_id.clone(),
                registry.clone(),
                chain_store.cheap_clone(),
                chain_store,
                store.subgraph_store(),
                eth_adapters.clone(),
                chain_head_update_listener.clone(),
                *ANCESTOR_COUNT,
                *REORG_THRESHOLD,
                is_ingestible,
            );
            (network_name.clone(), Arc::new(chain))
        });
    HashMap::from_iter(chains)
}


fn mock_data_source(path: &str) -> graph_chain_ethereum::DataSource {
    let runtime = std::fs::read(path).unwrap();

    graph_chain_ethereum::DataSource {
        kind: String::from("ethereum/contract"),
        name: String::from("example data source"),
        network: Some(String::from("mainnet")),
        source: Source {
            address: Some(Address::from_str("0123123123012312312301231231230123123123").unwrap()),
            abi: String::from("123123"),
            start_block: 0,
        },
        mapping: Mapping {
            kind: String::from("ethereum/events"),
            api_version: Version::parse("0.1.0").unwrap(),
            language: String::from("wasm/assemblyscript"),
            entities: vec![],
            abis: vec![],
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![MappingBlockHandler {
                handler: "handleBlock".to_string(),
                filter: None,
            }],
            link: Link {
                link: "link".to_owned(),
            },
            runtime: Arc::new(runtime.clone()),
        },
        context: Default::default(),
        creation_block: None,
        contract_abi: Arc::new(mock_abi()),
    }
}


fn mock_abi() -> MappingABI {
    MappingABI {
        name: "mock_abi".to_string(),
        contract: Contract::load(
            r#"[
            {
                "inputs": [
                    {
                        "name": "a",
                        "type": "address"
                    }
                ],
                "type": "constructor"
            }
        ]"#
                .as_bytes(),
        ).unwrap(),
    }
}
