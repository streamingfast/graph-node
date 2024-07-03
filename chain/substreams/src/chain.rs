use crate::block_ingestor::SubstreamsBlockIngestor;
use crate::{data_source::*, EntityChanges, TriggerData, TriggerFilter, TriggersAdapter};
use anyhow::Error;
use graph::blockchain::client::ChainClient;
use graph::blockchain::{
    BasicBlockchainBuilder, BlockIngestor, BlockTime, EmptyNodeCapabilities, NoopDecoderHook,
    ChainIdentifier, HostFn,
};
use graph_runtime_wasm::asc_abi::class::{AscEnumArray, EthereumValueKind };
use graph::runtime::{AscPtr, HostExportError};
use graph::components::store::DeploymentCursorTracker;
use graph::env::EnvVars;
use graph::firehose::FirehoseEndpoints;
use graph::prelude::{BlockHash, CheapClone, Entity, LoggerFactory, MetricsRegistry};
use graph::schema::EntityKey;
use graph::{
    blockchain::{
        self,
        block_stream::{BlockStream, BlockStreamBuilder, FirehoseCursor},
        BlockPtr, Blockchain, BlockchainKind, IngestorError, RuntimeAdapter as RuntimeAdapterTrait,
    },
    components::store::DeploymentLocator,
    data::subgraph::UnifiedMappingApiVersion,
    prelude::{async_trait, BlockNumber, ChainStore},
    slog::Logger,
};

use std::sync::Arc;

// ParsedChanges are an internal representation of the equivalent operations defined on the
// graph-out format used by substreams.
// Unset serves as a sentinel value, if for some reason an unknown value is sent or the value
// was empty then it's probably an unintended behaviour. This code was moved here for performance
// reasons, but the validation is still performed during trigger processing so while Unset will
// very likely just indicate an error somewhere, as far as the stream is concerned we just pass
// that along and let the downstream components deal with it.
#[derive(Debug, Clone)]
pub enum ParsedChanges {
    Unset,
    Delete(EntityKey),
    Upsert { key: EntityKey, entity: Entity },
}

#[derive(Default, Debug, Clone)]
pub struct Block {
    pub hash: BlockHash,
    pub number: BlockNumber,
    pub changes: EntityChanges,
    pub parsed_changes: Vec<ParsedChanges>,
}

impl blockchain::Block for Block {
    fn ptr(&self) -> BlockPtr {
        BlockPtr {
            hash: self.hash.clone(),
            number: self.number,
        }
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        None
    }

    fn timestamp(&self) -> BlockTime {
        BlockTime::NONE
    }
}

pub struct Chain {
    chain_store: Arc<dyn ChainStore>,
    block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,

    pub(crate) logger_factory: LoggerFactory,
    pub(crate) client: Arc<ChainClient<Self>>,
    pub(crate) metrics_registry: Arc<MetricsRegistry>,
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        firehose_endpoints: FirehoseEndpoints,
        metrics_registry: Arc<MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
    ) -> Self {
        Self {
            logger_factory,
            client: Arc::new(ChainClient::new_firehose(firehose_endpoints)),
            metrics_registry,
            chain_store,
            block_stream_builder,
        }
    }
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: substreams")
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Substreams;

    type Client = ();
    type Block = Block;
    type DataSource = DataSource;
    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = NoopDataSourceTemplate;
    type UnresolvedDataSourceTemplate = NoopDataSourceTemplate;

    /// Trigger data as parsed from the triggers adapter.
    type TriggerData = TriggerData;

    /// Decoded trigger ready to be processed by the mapping.
    /// New implementations should have this be the same as `TriggerData`.
    type MappingTrigger = TriggerData;

    /// Trigger filter used as input to the triggers adapter.
    type TriggerFilter = TriggerFilter;

    type NodeCapabilities = EmptyNodeCapabilities<Self>;

    type DecoderHook = NoopDecoderHook;

    fn triggers_adapter(
        &self,
        _log: &DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn blockchain::TriggersAdapter<Self>>, Error> {
        Ok(Arc::new(TriggersAdapter {}))
    }

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        store: impl DeploymentCursorTracker,
        _start_blocks: Vec<BlockNumber>,
        filter: Arc<Self::TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        self.block_stream_builder
            .build_substreams(
                self,
                store.input_schema(),
                deployment,
                store.firehose_cursor(),
                store.block_ptr(),
                filter,
            )
            .await
    }

    fn is_refetch_block_required(&self) -> bool {
        false
    }
    async fn refetch_firehose_block(
        &self,
        _logger: &Logger,
        _cursor: FirehoseCursor,
    ) -> Result<Block, Error> {
        unimplemented!("This chain does not support Dynamic Data Sources. is_refetch_block_required always returns false, this shouldn't be called.")
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        _logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        // This is the same thing TriggersAdapter does, not sure if it's going to work but
        // we also don't yet have a good way of getting this value until we sort out the
        // chain store.
        // TODO(filipe): Fix this once the chain_store is correctly setup for substreams.
        Ok(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number,
        })
    }

    fn runtime(&self) -> (Arc<dyn RuntimeAdapterTrait<Self>>, Self::DecoderHook) {
        let chain_identifier = self.chain_store.chain_identifier().clone();

        let runtime_adapter = Arc::new(RuntimeAdapter {
            chain_identifier,
        });

        (runtime_adapter, NoopDecoderHook)
        // Ok((Arc::new(NoopRuntimeAdapter::default()), NoopDecoderHook))
    }

    fn chain_client(&self) -> Arc<ChainClient<Self>> {
        self.client.clone()
    }

    fn block_ingestor(&self) -> anyhow::Result<Box<dyn BlockIngestor>> {
        Ok(Box::new(SubstreamsBlockIngestor::new(
            self.chain_store.cheap_clone(),
            self.client.cheap_clone(),
            self.logger_factory.component_logger("", None),
            "substreams".to_string(),
            self.metrics_registry.cheap_clone(),
        )))
    }
}

pub struct RuntimeAdapter {
    pub chain_identifier: ChainIdentifier,
}

#[async_trait]
impl RuntimeAdapterTrait<Chain> for RuntimeAdapter {

    fn host_fns(&self, ds: &DataSource) -> Result<Vec<HostFn>, Error> {
        //let abis = ds.mapping.abis.clone();
        //let call_cache = self.call_cache.cheap_clone();
        //let eth_adapters = self.eth_adapters.cheap_clone();
        //let archive = ds.mapping.requires_archive()?;
        //let eth_call_gas = eth_call_gas(&self.chain_identifier);

        let ethereum_call = HostFn {
            name: "ethereum.call",
            func: Arc::new(move |ctx, wasm_ptr| {
                ethereum_call(
     //               &eth_adapter,
     //               call_cache.cheap_clone(),
     //               ctx,
     //               wasm_ptr,
     //               &abis,
     //               eth_call_gas,
                )
                .map(|ptr| ptr.wasm_ptr())
            }),
        };

        //let eth_adapters = self.eth_adapters.cheap_clone();
        //let ethereum_get_balance = HostFn {
        //    name: "ethereum.getBalance",
        //    func: Arc::new(move |ctx, wasm_ptr| {
        //        let eth_adapter = eth_adapters.unverified_cheapest_with(&NodeCapabilities {
        //            archive,
        //            traces: false,
        //        })?;
        //        eth_get_balance(&eth_adapter, ctx, wasm_ptr).map(|ptr| ptr.wasm_ptr())
        //    }),
        //};

        //let eth_adapters = self.eth_adapters.cheap_clone();
        //let ethereum_get_code = HostFn {
        //    name: "ethereum.hasCode",
        //    func: Arc::new(move |ctx, wasm_ptr| {
        //        let eth_adapter = eth_adapters.unverified_cheapest_with(&NodeCapabilities {
        //            archive,
        //            traces: false,
        //        })?;
        //        eth_has_code(&eth_adapter, ctx, wasm_ptr).map(|ptr| ptr.wasm_ptr())
        //    }),
        //};

        Ok(vec![ethereum_call])
        //Ok(vec![ethereum_call, ethereum_get_balance, ethereum_get_code])
    }


}


fn ethereum_call(
//    //eth_adapter: &EthereumAdapter,
//    //call_cache: Arc<dyn EthereumCallCache>,
//    //ctx: HostFnCtx,
//    //wasm_ptr: u32,
//    //abis: &[Arc<MappingABI>],
//    //eth_call_gas: Option<u32>,
) -> Result<AscEnumArray<EthereumValueKind>, HostExportError> {
//    //ctx.gas
//    //    .consume_host_fn_with_metrics(ETHEREUM_CALL, "ethereum_call")?;
//
    panic!("Not implemented");
//    Ok(AscPtr::null())
//
//    //// For apiVersion >= 0.0.4 the call passed from the mapping includes the
//    //// function signature; subgraphs using an apiVersion < 0.0.4 don't pass
//    //// the signature along with the call.
//    //let call: UnresolvedContractCall = if ctx.heap.api_version() >= Version::new(0, 0, 4) {
//    //    asc_get::<_, AscUnresolvedContractCall_0_0_4, _>(ctx.heap, wasm_ptr.into(), &ctx.gas, 0)?
//    //} else {
//    //    asc_get::<_, AscUnresolvedContractCall, _>(ctx.heap, wasm_ptr.into(), &ctx.gas, 0)?
//    //};
//
//    //let result = eth_call(
//    //    eth_adapter,
//    //    call_cache,
//    //    &ctx.logger,
//    //    &ctx.block_ptr,
//    //    call,
//    //    abis,
//    //    eth_call_gas,
//    //    ctx.metrics.cheap_clone(),
//    //)?;
//    //match result {
//    //    Some(tokens) => Ok(asc_new(ctx.heap, tokens.as_slice(), &ctx.gas)?),
//    //    None => Ok(AscPtr::null()),
//    //}
}

#[async_trait]
impl blockchain::BlockchainBuilder<super::Chain> for BasicBlockchainBuilder {
    fn build(self, _config: &Arc<EnvVars>) -> Chain {
        let BasicBlockchainBuilder {
            logger_factory,
            name: _,
            chain_store,
            firehose_endpoints,
            metrics_registry,
        } = self;

        Chain {
            chain_store,
            block_stream_builder: Arc::new(crate::BlockStreamBuilder::new()),
            logger_factory,
            client: Arc::new(ChainClient::new_firehose(firehose_endpoints)),
            metrics_registry,
        }
    }
}
