use graph::blockchain;
use graph::blockchain::Block;
use graph::blockchain::TriggerData;
use graph::cheap_clone::CheapClone;
use graph::prelude::hex;
use graph::prelude::web3::types::H256;
use graph::prelude::BlockNumber;
use graph::runtime::asc_new;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::DeterministicHostError;
use std::{cmp::Ordering, sync::Arc};

use crate::codec;

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for SolanaTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock<'a> {
            Block,
            Instruction { instruction: &'a codec::Instruction },
        }

        let trigger_without_block = match self {
            SolanaTrigger::Block(_) => MappingTriggerWithoutBlock::Block,
            SolanaTrigger::Instruction(instruction_with_block) => {
                MappingTriggerWithoutBlock::Instruction {
                    instruction: &instruction_with_block.instruction,
                }
            }
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl blockchain::MappingTrigger for SolanaTrigger {
    fn to_asc_ptr<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            SolanaTrigger::Block(block) => asc_new(heap, block.as_ref())?.erase(),
            SolanaTrigger::Instruction(instruction_with_block) => {
                asc_new(heap, instruction_with_block.as_ref())?.erase()
            }
        })
    }
}

#[derive(Clone)]
pub enum SolanaTrigger {
    Block(Arc<codec::Block>),
    Instruction(Arc<InstructionWithInfo>),
}

impl CheapClone for SolanaTrigger {
    fn cheap_clone(&self) -> SolanaTrigger {
        match self {
            SolanaTrigger::Block(block) => SolanaTrigger::Block(block.cheap_clone()),
            SolanaTrigger::Instruction(instruction_with_block) => {
                SolanaTrigger::Instruction(instruction_with_block.cheap_clone())
            }
        }
    }
}

impl PartialEq for SolanaTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr), Self::Block(b_ptr)) => a_ptr == b_ptr,
            (Self::Instruction(a), Self::Instruction(b)) => {
                let i = &a.instruction;
                let j = &b.instruction;

                return i.program_id == j.program_id
                    && i.ordinal == j.ordinal
                    && i.parent_ordinal == j.parent_ordinal
                    && i.depth == j.depth;
            }

            (Self::Block(_), Self::Instruction(_)) | (Self::Instruction(_), Self::Block(_)) => {
                false
            }
        }
    }
}

impl Eq for SolanaTrigger {}

impl SolanaTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            SolanaTrigger::Block(block) => block.number(),
            SolanaTrigger::Instruction(instruction_with_info) => instruction_with_info.number(),
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            SolanaTrigger::Block(block) => block.ptr().hash_as_h256(),
            SolanaTrigger::Instruction(instruction_with_block) => {
                H256::from_slice(instruction_with_block.block_id.as_slice())
            }
        }
    }
}

impl Ord for SolanaTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,

            // Block triggers always come last
            (Self::Block(..), _) => Ordering::Greater,
            (_, Self::Block(..)) => Ordering::Less,

            // We assumed the provide instructions are ordered correctly, so we say they
            // are equal here and array ordering will be used.
            (Self::Instruction(..), Self::Instruction(..)) => Ordering::Equal,
        }
    }
}

impl PartialOrd for SolanaTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TriggerData for SolanaTrigger {
    fn error_context(&self) -> std::string::String {
        match self {
            SolanaTrigger::Block(..) => {
                format!("Block #{} ({})", self.block_number(), self.block_hash())
            }

            SolanaTrigger::Instruction(instruction_with_block) => {
                format!(
                    "Instruction #{} (from #{}) for program {} (Block #{} ({})",
                    instruction_with_block.instruction.ordinal,
                    instruction_with_block.instruction.parent_ordinal,
                    hex::encode(&instruction_with_block.instruction.program_id),
                    self.block_number(),
                    self.block_hash()
                )
            }
        }
    }
}

pub struct InstructionWithInfo {
    pub instruction: codec::Instruction,
    pub block_num: u64,
    pub block_id: Vec<u8>,
    pub transaction_id: Vec<u8>,
}

impl InstructionWithInfo {
    pub fn number(&self) -> BlockNumber {
        let num = self.block_num as i32;
        num.into()
    }
}

// #[cfg(test)]
// mod tests {
//     use std::convert::TryFrom;

//     use super::*;

//     use graph::{
//         anyhow::anyhow,
//         data::subgraph::API_VERSION_0_0_5,
//         prelude::{hex, BigInt},
//     };

//     #[test]
//     fn block_trigger_to_asc_ptr() {
//         let mut heap = BytesHeap::new(API_VERSION_0_0_5);
//         let trigger = SolanaTrigger::Block(Arc::new(block()));

//         let result = blockchain::MappingTrigger::to_asc_ptr(trigger, &mut heap);
//         assert!(result.is_ok());
//     }

//     #[test]
//     fn receipt_trigger_to_asc_ptr() {
//         let mut heap = BytesHeap::new(API_VERSION_0_0_5);
//         let trigger = SolanaTrigger::Instruction(Arc::new(ReceiptWithOutcome {
//             block: Arc::new(block()),
//             outcome: execution_outcome_with_id().unwrap(),
//             receipt: receipt().unwrap(),
//         }));

//         let result = blockchain::MappingTrigger::to_asc_ptr(trigger, &mut heap);
//         assert!(result.is_ok());
//     }

//     fn block() -> codec::BlockWrapper {
//         codec::BlockWrapper {
//             block: Some(codec::Block {
//                 author: "test".to_string(),
//                 header: Some(codec::BlockHeader {
//                     height: 2,
//                     prev_height: 1,
//                     epoch_id: hash("01"),
//                     next_epoch_id: hash("02"),
//                     hash: hash("01"),
//                     prev_hash: hash("00"),
//                     prev_state_root: hash("bb00010203"),
//                     chunk_receipts_root: hash("bb00010203"),
//                     chunk_headers_root: hash("bb00010203"),
//                     chunk_tx_root: hash("bb00010203"),
//                     outcome_root: hash("cc00010203"),
//                     chunks_included: 1,
//                     challenges_root: hash("aa"),
//                     timestamp: 100,
//                     timestamp_nanosec: 0,
//                     random_value: hash("010203"),
//                     validator_proposals: vec![],
//                     chunk_mask: vec![],
//                     gas_price: big_int(10),
//                     block_ordinal: 0,
//                     validator_reward: big_int(100),
//                     total_supply: big_int(1_000),
//                     challenges_result: vec![],
//                     last_final_block: hash("00"),
//                     last_ds_final_block: hash("00"),
//                     next_bp_hash: hash("bb"),
//                     block_merkle_root: hash("aa"),
//                     epoch_sync_data_hash: vec![0x00, 0x01],
//                     approvals: vec![],
//                     signature: None,
//                     latest_protocol_version: 0,
//                 }),
//                 chunks: vec![chunk_header().unwrap()],
//             }),
//             shards: vec![codec::IndexerShard {
//                 shard_id: 0,
//                 chunk: Some(codec::IndexerChunk {
//                     author: "near".to_string(),
//                     header: chunk_header(),
//                     transactions: vec![codec::IndexerTransactionWithOutcome {
//                         transaction: Some(codec::SignedTransaction {
//                             signer_id: "signer".to_string(),
//                             public_key: Some(codec::PublicKey { bytes: vec![] }),
//                             nonce: 1,
//                             receiver_id: "receiver".to_string(),
//                             actions: vec![],
//                             signature: Some(codec::Signature {
//                                 r#type: 1,
//                                 bytes: vec![],
//                             }),
//                             hash: hash("bb"),
//                         }),
//                         outcome: Some(codec::IndexerExecutionOutcomeWithOptionalReceipt {
//                             execution_outcome: execution_outcome_with_id(),
//                             receipt: receipt(),
//                         }),
//                     }],
//                     receipts: vec![receipt().unwrap()],
//                 }),
//                 receipt_execution_outcomes: vec![codec::IndexerExecutionOutcomeWithReceipt {
//                     execution_outcome: execution_outcome_with_id(),
//                     receipt: receipt(),
//                 }],
//             }],
//             state_changes: vec![],
//         }
//     }

//     fn receipt() -> Option<codec::Receipt> {
//         Some(codec::Receipt {
//             predecessor_id: "genesis.near".to_string(),
//             receiver_id: "near".to_string(),
//             receipt_id: hash("dead"),
//             receipt: Some(codec::receipt::Receipt::Action(codec::ReceiptAction {
//                 signer_id: "near".to_string(),
//                 signer_public_key: Some(codec::PublicKey { bytes: vec![] }),
//                 gas_price: big_int(2),
//                 output_data_receivers: vec![],
//                 input_data_ids: vec![],
//                 actions: vec![
//                     codec::Action {
//                         action: Some(codec::action::Action::CreateAccount(
//                             codec::CreateAccountAction {},
//                         )),
//                     },
//                     codec::Action {
//                         action: Some(codec::action::Action::DeployContract(
//                             codec::DeployContractAction {
//                                 code: "/6q7zA==".to_string(),
//                             },
//                         )),
//                     },
//                     codec::Action {
//                         action: Some(codec::action::Action::FunctionCall(
//                             codec::FunctionCallAction {
//                                 method_name: "func".to_string(),
//                                 args: "e30=".to_string(),
//                                 gas: 1000,
//                                 deposit: big_int(100),
//                             },
//                         )),
//                     },
//                     codec::Action {
//                         action: Some(codec::action::Action::Transfer(codec::TransferAction {
//                             deposit: big_int(100),
//                         })),
//                     },
//                     codec::Action {
//                         action: Some(codec::action::Action::Stake(codec::StakeAction {
//                             stake: big_int(100),
//                             public_key: Some(codec::PublicKey { bytes: vec![] }),
//                         })),
//                     },
//                     codec::Action {
//                         action: Some(codec::action::Action::AddKey(codec::AddKeyAction {
//                             public_key: Some(codec::PublicKey { bytes: vec![] }),
//                             access_key: Some(codec::AccessKey {
//                                 nonce: 1,
//                                 permission: Some(codec::AccessKeyPermission {
//                                     permission: Some(
//                                         codec::access_key_permission::Permission::FullAccess(
//                                             codec::FullAccessPermission {},
//                                         ),
//                                     ),
//                                 }),
//                             }),
//                         })),
//                     },
//                     codec::Action {
//                         action: Some(codec::action::Action::DeleteKey(codec::DeleteKeyAction {
//                             public_key: Some(codec::PublicKey { bytes: vec![] }),
//                         })),
//                     },
//                     codec::Action {
//                         action: Some(codec::action::Action::DeleteAccount(
//                             codec::DeleteAccountAction {
//                                 beneficiary_id: "suicided.near".to_string(),
//                             },
//                         )),
//                     },
//                 ],
//             })),
//         })
//     }

//     fn chunk_header() -> Option<codec::ChunkHeader> {
//         Some(codec::ChunkHeader {
//             chunk_hash: vec![0x00],
//             prev_block_hash: vec![0x01],
//             outcome_root: vec![0x02],
//             prev_state_root: vec![0x03],
//             encoded_merkle_root: vec![0x04],
//             encoded_length: 1,
//             height_created: 2,
//             height_included: 3,
//             shard_id: 4,
//             gas_used: 5,
//             gas_limit: 6,
//             validator_reward: big_int(7),
//             balance_burnt: big_int(7),
//             outgoing_receipts_root: vec![0x07],
//             tx_root: vec![0x08],
//             validator_proposals: vec![codec::ValidatorStake {
//                 account_id: "account".to_string(),
//                 public_key: public_key("aa"),
//                 stake: big_int(10),
//             }],
//             signature: Some(codec::Signature {
//                 r#type: 0,
//                 bytes: vec![],
//             }),
//         })
//     }

//     fn execution_outcome_with_id() -> Option<codec::ExecutionOutcomeWithIdView> {
//         Some(codec::ExecutionOutcomeWithIdView {
//             proof: Some(codec::MerklePath { path: vec![] }),
//             block_hash: hash("aa"),
//             id: hash("beef"),
//             outcome: execution_outcome(),
//         })
//     }

//     fn execution_outcome() -> Option<codec::ExecutionOutcome> {
//         Some(codec::ExecutionOutcome {
//             logs: vec!["string".to_string()],
//             receipt_ids: vec![],
//             gas_burnt: 1,
//             tokens_burnt: big_int(2),
//             executor_id: "near".to_string(),
//             status: Some(codec::execution_outcome::Status::SuccessValue(
//                 codec::SuccessValueExecutionStatus {
//                     value: "/6q7zA==".to_string(),
//                 },
//             )),
//         })
//     }

//     fn big_int(input: u64) -> Option<codec::BigInt> {
//         let value =
//             BigInt::try_from(input).expect(format!("Invalid BigInt value {}", input).as_ref());
//         let bytes = value.to_signed_bytes_le();

//         Some(codec::BigInt { bytes })
//     }

//     fn hash(input: &str) -> Option<codec::CryptoHash> {
//         Some(codec::CryptoHash {
//             bytes: hex::decode(input).expect(format!("Invalid hash value {}", input).as_ref()),
//         })
//     }

//     fn public_key(input: &str) -> Option<codec::PublicKey> {
//         Some(codec::PublicKey {
//             bytes: hex::decode(input).expect(format!("Invalid PublicKey value {}", input).as_ref()),
//         })
//     }

//     struct BytesHeap {
//         api_version: graph::semver::Version,
//         memory: Vec<u8>,
//     }

//     impl BytesHeap {
//         fn new(api_version: graph::semver::Version) -> Self {
//             Self {
//                 api_version,
//                 memory: vec![],
//             }
//         }
//     }

//     impl AscHeap for BytesHeap {
//         fn raw_new(&mut self, bytes: &[u8]) -> Result<u32, DeterministicHostError> {
//             self.memory.extend_from_slice(bytes);
//             Ok((self.memory.len() - bytes.len()) as u32)
//         }

//         fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, DeterministicHostError> {
//             let memory_byte_count = self.memory.len();
//             if memory_byte_count == 0 {
//                 return Err(DeterministicHostError(anyhow!("No memory is allocated")));
//             }

//             let start_offset = offset as usize;
//             let end_offset_exclusive = start_offset + size as usize;

//             if start_offset >= memory_byte_count {
//                 return Err(DeterministicHostError(anyhow!(
//                     "Start offset {} is outside of allocated memory, max offset is {}",
//                     start_offset,
//                     memory_byte_count - 1
//                 )));
//             }

//             if end_offset_exclusive > memory_byte_count {
//                 return Err(DeterministicHostError(anyhow!(
//                     "End of offset {} is outside of allocated memory, max offset is {}",
//                     end_offset_exclusive,
//                     memory_byte_count - 1
//                 )));
//             }

//             return Ok(Vec::from(&self.memory[start_offset..end_offset_exclusive]));
//         }

//         fn api_version(&self) -> graph::semver::Version {
//             self.api_version.clone()
//         }

//         fn asc_type_id(
//             &mut self,
//             type_id_index: graph::runtime::IndexForAscTypeId,
//         ) -> Result<u32, DeterministicHostError> {
//             // Not totally clear what is the purpose of this method, why not a default implementation here?
//             Ok(type_id_index as u32)
//         }
//     }
// }
