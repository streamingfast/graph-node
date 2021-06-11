use anyhow::anyhow;
use anyhow::{ensure, Error};
use diesel::prelude::*;
use diesel::sql_types::{Binary, Integer, Nullable};

use graph::prelude::web3::types::*;
use itertools::Itertools;
use std::convert::TryFrom;

/// Type that comes straight out of a SQL query
#[derive(QueryableByName)]
struct RawTransactionReceipt {
    #[sql_type = "Binary"]
    transaction_hash: Vec<u8>,
    #[sql_type = "Binary"]
    transaction_index: Vec<u8>,
    #[sql_type = "Nullable<Binary>"]
    block_hash: Option<Vec<u8>>,
    #[sql_type = "Nullable<Binary>"]
    block_number: Option<Vec<u8>>,
    #[sql_type = "Nullable<Binary>"]
    gas_used: Option<Vec<u8>>,
    #[sql_type = "Nullable<Binary>"]
    status: Option<Vec<u8>>,
}

/// Like web3::types::Receipt, but with fewer fields.
pub(crate) struct LightTransactionReceipt {
    pub transaction_hash: H256,
    pub transaction_index: U64,
    pub block_hash: Option<H256>,
    pub block_number: Option<U64>,
    pub gas_used: Option<U256>,
    pub status: Option<U64>,
}

/// Converts Vec<u8> to [u8; N], where N is the vector's expected lenght.
/// Fails if other than N bytes are transfered this way.
fn drain_vector<I: IntoIterator<Item = u8>, const N: usize>(
    source: I,
    size: usize,
) -> Result<[u8; N], anyhow::Error> {
    let mut output = [0u8; N];
    let bytes_read = output.iter_mut().set_from(source);
    ensure!(bytes_read == size, "failed reading bytes from source");
    Ok(output)
}

impl TryFrom<RawTransactionReceipt> for LightTransactionReceipt {
    type Error = anyhow::Error;

    fn try_from(value: RawTransactionReceipt) -> Result<Self, Self::Error> {
        let RawTransactionReceipt {
            transaction_hash,
            transaction_index,
            block_hash,
            block_number,
            gas_used,
            status,
        } = value;

        let transaction_hash = drain_vector(transaction_hash, 32)?;
        let transaction_index = drain_vector(transaction_index, 8)?;
        let block_hash = block_hash.map(|x| drain_vector(x, 32)).transpose()?;
        let block_number = block_number.map(|x| drain_vector(x, 8)).transpose()?;
        let gas_used = gas_used.map(|x| drain_vector(x, 32)).transpose()?;
        let status = status.map(|x| drain_vector(x, 8)).transpose()?;

        Ok(LightTransactionReceipt {
            transaction_hash: transaction_hash.into(),
            transaction_index: transaction_index.into(),
            block_hash: block_hash.map(Into::into),
            block_number: block_number.map(Into::into),
            gas_used: gas_used.map(Into::into),
            status: status.map(Into::into),
        })
    }
}

pub(crate) fn find_transaction_receipts_for_block(
    conn: &PgConnection,
    chain_name: &str,
    block_hash: &H256,
) -> Result<Vec<LightTransactionReceipt>, Error> {
    use diesel::dsl::sql_query;
    let query = "";
    sql_query(query)
        .bind::<Integer, _>(12556561)
        .get_results::<RawTransactionReceipt>(conn)
        .or_else(|e| Err(anyhow::anyhow!("Error fetching from database: {}", e)))?
        .into_iter()
        .map(|r| LightTransactionReceipt::try_from(r))
        .collect()
}
