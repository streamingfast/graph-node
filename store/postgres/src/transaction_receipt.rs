use anyhow::{ensure, Error};
use diesel::pg::{Pg, PgConnection};
use diesel::prelude::*;
use diesel::query_builder::{Query, QueryFragment};
use diesel::sql_types::{Binary, Nullable, Text};
use graph::prelude::web3::types::*;
use itertools::Itertools;
use std::convert::TryFrom;

struct TransactionReceiptQuery<'a> {
    block_hash: &'a str,
    schema_name: &'a str,
}

impl<'a> diesel::query_builder::QueryId for TransactionReceiptQuery<'a> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> QueryFragment<Pg> for TransactionReceiptQuery<'a> {
    /// Writes the following SQL:
    ///
    /// ```sql
    /// select
    ///     decode(
    ///         case when length(receipt ->> 'gasUsed') % 2 = 0 then
    ///             ltrim(receipt ->> 'gasUsed', '0x')
    ///         else
    ///             replace((receipt ->> 'gasUsed'), 'x', '')
    ///         end, 'hex') as gas_used,
    ///     decode(replace(receipt ->> 'status', 'x', ''), 'hex') as status
    /// from (
    ///     select
    ///         jsonb_array_elements(data -> 'transaction_receipts') as receipt
    ///     from
    ///         $CHAIN_SCHEMA.blocks
    ///     where
    ///         hash = $BLOCK_HASH) as foo;
    ///```
    fn walk_ast(&self, mut out: diesel::query_builder::AstPass<Pg>) -> QueryResult<()> {
        out.push_sql(
            r#"
select decode(
    case when length(receipt ->> 'gasUsed') % 2 = 0 then
        ltrim(receipt ->> 'gasUsed', '0x')
    else
        replace((receipt ->> 'gasUsed'), 'x', '')
    end, 'hex') as gas_used,
    decode(replace(receipt ->> 'status', 'x', ''), 'hex') as status
from (
    select jsonb_array_elements(data -> 'transaction_receipts') as receipt
    from"#,
        );
        out.push_identifier(&self.schema_name)?;
        out.push_sql(".");
        out.push_identifier("blocks")?;
        out.push_sql(" where hash = ");
        out.push_bind_param::<Text, _>(&self.block_hash)?;
        out.push_sql(") as foo;");
        Ok(())
    }
}

impl<'a> Query for TransactionReceiptQuery<'a> {
    type SqlType = (
        Binary,
        Binary,
        Nullable<Binary>,
        Nullable<Binary>,
        Nullable<Binary>,
        Nullable<Binary>,
    );
}

impl<'a> RunQueryDsl<PgConnection> for TransactionReceiptQuery<'a> {}

/// Type that comes straight out of a SQL query
#[derive(QueryableByName, Queryable)]
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

impl LightTransactionReceipt {
    pub fn is_sucessful(&self) -> bool {
        // EIP-658
        matches!(self.status, Some(status) if !status.is_zero())
    }
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
) -> anyhow::Result<Vec<LightTransactionReceipt>> {
    let query = TransactionReceiptQuery {
        // convert block_hash to its string representation
        block_hash: &format!("0x{}", hex::encode(block_hash.as_bytes())),
        schema_name: chain_name,
    };

    query
        .get_results::<RawTransactionReceipt>(conn)
        .or_else(|error| {
            Err(anyhow::anyhow!(
                "Error fetching transaction receipt from database: {}",
                error
            ))
        })?
        .into_iter()
        .map(LightTransactionReceipt::try_from)
        .collect()
}
