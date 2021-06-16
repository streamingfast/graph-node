//! Code for retrieving transaction gas information from the database.
//!
//! This module exposes the [`find_transaction_gas_in_block_range`] function, that queries the
//! database and returns how much gas each transaction were issued with.

use super::transaction_receipt::drain_vector;
use diesel::{
    pg::{Pg, PgConnection},
    prelude::*,
    query_builder::{Query, QueryFragment, QueryId},
    sql_types::{Binary, Integer},
};
use graph::prelude::{
    web3::types::{H256, U256},
    BlockNumber,
};
use std::{collections::HashMap, convert::TryFrom, ops::Range};

/// Parameters for querying for all transaction gas for a given block.
struct TransactionGasQuery<'a> {
    block_range: &'a Range<BlockNumber>,
    transaction_hashes: &'a [&'a H256],
    schema_name: &'a str,
}

impl<'a> QueryId for TransactionGasQuery<'a> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> QueryFragment<Pg> for TransactionGasQuery<'a> {
    /// Writes the following SQL:
    ///
    /// ```sql
    /// select
    ///     ethereum_hex_to_bytea (txn ->> 'hash') as transaction_hash,
    ///     ethereum_hex_to_bytea (txn ->> 'gas')
    /// from (
    ///     select
    ///         jsonb_array_elements(block -> 'transactions') as txn
    ///     from (
    ///         select
    ///             data -> 'block' as block
    ///         from
    ///             CHAIN_NAME.blocks
    ///         where
    ///             number between $START_BLOCK
    ///             and $END_BLOCK) as blocks) as transactions
    /// where
    ///     ethereum_hex_to_bytea (txn ->> 'hash') in ($LIST_OF_TRANSACTION_HASHES)
    ///
    ///```
    fn walk_ast(&self, mut out: diesel::query_builder::AstPass<Pg>) -> QueryResult<()> {
        out.push_sql(
            r#"
select
    ethereum_hex_to_bytea (txn ->> 'hash') as transaction_hash,
    ethereum_hex_to_bytea (txn ->> 'gas')
from (
    select
        jsonb_array_elements(block -> 'transactions') as txn
    from (
        select
            data -> 'block' as block
        from
"#,
        );
        out.push_identifier(&self.schema_name)?;
        out.push_sql(".blocks where number between ");
        out.push_bind_param::<Integer, _>(&self.block_range.start)?;
        out.push_sql(" and ");
        out.push_bind_param::<Integer, _>(&self.block_range.end)?;
        out.push_sql(") as blocks) as transactions ");
        out.push_sql("where ethereum_hex_to_bytea(txn ->> 'hash') in (");

        let mut iterator = self.transaction_hashes.iter().peekable();
        while let Some(transaction) = iterator.next() {
            out.push_bind_param::<Binary, _>(&transaction.as_bytes())?;
            if iterator.peek().is_some() {
                out.push_sql(", ")
            }
        }
        out.push_sql(")");
        Ok(())
    }
}

impl<'a> Query for TransactionGasQuery<'a> {
    type SqlType = (Binary, Binary);
}

impl<'a> RunQueryDsl<PgConnection> for TransactionGasQuery<'a> {}

/// Type that comes straight out of a SQL query
#[derive(QueryableByName, Queryable)]
struct RawTransactionGas {
    #[sql_type = "Binary"]
    transaction_hash: Vec<u8>,
    #[sql_type = "Binary"]
    gas: Vec<u8>,
}

/// Like web3::types::Transaction, but with fewer fields.
struct TransactionGas {
    pub transaction_hash: H256,
    pub gas: U256,
}

impl TryFrom<RawTransactionGas> for TransactionGas {
    type Error = anyhow::Error;

    fn try_from(value: RawTransactionGas) -> Result<Self, Self::Error> {
        let RawTransactionGas {
            transaction_hash,
            gas,
        } = value;
        let transaction_hash = drain_vector(transaction_hash)?;
        let gas = drain_vector(gas)?;

        Ok(TransactionGas {
            transaction_hash: transaction_hash.into(),
            gas: gas.into(),
        })
    }
}

/// Queries the database for gas used by given transactions in a given block range.
pub(crate) fn find_transaction_gas_in_block_range(
    conn: &PgConnection,
    chain_name: &str,
    transaction_hashes: &[&H256],
    block_range: &Range<BlockNumber>,
) -> anyhow::Result<HashMap<H256, U256>> {
    let query = TransactionGasQuery {
        block_range,
        transaction_hashes,
        schema_name: chain_name,
    };

    let rows: anyhow::Result<Vec<TransactionGas>> = query
        .get_results::<RawTransactionGas>(conn)
        .or_else(|error| {
            Err(anyhow::anyhow!(
                "Error fetching transaction gas from database: {}",
                error
            ))
        })?
        .into_iter()
        .map(TransactionGas::try_from)
        .collect();

    Ok(rows?
        .into_iter()
        .map(|txn| (txn.transaction_hash, txn.gas))
        .collect())
}
