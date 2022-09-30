//! Parse Postgres index definition into a form that is meaningful for us.
use std::fmt::Display;

use graph::itertools::Itertools;
use graph::prelude::{
    regex::{Captures, Regex},
    BlockNumber,
};

#[derive(Debug, PartialEq)]
pub enum Method {
    Brin,
    BTree,
    Gin,
    Gist,
    Unknown(String),
}

impl Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Method::*;
        match self {
            Brin => write!(f, "brin")?,
            BTree => write!(f, "btree")?,
            Gin => write!(f, "gin")?,
            Gist => write!(f, "gist")?,
            Unknown(s) => write!(f, "{s}")?,
        }
        Ok(())
    }
}

impl Method {
    fn parse(method: String) -> Self {
        use Method::*;

        match method.as_str() {
            "brin" => Brin,
            "btree" => BTree,
            "gin" => Gin,
            "gist" => Gist,
            _ => Unknown(method),
        }
    }
}

/// An index expression, i.e., a 'column' in an index
#[derive(Debug, PartialEq)]
pub enum Expr {
    /// A named column; only user-defined columns appear here
    Column(String),
    /// A prefix of a named column, used for indexes on `text` and `bytea`
    Prefix(String),
    /// The `vid` column
    Vid,
    /// The `block$` column
    Block,
    /// The `block_range` column
    BlockRange,
    /// The expression `lower(block_range)`
    BlockRangeLower,
    /// The expression `coalesce(upper(block_range), 2147483647)`
    BlockRangeUpper,
    /// The literal index expression since none of the previous options
    /// matched
    Unknown(String),
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(s) => write!(f, "{s}")?,
            Expr::Prefix(s) => write!(f, "{s}")?,
            Expr::Vid => write!(f, "vid")?,
            Expr::Block => write!(f, "block")?,
            Expr::BlockRange => write!(f, "block_range")?,
            Expr::BlockRangeLower => write!(f, "lower(block_range)")?,
            Expr::BlockRangeUpper => write!(f, "upper(block_range)")?,
            Expr::Unknown(e) => write!(f, "{e}")?,
        }
        Ok(())
    }
}

impl Expr {
    fn parse(expr: &str) -> Self {
        use Expr::*;

        let expr = expr.trim().to_string();

        let prefix_rx = Regex::new("^(substring|left)\\((?P<name>[a-z0-9$_]+)").unwrap();

        if expr == "vid" {
            Vid
        } else if expr == "lower(block_range)" {
            BlockRangeLower
        } else if expr == "coalesce(upper(block_range), 2147483647)" {
            BlockRangeUpper
        } else if expr == "block_range" {
            BlockRange
        } else if expr == "block$" {
            Block
        } else if expr
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '$' || c == '_')
        {
            Column(expr)
        } else if let Some(caps) = prefix_rx.captures(&expr) {
            if let Some(name) = caps.name("name") {
                Prefix(name.as_str().to_string())
            } else {
                Unknown(expr)
            }
        } else {
            Unknown(expr)
        }
    }

    fn is_attribute(&self) -> bool {
        use Expr::*;

        match self {
            Column(_) | Prefix(_) => true,
            Vid | Block | BlockRange | BlockRangeLower | BlockRangeUpper | Unknown(_) => false,
        }
    }

    fn is_id(&self) -> bool {
        use Expr::*;
        match self {
            Column(s) => s == "id",
            _ => false,
        }
    }
}

/// The condition for a partial index, i.e., the statement after `where ..`
/// in a `create index` statement
#[derive(Debug, PartialEq)]
pub enum Cond {
    /// The expression `coalesce(upper(block_range), 2147483647) > $number`
    Partial(BlockNumber),
    /// The expression `coalesce(upper(block_range), 2147483647) < 2147483647`
    Closed,
    /// Any other expression
    Unknown(String),
}

impl Display for Cond {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Cond::*;

        match self {
            Partial(number) => write!(f, "upper(block_range) > {number}"),
            Closed => write!(f, "closed(block_range)"),
            Unknown(s) => write!(f, "{s}"),
        }
    }
}

impl Cond {
    fn parse(cond: String) -> Self {
        fn parse_partial(cond: &str) -> Option<Cond> {
            let cond_rx =
                Regex::new("coalesce\\(upper\\(block_range\\), 2147483647\\) > (?P<number>[0-9]+)")
                    .unwrap();

            let caps = cond_rx.captures(cond)?;
            caps.name("number")
                .map(|number| number.as_str())
                .and_then(|number| number.parse::<BlockNumber>().ok())
                .map(|number| Cond::Partial(number))
        }

        if &cond == "coalesce(upper(block_range), 2147483647) < 2147483647" {
            Cond::Closed
        } else {
            parse_partial(&cond).unwrap_or_else(|| Cond::Unknown(cond))
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum CreateIndex {
    /// The literal index definition passed to `parse`. This is used when we
    /// can't parse a `create index` statement, e.g. because it uses
    /// features we don't care about.
    Unknown { defn: String },
    /// Representation of a `create index` statement that we successfully
    /// parsed.
    Parsed {
        /// Is this a `unique` index
        unique: bool,
        /// The name of the index
        name: String,
        /// The namespace of the table to which this index belongs
        nsp: String,
        /// The name of the table to which this index belongs
        table: String,
        /// The index method
        method: Method,
        /// The columns (or more generally expressions) that are indexed
        columns: Vec<Expr>,
        /// The condition for partial indexes
        cond: Option<Cond>,
        /// Storage parameters for the index
        with: Option<String>,
    },
}

impl Display for CreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use CreateIndex::*;

        match self {
            Unknown { defn } => {
                writeln!(f, "{defn}")?;
            }
            Parsed {
                unique,
                name,
                nsp: _,
                table: _,
                method,
                columns,
                cond,
                with,
            } => {
                let columns = columns.into_iter().map(|c| c.to_string()).join(", ");
                let unique = if *unique { "[uq]" } else { "" };
                write!(f, "{name}{unique} {method}({columns})")?;
                if let Some(cond) = cond {
                    write!(f, " where {cond}")?;
                }
                if let Some(with) = with {
                    write!(f, " with {with}")?;
                }
                writeln!(f, "")?;
            }
        }
        Ok(())
    }
}

impl CreateIndex {
    /// Parse a `create index` statement. We are mostly concerned with
    /// parsing indexes that `graph-node` created. If we can't parse an
    /// index definition, it is returned as `CreateIndex::Unknown`.
    ///
    ///  The `defn` should be formatted the way it is formatted in Postgres'
    /// `pg_indexes.indexdef` system catalog; it's likely that deviating
    /// from that formatting will make the index definition not parse
    /// properly and return a `CreateIndex::Unknown`.
    pub fn parse(mut defn: String) -> Self {
        fn field(cap: &Captures, name: &str) -> Option<String> {
            cap.name(name).map(|mtch| mtch.as_str().to_string())
        }

        fn split_columns(s: &str) -> Vec<Expr> {
            let mut parens = 0;
            let mut column = String::new();
            let mut columns = Vec::new();

            for c in s.chars() {
                match c {
                    '"' => { /* strip double quotes */ }
                    '(' => {
                        parens += 1;
                        column.push(c);
                    }
                    ')' => {
                        parens -= 1;
                        column.push(c);
                    }
                    ',' if parens == 0 => {
                        columns.push(Expr::parse(&column));
                        column = String::new();
                    }
                    _ => column.push(c),
                }
            }
            columns.push(Expr::parse(&column));

            columns
        }

        fn new_parsed(defn: &str) -> Option<CreateIndex> {
            let rx = Regex::new(
                "create (?P<unique>unique )?index (?P<name>[a-z0-9$_]+) \
            on (?P<nsp>sgd[0-9]+)\\.(?P<table>[a-z$_]+) \
            using (?P<method>[a-z]+) \\((?P<columns>.*?)\\)\
            ( where \\((?P<cond>.*)\\))?\
            ( with \\((?P<with>.*)\\))?$",
            )
            .unwrap();

            let cap = rx.captures(&defn)?;
            let unique = cap.name("unique").is_some();
            let name = field(&cap, "name")?;
            let nsp = field(&cap, "nsp")?;
            let table = field(&cap, "table")?;
            let columns = field(&cap, "columns")?;
            let method = Method::parse(field(&cap, "method")?);
            let cond = field(&cap, "cond").map(Cond::parse);
            let with = field(&cap, "with");

            let columns = split_columns(&columns);
            Some(CreateIndex::Parsed {
                unique,
                name,
                nsp,
                table,
                method,
                columns,
                cond,
                with,
            })
        }

        defn.make_ascii_lowercase();
        new_parsed(&defn).unwrap_or_else(|| CreateIndex::Unknown { defn })
    }

    pub fn is_attribute_index(&self) -> bool {
        use CreateIndex::*;
        match self {
            Unknown { defn: _ } => false,
            Parsed {
                columns,
                cond,
                with,
                method,
                ..
            } => {
                if cond.is_some() || with.is_some() {
                    return false;
                }
                match method {
                    Method::Gist => {
                        columns.len() == 2
                            && columns[0].is_attribute()
                            && !columns[0].is_id()
                            && columns[1] == Expr::BlockRange
                    }
                    Method::Brin => false,
                    Method::BTree | Method::Gin => {
                        columns.len() == 1
                            && columns[0].is_attribute()
                            && cond.is_none()
                            && with.is_none()
                    }
                    Method::Unknown(_) => false,
                }
            }
        }
    }
}

#[test]
fn parse() {
    use Method::*;

    #[derive(Debug)]
    enum TestExpr {
        Name(&'static str),
        Prefix(&'static str),
        Vid,
        Block,
        BlockRange,
        BlockRangeLower,
        BlockRangeUpper,
        #[allow(dead_code)]
        Unknown(&'static str),
    }

    impl<'a> From<&'a TestExpr> for Expr {
        fn from(expr: &'a TestExpr) -> Self {
            match expr {
                TestExpr::Name(name) => Expr::Column(name.to_string()),
                TestExpr::Prefix(name) => Expr::Prefix(name.to_string()),
                TestExpr::Vid => Expr::Vid,
                TestExpr::Block => Expr::Block,
                TestExpr::BlockRange => Expr::BlockRange,
                TestExpr::BlockRangeLower => Expr::BlockRangeLower,
                TestExpr::BlockRangeUpper => Expr::BlockRangeUpper,
                TestExpr::Unknown(s) => Expr::Unknown(s.to_string()),
            }
        }
    }

    #[derive(Debug)]
    enum TestCond {
        Partial(BlockNumber),
        Closed,
        Unknown(&'static str),
    }

    impl From<TestCond> for Cond {
        fn from(expr: TestCond) -> Self {
            match expr {
                TestCond::Partial(number) => Cond::Partial(number),
                TestCond::Unknown(s) => Cond::Unknown(s.to_string()),
                TestCond::Closed => Cond::Closed,
            }
        }
    }

    #[derive(Debug)]
    struct Parsed {
        unique: bool,
        name: &'static str,
        nsp: &'static str,
        table: &'static str,
        method: Method,
        columns: &'static [TestExpr],
        cond: Option<TestCond>,
    }

    impl From<Parsed> for CreateIndex {
        fn from(p: Parsed) -> Self {
            let Parsed {
                unique,
                name,
                nsp,
                table,
                method,
                columns,
                cond,
            } = p;
            let columns: Vec<_> = columns.into_iter().map(|c| Expr::from(c)).collect();
            let cond = cond.map(Cond::from);
            CreateIndex::Parsed {
                unique,
                name: name.to_string(),
                nsp: nsp.to_string(),
                table: table.to_string(),
                method,
                columns,
                cond,
                with: None,
            }
        }
    }

    #[track_caller]
    fn parse_one(defn: &str, exp: Parsed) {
        let act = CreateIndex::parse(defn.to_string());
        let exp = CreateIndex::from(exp);
        assert_eq!(exp, act);
    }

    use TestCond::*;
    use TestExpr::*;

    let sql = "create index attr_1_0_token_id on sgd44.token using btree (id)";
    let exp = Parsed {
        unique: false,
        name: "attr_1_0_token_id",
        nsp: "sgd44",
        table: "token",
        method: BTree,
        columns: &[Name("id")],
        cond: None,
    };
    parse_one(sql, exp);

    let sql =
        "create index attr_1_1_token_symbol on sgd44.token using btree (\"left\"(symbol, 256))";
    let exp = Parsed {
        unique: false,
        name: "attr_1_1_token_symbol",
        nsp: "sgd44",
        table: "token",
        method: BTree,
        columns: &[Prefix("symbol")],
        cond: None,
    };
    parse_one(sql, exp);

    let sql = "create index attr_1_5_token_trade_volume on sgd44.token using btree (trade_volume)";
    let exp = Parsed {
        unique: false,
        name: "attr_1_5_token_trade_volume",
        nsp: "sgd44",
        table: "token",
        method: BTree,
        columns: &[Name("trade_volume")],
        cond: None,
    };
    parse_one(sql, exp);

    let sql = "create unique index token_pkey on sgd44.token using btree (vid)";
    let exp = Parsed {
        unique: true,
        name: "token_pkey",
        nsp: "sgd44",
        table: "token",
        method: BTree,
        columns: &[Vid],
        cond: None,
    };
    parse_one(sql, exp);

    let sql = "create index brin_token on sgd44.token using brin (lower(block_range), coalesce(upper(block_range), 2147483647), vid)";
    let exp = Parsed {
        unique: false,
        name: "brin_token",
        nsp: "sgd44",
        table: "token",
        method: Brin,
        columns: &[BlockRangeLower, BlockRangeUpper, Vid],
        cond: None,
    };
    parse_one(sql, exp);

    let sql = "create index token_block_range_closed on sgd44.token using btree (coalesce(upper(block_range), 2147483647)) where (coalesce(upper(block_range), 2147483647) < 2147483647)";
    let exp = Parsed {
        unique: false,
        name: "token_block_range_closed",
        nsp: "sgd44",
        table: "token",
        method: BTree,
        columns: &[BlockRangeUpper],
        cond: Some(Closed),
    };
    parse_one(sql, exp);

    let sql = "create index token_id_block_range_excl on sgd44.token using gist (id, block_range)";
    let exp = Parsed {
        unique: false,
        name: "token_id_block_range_excl",
        nsp: "sgd44",
        table: "token",
        method: Gist,
        columns: &[Name("id"), BlockRange],
        cond: None,
    };
    parse_one(sql, exp);

    let sql="create index attr_1_11_pool_owner on sgd411585.pool using btree (\"substring\"(owner, 1, 64))";
    let exp = Parsed {
        unique: false,
        name: "attr_1_11_pool_owner",
        nsp: "sgd411585",
        table: "pool",
        method: BTree,
        columns: &[Prefix("owner")],
        cond: None,
    };
    parse_one(sql, exp);

    let sql =
        "create index attr_1_20_pool_vault_id on sgd411585.pool using gist (vault_id, block_range)";
    let exp = Parsed {
        unique: false,
        name: "attr_1_20_pool_vault_id",
        nsp: "sgd411585",
        table: "pool",
        method: Gist,
        columns: &[Name("vault_id"), BlockRange],
        cond: None,
    };
    parse_one(sql, exp);

    let sql = "create index attr_1_22_pool_tokens_list on sgd411585.pool using gin (tokens_list)";
    let exp = Parsed {
        unique: false,
        name: "attr_1_22_pool_tokens_list",
        nsp: "sgd411585",
        table: "pool",
        method: Gin,
        columns: &[Name("tokens_list")],
        cond: None,
    };
    parse_one(sql, exp);

    let sql = "create index manual_partial_pool_total_liquidity on sgd411585.pool using btree (total_liquidity) where (coalesce(upper(block_range), 2147483647) > 15635000)";
    let exp = Parsed {
        unique: false,
        name: "manual_partial_pool_total_liquidity",
        nsp: "sgd411585",
        table: "pool",
        method: BTree,
        columns: &[Name("total_liquidity")],
        cond: Some(Partial(15635000)),
    };
    parse_one(sql, exp);

    let sql = "create index manual_swap_pool_timestamp_id on sgd217942.swap using btree (pool, \"timestamp\", id)";
    let exp = Parsed {
        unique: false,
        name: "manual_swap_pool_timestamp_id",
        nsp: "sgd217942",
        table: "swap",
        method: BTree,
        columns: &[Name("pool"), Name("timestamp"), Name("id")],
        cond: None,
    };
    parse_one(sql, exp);

    let sql = "CREATE INDEX brin_scy ON sgd314614.scy USING brin (\"block$\", vid)";
    let exp = Parsed {
        unique: false,
        name: "brin_scy",
        nsp: "sgd314614",
        table: "scy",
        method: Brin,
        columns: &[Block, Vid],
        cond: None,
    };
    parse_one(sql, exp);

    let sql =
        "CREATE INDEX brin_scy ON sgd314614.scy USING brin (\"block$\", vid) where (amount > 0)";
    let exp = Parsed {
        unique: false,
        name: "brin_scy",
        nsp: "sgd314614",
        table: "scy",
        method: Brin,
        columns: &[Block, Vid],
        cond: Some(TestCond::Unknown("amount > 0")),
    };
    parse_one(sql, exp);

    let sql =
        "CREATE INDEX manual_token_random_cond ON sgd44.token USING btree (decimals) WHERE (decimals > (5)::numeric)";
    let exp = Parsed {
        unique: false,
        name: "manual_token_random_cond",
        nsp: "sgd44",
        table: "token",
        method: BTree,
        columns: &[Name("decimals")],
        cond: Some(TestCond::Unknown("decimals > (5)::numeric")),
    };
    parse_one(sql, exp);
}
