#![allow(unused)]

use std::path::PathBuf;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn, Level};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use tracing_subscriber::prelude::*;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_json::json;
use serde_plain::Error;
use surrealdb::engine::remote::ws::{Client, Wss};
use surrealdb::Surreal;
use surrealdb::types::{RecordId, SurrealValue};
use tokio::io::{stdin, AsyncBufReadExt, AsyncRead, BufReader};
use tokio::fs::File;
use tokio::time::timeout;

/// How long to wait for a single database operation before assuming the connection is dead
const DB_OP_TIMEOUT: Duration = Duration::from_secs(10);
/// Initial/maximum delay between reconnection attempts
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<()> {
    // Filter logs from other crates
    let filter = tracing_subscriber::filter::Targets::new()
        .with_default(Level::ERROR)
        .with_target(module_path!(), Level::TRACE);
    // Initialize the logger
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .init();

    // Parse the cli arguments
    let args = Args::parse();

    // Connect and authenticate with the database, retrying indefinitely if it's unreachable
    let mut db = connect_db_with_retry(&args).await;

    // Open the file/stdin
    let reader = BufReader::new(open_input(args.input_path.clone()).await?);
    // Process every new line as a separate pager
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {

        // A flexnext/reflex packet
        if line.starts_with("FLEX_NEXT|") {
            let mut parts = line.splitn(9, '|');

            let Some(protocol) = extract::<String>(parts.next()) else { continue };
            let Some(timestamp) = extract::<String>(parts.next()) else { continue };
            let Some(baud_level) = extract::<String>(parts.next()) else { continue };
            let Some(cfp) = extract::<String>(parts.next()) else { continue };
            let Some(capcode) = extract(parts.next()) else { continue };
            let Some(addr) = extract(parts.next()) else { continue };
            let Some(msg_type) = extract(parts.next()) else { continue };
            let Some(flags) = extract(parts.next()) else { continue };
            let Some(message) = extract(parts.next()) else { continue };

            let msg = FlexNextRecord {
                timestamp: DateTime::parse_from_rfc3339(&timestamp)?.to_utc(),
                baud_level,
                cfp,
                capcode,
                addr,
                msg_type,
                flags,
                message,
            };

            // Try to insert the record into the database, reconnecting first if the connection was lost
            let Some(r) = insert_record(&mut db, &args, "flex_next_messages", msg).await else { continue };
            trace!("Inserted FlexNext pager into database: {:?}", r.key);

            continue;
        }

        // An original flex packet
        if line.starts_with("FLEX|") {
            let mut parts = line.splitn(7, '|');

            let Some(protocol) = extract::<String>(parts.next()) else { continue };
            let Some(timestamp) = extract::<String>(parts.next()) else { continue };
            let Some(baud_level_frag_phase) = extract::<String>(parts.next()) else { continue };
            let Some(cf) = extract::<String>(parts.next()) else { continue };
            let Some(capcode) = extract(parts.next()) else { continue };
            let Some(msg_type) = extract::<String>(parts.next()) else { continue };
            let Some(message) = extract(parts.next()) else { continue };

            let msg = FlexRecord {
                timestamp: NaiveDateTime::parse_from_str(&timestamp, "%Y-%m-%d %H:%M:%S")?.and_utc(),
                baud_level_frag_phase,
                cf,
                capcode,
                msg_type: FlexType::ALN,
                message,
            };

            // Try to insert the record into the database, reconnecting first if the connection was lost
            let Some(r) = insert_record(&mut db, &args, "flex_messages", msg).await else { continue };
            trace!("Inserted Flex pager into database: {:?}", r.key);

            continue;
        }

        // Dump unrecognized packets to console (this also dumps malformed packets or packets with newlines in them)
        debug!("Unsupported protocol detected, raw message dump: {line}");

    }

    info!("Exiting...");
    Ok(())
}

/// Attempts to open the file at the specified input path, or stdin if the input path is "-"
async fn open_input(input_path: PathBuf) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    if &input_path == "-" {
        Ok(Box::new(stdin()))
    } else {
        let file = File::open(input_path)
            .await?;
        Ok(Box::new(file))
    }
}

/// Connects to the database and signs in once, without retrying.
async fn connect_db(args: &Args) -> Result<Surreal<Client>> {
    let db = Surreal::new::<Wss>(args.database_url.clone()).await?;
    db.signin(surrealdb::opt::auth::Database {
        namespace: args.ns.clone(),
        database: args.db.clone(),
        username: args.username.clone(),
        password: args.password.clone(),
    }).await?;
    Ok(db)
}

/// Connects to the database, retrying with exponential backoff (capped at `RECONNECT_MAX_DELAY`)
/// for as long as it takes. Used both for the initial connection and for reconnecting after the
/// connection is lost, so that a database outage never permanently stops new pagers from being inserted.
async fn connect_db_with_retry(args: &Args) -> Surreal<Client> {
    let mut delay = RECONNECT_INITIAL_DELAY;
    loop {
        match connect_db(args).await {
            Ok(db) => {
                info!("Connected to the database");
                return db;
            }
            Err(e) => {
                warn!("Failed to connect to the database, retrying in {delay:?}: {e}");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RECONNECT_MAX_DELAY);
            }
        }
    }
}

/// Inserts `content` into `table`, reconnecting and retrying if the operation times out or fails
/// due to a connection error. Other errors (e.g. invalid data) are logged and `None` is returned.
async fn insert_record<T>(db: &mut Surreal<Client>, args: &Args, table: &str, content: T) -> Option<RecordId>
where
    T: SurrealValue + Clone,
{
    loop {
        match timeout(DB_OP_TIMEOUT, db.create::<Option<RecordId>>(table).content(content.clone())).await {
            Ok(Ok(record)) => return record,
            Ok(Err(e)) if e.details().is_connection() => {
                warn!("Lost connection to the database, reconnecting: {e}");
                *db = connect_db_with_retry(args).await;
            }
            Ok(Err(e)) => {
                error!("Failed to insert record into '{table}': {e}");
                return None;
            }
            Err(_) => {
                warn!("Database operation on '{table}' timed out after {DB_OP_TIMEOUT:?}, reconnecting");
                *db = connect_db_with_retry(args).await;
            }
        }
    }
}

/// Attempts to deserialize the provided string into the specified type, logging any failures along the way so I can mitigate repeated code and just move on if the returned value is None.
/// `None` implies that something failed
fn extract<T>(p: Option<&str>) -> Option<T>
where T: DeserializeOwned {
    match p {
        Some(part) =>  {
            match serde_plain::from_str::<T>(part) {
                Ok(part) => Some(part),
                Err(_) => {
                    warn!("Failed to deserialize column into requested type: {p:?}");
                    None
                }
            }
        },
        None => {
            warn!("Failed to extract message column: {p:?}");
            None
        }
    }
}

#[derive(Debug, Parser)]
struct Args {
    /// The path to the input file
    input_path: PathBuf,
    /// The surrealdb instance hostname
    #[arg(long, env = "SC_DB_URL")]
    database_url: String,
    /// The surrealdb database
    #[arg(long, env = "SC_DB_NAME")]
    db: String,
    /// The surrealdb namespace
    #[arg(long, env = "SC_DB_NS")]
    ns: String,
    /// The username of the database user
    #[arg(long, env = "SC_DB_USERNAME")]
    username: String,
    /// The password of the database user
    #[arg(long, env = "SC_DB_PASSWORD")]
    password: String,
}

/// A FLEX_NEXT message
/// As of multimon v1.5.0, the structure is `FLEX_NEXT|timestamp+tz|baud/levels|cycle.frame.phase|capcode|addr|TYPE|flags|message`
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct FlexNextRecord {
    timestamp: DateTime<Utc>,
    /// Baud rate and modulation level
    baud_level: String,
    /// Cycle, frame, and phase
    cfp: String,
    capcode: u64,
    #[surreal(wrap)]
    addr: char,
    /// The type of message
    #[surreal(wrap)]
    msg_type: FlexNextType,
    /// The message flags
    flags: String,
    /// The message body itself
    message: String,
}
/// The FLEX_NEXT message variants
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
enum FlexNextType {
    /// Alphanumeric
    ALN,
    /// Numeric
    NUM,
    SNUM,
    NNUM,
    /// Hexadecimal
    HEX,
    SMSG,
    INS,
    FLEXTIME,
    #[serde(alias = "SEC:ALN", alias = "SEC:BIN", alias = "SEC:VEN", alias = "SEC:RSV")]
    SEC
}

/// A FLEX message
/// As of multimon v1.5.0, the structure for an alphanumeric flex message is `FLEX|timestamp|baud/level/frag_flag/phase|cycle.frame/capcode/ALN/message`
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct FlexRecord {
    timestamp: DateTime<Utc>,
    /// Baud rate, modulation level, fragmentation flag, phase
    #[surreal(rename = "blfp")]
    baud_level_frag_phase: String,
    /// Cycle and frame numbers
    cf: String,
    capcode: u64,
    /// The type of message
    #[surreal(wrap)]
    msg_type: FlexType,
    /// The message body itself
    message: String
}
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
enum FlexType {
    /// Alphanumeric
    ALN
}
