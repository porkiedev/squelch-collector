#![allow(unused)]

use std::path::PathBuf;
use tracing::{debug, error, info, trace, warn, Level};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use tracing_subscriber::prelude::*;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_json::json;
use serde_plain::Error;
use surrealdb::engine::remote::ws::Wss;
use surrealdb::Surreal;
use surrealdb::types::{RecordId, SurrealValue};
use tokio::io::{stdin, AsyncBufReadExt, AsyncRead, BufReader};
use tokio::fs::File;

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

    // Connect and authenticate with the database
    let db = Surreal::new::<Wss>(args.database_url).await?;
    db.signin(surrealdb::opt::auth::Database {
        namespace: args.ns,
        database: args.db,
        username: args.username,
        password: args.password,
    }).await?;

    // Open the file/stdin
    let reader = BufReader::new(open_input(args.input_path).await?);
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

            // Try to insert the record into the database
            let r = match db.create::<Option<RecordId>>("flex_next_messages").content(msg).await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to insert FlexNext pager into database: {e}");
                    continue;
                }
            };
            let Some(r) = r else { continue };

            // let Some(created) = db.create::<Option<RecordId>>("flex_next_messages").content(msg).await? else { continue };
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

            // Try to insert the record into the database
            let r = match db.create::<Option<RecordId>>("flex_messages").content(msg).await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to insert Flex pager into database: {e}");
                    continue;
                }
            };
            let Some(r) = r else { continue };
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
#[derive(Debug, Serialize, Deserialize, SurrealValue)]
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
#[derive(Debug, Serialize, Deserialize, SurrealValue)]
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
#[derive(Debug, Serialize, Deserialize, SurrealValue)]
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
#[derive(Debug, Serialize, Deserialize, SurrealValue)]
enum FlexType {
    /// Alphanumeric
    ALN
}
