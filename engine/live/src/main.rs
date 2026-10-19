use engine_core::{
    ENGINE_VERSION, SECOND,
    session::{Checkpoint, Cutoff, Queues, TICK_DELAY_NS, is_carry_in},
};
use engine_replay::{
    Result, error,
    input::day_bounds,
    state_io::{self, utc_date},
};
use rdkafka::{
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext},
};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn stop(_: i32) {
    STOP.store(true, Ordering::Relaxed);
}
#[cfg(unix)]
fn install_signals() -> Result<()> {
    // POSIX signal handler only stores a lock-free flag. SIGINT=2, SIGTERM=15 on
    // the supported Unix targets; no allocator, locks, or I/O run in the handler.
    unsafe extern "C" {
        fn signal(sig: i32, handler: usize) -> usize;
    }
    for sig in [2, 15] {
        if unsafe { signal(sig, stop as *const () as usize) } == usize::MAX {
            return Err(error("signal registration failed"));
        }
    }
    Ok(())
}
#[cfg(not(unix))]
fn install_signals() -> Result<()> {
    Err(error("engine-live currently requires Unix signals"))
}
fn now() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}
struct Delivery {
    failed: AtomicBool,
}
impl ClientContext for Delivery {}
impl ProducerContext for Delivery {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        if let Err((e, _)) = result {
            eprintln!("Kafka output delivery failed: {e}");
            self.failed.store(true, Ordering::Relaxed);
        }
    }
}
fn send(producer: &BaseProducer<Delivery>, topic: &str, key: &str, payload: &str) -> Result<()> {
    if !matches!(topic, "features.1s.v1" | "engine.cutoffs.v1") {
        return Err(error("unauthorized output topic"));
    }
    producer
        .send(BaseRecord::to(topic).partition(0).key(key).payload(payload))
        .map_err(|(e, _)| e)?;
    Ok(())
}
fn flush(producer: &BaseProducer<Delivery>) -> Result<()> {
    producer.flush(Duration::from_secs(10))?;
    if producer.context().failed.load(Ordering::Relaxed) {
        return Err(error("Kafka output delivery failed; stopping"));
    }
    Ok(())
}
struct DayFiles {
    date: String,
    cutoffs: File,
    carry: File,
    last_sync: Instant,
}
impl DayFiles {
    fn open(root: &Path, date: &str) -> Result<Self> {
        let dir = root.join("engine").join(format!("date={date}"));
        fs::create_dir_all(&dir)?;
        Ok(Self {
            date: date.into(),
            cutoffs: OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("cutoffs.jsonl"))?,
            carry: OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("carry_in.jsonl"))?,
            last_sync: Instant::now(),
        })
    }
    fn sync(&mut self) -> Result<()> {
        self.carry.sync_all()?;
        self.cutoffs.sync_all()?;
        self.last_sync = Instant::now();
        Ok(())
    }
}
fn main() {
    if let Err(e) = run() {
        eprintln!("engine-live: {e}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let mut root = PathBuf::from("data");
    let mut run_seconds = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                root = PathBuf::from(args.next().ok_or_else(|| error("--root requires path"))?)
            }
            "--run-seconds" => {
                run_seconds = Some(
                    args.next()
                        .ok_or_else(|| error("--run-seconds requires integer"))?
                        .parse::<u64>()?,
                )
            }
            "--help" => {
                println!(
                    "engine-live [--root data] [--run-seconds N]\nKafka is restricted to localhost:9092; startup assigns raw.* at latest, with no commits."
                );
                return Ok(());
            }
            _ => return Err(error(format!("unknown argument {arg}"))),
        }
    }
    install_signals()?;
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "engine-live")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "error")
        .set("allow.auto.create.topics", "false")
        .set("fetch.wait.max.ms", "10")
        .set("queued.max.messages.kbytes", "65536")
        .create()?;
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;
    let mut assignment = TopicPartitionList::new();
    let mut processed = std::collections::BTreeMap::new();
    for topic in metadata
        .topics()
        .iter()
        .filter(|t| t.name().starts_with("raw."))
    {
        for partition in topic.partitions() {
            let (_, end) =
                consumer.fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(10))?;
            assignment.add_partition_offset(topic.name(), partition.id(), Offset::Offset(end))?;
            processed.insert(format!("{}/{}", topic.name(), partition.id()), end - 1);
        }
    }
    if processed.len() != 46 {
        return Err(error(format!(
            "expected 46 raw partitions, found {}",
            processed.len()
        )));
    }
    consumer.assign(&assignment)?;
    let producer: BaseProducer<Delivery> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "0")
        .set("message.timeout.ms", "10000")
        .create_with_context(Delivery {
            failed: AtomicBool::new(false),
        })?;
    let started = Instant::now();
    let startup_tick = (now()? + SECOND - 1).div_euclid(SECOND) * SECOND;
    let mut state = Checkpoint::new(startup_tick, processed);
    let date = utc_date(startup_tick);
    let startup_path = root
        .join("engine")
        .join(format!("date={date}"))
        .join(format!("start_state_{startup_tick}.json"));
    state_io::save(&startup_path, &state)?;
    if startup_tick.rem_euclid(86_400 * SECOND) == 0 {
        state_io::save(&startup_path.with_file_name("start_state.json"), &state)?;
    }
    let mut day = DayFiles::open(&root, &date)?;
    let mut metrics = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(startup_path.with_file_name(format!("metrics_{startup_tick}.jsonl")))?;
    println!(
        "{}",
        serde_json::json!({"event":"started","pid":std::process::id(),"startup_tick":startup_tick,"state":startup_path,"partitions":46,"engine_version":ENGINE_VERSION})
    );
    let mut queues = Queues::default();
    let mut rows_written = 0u64;
    let mut ticks = 0u64;
    while !STOP.load(Ordering::Relaxed)
        && run_seconds.is_none_or(|limit| started.elapsed().as_secs() < limit)
    {
        let deadline = state.next_tick_ns + TICK_DELAY_NS;
        let wall = now()?;
        if wall < deadline {
            let timeout =
                Duration::from_nanos((deadline - wall) as u64).min(Duration::from_millis(25));
            if let Some(message) = consumer.poll(timeout) {
                let message = message?;
                queues.push(engine_live::archived(
                    message
                        .payload()
                        .ok_or_else(|| error("empty raw message"))?,
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )?)?;
            }
            continue;
        }
        // poll(0) drains only the finite batch already fetched at the cutoff. Limit
        // this drain so sustained input can never postpone a scheduled tick forever.
        for _ in 0..100_000 {
            let Some(message) = consumer.poll(Duration::ZERO) else {
                break;
            };
            let message = message?;
            queues.push(engine_live::archived(
                message
                    .payload()
                    .ok_or_else(|| error("empty raw message"))?,
                message.topic(),
                message.partition(),
                message.offset(),
            )?)?;
        }
        let tick = state.next_tick_ns;
        let date = utc_date(tick);
        if date != day.date {
            day.sync()?;
            state_io::save(
                &root
                    .join("engine")
                    .join(format!("date={date}"))
                    .join("start_state.json"),
                &state,
            )?;
            day = DayFiles::open(&root, &date)?;
        }
        let day_start = day_bounds(&date)?.0;
        let mut processed = state.last_cutoff.processed.clone();
        let batch = queues.take(tick, &mut processed);
        let count = batch.len();
        let mut records = Vec::with_capacity(count);
        for queued in batch {
            if is_carry_in(&queued.record, day_start) {
                day.carry.write_all(queued.line.as_bytes())?;
            }
            records.push(queued.record);
        }
        let cutoff = Cutoff {
            tick_ns: tick,
            engine_version: ENGINE_VERSION.into(),
            processed,
        };
        let rows = state.apply(cutoff.clone(), records)?;
        let mut latency_ns = 0;
        for mut row in rows {
            row.available_ns = now()?;
            latency_ns = latency_ns.max(row.available_ns - tick);
            let key = format!(
                "{}/{}/{}/{}/{}",
                row.exchange, row.market, row.symbol, tick, ENGINE_VERSION
            );
            send(
                &producer,
                "features.1s.v1",
                &key,
                &serde_json::to_string(&row)?,
            )?;
            rows_written += 1;
        }
        flush(&producer)?; // publish cutoff only after every feature row is acknowledged.
        let line = serde_json::to_string(&cutoff)?;
        send(&producer, "engine.cutoffs.v1", &tick.to_string(), &line)?;
        flush(&producer)?;
        writeln!(day.cutoffs, "{line}")?;
        writeln!(
            metrics,
            "{}",
            serde_json::json!({"tick_ns":tick,"records":count,"emit_latency_ns":latency_ns,"rows_total":rows_written,"records_total":state.engine.records})
        )?;
        ticks += 1;
        if day.last_sync.elapsed() >= Duration::from_secs(2) {
            day.sync()?;
            metrics.sync_all()?;
        }
    }
    flush(&producer)?;
    day.sync()?;
    metrics.sync_all()?;
    println!(
        "{}",
        serde_json::json!({"event":"stopped","ticks":ticks,"rows":rows_written,"records":state.engine.records,"wall_s":started.elapsed().as_secs_f64(),"last_tick":state.last_cutoff.tick_ns,"book_breaks":state.engine.book_breaks})
    );
    Ok(())
}
