// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only end-to-end integration tests for the ETW receiver.
//!
//! These tests build the receiver from its JSON config, register real ETW
//! providers, emit TraceLogging events through both the dynamic
//! (`tracelogging_dynamic`) and static (`tracelogging`) producer crates,
//! and validate that the receiver decodes them into OTAP Arrow records
//! with the expected structure and values. A second test emulates a .NET
//! manifest-mode `EventSource` (in-band manifest chunks + payload events)
//! and validates in-band manifest decoding.
//!
//! The tests are `#[ignore]` because they require Administrator privileges
//! and create a real-time ETW kernel session. They share one ETW session
//! GUID, so run them one at a time. Run locally from an elevated
//! PowerShell:
//!
//! ```pwsh
//! cargo test -p otel-arrow-dfe-contrib-nodes --features etw `
//!     etw_receiver_decodes_ -- --ignored --nocapture --test-threads=1
//! ```

use super::*;
use arrow::array::Array;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::testing::{receiver::TestRuntime, test_node, test_pipeline_ctx};
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::schema::consts;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::time;
use tracelogging_dynamic as tld;

// Static `tracelogging` provider -- must live at module scope since
// `define_provider!` declares a `static` symbol.
tracelogging::define_provider!(STATIC_PROVIDER, "OtapEtwE2E.Static");

const STATIC_PROVIDER_NAME: &str = "OtapEtwE2E.Static";
const DYNAMIC_EVENT_NAME: &str = "E2eDynamicEvent";
const GUID_DYNAMIC_EVENT_NAME: &str = "E2eGuidDynamicEvent";
/// Must stay in sync with the string literal passed to `write_event!` in
/// [`emit_static_event`] -- the `tracelogging::write_event!` macro requires
/// the event name to be a compile-time string literal.
const STATIC_EVENT_NAME: &str = "E2eStaticEvent";

/// Scenario: Named and GUID-only TraceLogging subscriptions capture dynamic
/// and static events through the same real Windows ETW session.
/// Guarantees: Named subscriptions emit their configured provider name;
/// unregistered GUID subscriptions omit only that name while preserving the
/// exact provider GUID and decoded payload fields.
#[test]
#[ignore = "requires Administrator privileges and creates a real ETW kernel session; run explicitly with `-- --ignored`"]
fn etw_receiver_decodes_tracelogging_events_end_to_end() {
    // Without Administrator rights, `StartTraceA` fails with
    // ERROR_ACCESS_DENIED on a detached thread -- `subscribe()` still
    // returns Ok and the only symptom is that `provider.enabled(..)`
    // never flips true, looking like a generic timeout.
    assert!(
        is_process_elevated(),
        "ETW receiver e2e test requires Administrator privileges. \
         Re-run from an elevated PowerShell (right-click `PowerShell` -> \
         `Run as administrator`)."
    );

    // Per-PID names make leaked sessions easy to attribute to the
    // process that left them behind.  The static provider's name is
    // fixed (it's a compile-time literal in `define_provider!`).
    let pid = std::process::id();
    let dynamic_provider_name = format!("OtapEtwE2E.Dyn.{pid}");
    let guid_dynamic_provider_name = format!("OtapEtwE2E.GuidDyn.{pid}");
    let session_name = format!("OtapEtwE2E-{pid}");

    // Stop every leftover `OtapEtwE2E-*` session on the box.  We can't
    // narrow this to our own per-PID `session_name`: `EtwSession::new()`
    // in `one_collect` uses a fixed internal session GUID, so any
    // session created by a previous run of this test -- regardless of
    // its name -- occupies that GUID and makes `StartTraceW` fail with
    // `ERROR_ALREADY_EXISTS` (183).  The same constraint means two
    // copies of this test physically cannot run concurrently on the
    // same machine, so the broad cleanup never disrupts a peer that
    // would otherwise have succeeded.
    cleanup_stale_otap_etw_sessions();

    // Subscribe to all providers in a single session. Use default
    // batching so we exercise the production flush path; a single event
    // flushes via the default timer (~100ms).
    let config_json = serde_json::json!({
        "session_name": session_name,
        "providers": [
            { "name": dynamic_provider_name, "kind": "tracelogging", "level": "verbose" },
            { "guid": session::CanonicalGuid::from(one_collect::guid_from_provider_name(&guid_dynamic_provider_name)).to_string(), "level": "verbose" },
            { "guid": session::CanonicalGuid::from(one_collect::guid_from_provider_name(STATIC_PROVIDER_NAME)).to_string(), "level": "verbose" },
        ],
    });

    let (pipeline_ctx, _registry) = test_pipeline_ctx();
    let receiver = EtwReceiver::from_config(pipeline_ctx, &config_json)
        .expect("EtwReceiver::from_config should succeed (Administrator required)");

    let test_runtime = TestRuntime::<OtapPdata>::new();
    let node_config = Arc::new(NodeUserConfig::new_receiver_config(ETW_RECEIVER_URN));
    let receiver_wrapper = ReceiverWrapper::local(
        receiver,
        test_node(test_runtime.config().name.clone()),
        node_config,
        test_runtime.config(),
    );

    test_runtime
        .set_receiver(receiver_wrapper)
        .run_test(producer_scenario(
            dynamic_provider_name.clone(),
            guid_dynamic_provider_name.clone(),
        ))
        .run_validation(producer_validation(
            dynamic_provider_name,
            guid_dynamic_provider_name,
        ));
}

// -- Producer ------------------------------------------------------------

/// Test scenario: register all providers, emit one event from each,
/// then signal `DrainIngress` + `Shutdown` so the validation phase sees
/// the batch.
fn producer_scenario(
    dynamic_provider_name: String,
    guid_dynamic_provider_name: String,
) -> impl FnOnce(
    otel_arrow_dfe_engine::testing::receiver::TestContext<OtapPdata>,
) -> Pin<Box<dyn Future<Output = ()>>> {
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    move |ctx| {
        Box::pin(async move {
            // Catch panics so we can always send `Shutdown` afterwards.
            // Without this, a failed assertion would leave the receiver
            // running (its `parse_until` callback never stops on its own)
            // and `TestRuntime::run_validation` would hang forever
            // instead of surfacing the panic.
            let outcome = AssertUnwindSafe(run_producer_body(
                &dynamic_provider_name,
                &guid_dynamic_provider_name,
            ))
            .catch_unwind()
            .await;

            // Best-effort cleanup of the static provider: `run_producer_body`
            // only unregisters on the happy path, so a panic before that
            // point would otherwise leak the provider registration for the
            // remainder of the test binary and interfere with any later
            // tests in the same process.  `unregister()` is idempotent at
            // the Win32 level, so calling it again on the happy path is a
            // harmless no-op.
            let _ = STATIC_PROVIDER.unregister();

            // `DrainIngress` first so the receiver flushes any in-flight
            // ETW events from its MPSC channel and the pending Arrow
            // batch downstream -- this is what the engine sends in
            // production before `Shutdown`.  A bare `Shutdown` would
            // cause the receiver to drop events that arrived but had
            // not yet been timer-flushed.  Ignore send errors (the
            // control channel may already be closed).
            let drain_deadline = Instant::now() + Duration::from_secs(5);
            let _ = ctx
                .send_control_msg(NodeControlMsg::DrainIngress {
                    deadline: drain_deadline,
                    reason: "Test drain".to_owned(),
                })
                .await;
            let _ = ctx.send_shutdown(Instant::now(), "Test").await;

            if let Err(panic) = outcome {
                std::panic::resume_unwind(panic);
            }
        })
    }
}

async fn run_producer_body(dynamic_provider_name: &str, guid_dynamic_provider_name: &str) {
    // -- Dynamic provider (`tracelogging_dynamic`) -----------------------
    let dynamic_provider = Box::pin(tld::Provider::new(
        dynamic_provider_name,
        &tld::Provider::options(),
    ));
    // SAFETY: `dynamic_provider` is pinned and lives until the end of
    // this async block; it auto-unregisters on drop.
    #[allow(unsafe_code)]
    let r = unsafe { dynamic_provider.as_ref().register() };
    assert_eq!(r, 0, "dynamic Provider::register returned errno {r}");

    let guid_dynamic_provider = Box::pin(tld::Provider::new(
        guid_dynamic_provider_name,
        &tld::Provider::options(),
    ));
    // SAFETY: The provider stays pinned until this function exits and
    // automatically unregisters on drop, including during unwinding.
    #[allow(unsafe_code)]
    let r = unsafe { guid_dynamic_provider.as_ref().register() };
    assert_eq!(r, 0, "GUID dynamic Provider::register returned errno {r}");

    // -- Static provider (`tracelogging`) --------------------------------
    // SAFETY: this is an EXE-style test binary; process exit triggers
    // `unregister` for any provider still registered.  We also call
    // `unregister()` at the end of this function on the happy path.
    #[allow(unsafe_code)]
    let r = unsafe { STATIC_PROVIDER.register() };
    assert_eq!(r, 0, "static Provider::register returned errno {r}");

    // Wait for all providers to be enabled by the kernel session. 10s
    // is generous; in practice this flips within a few hundred ms on a
    // healthy machine.
    wait_until_enabled(
        || dynamic_provider.enabled(tld::Level::Verbose, 0),
        dynamic_provider_name,
    )
    .await;
    wait_until_enabled(
        || guid_dynamic_provider.enabled(tld::Level::Verbose, 0),
        guid_dynamic_provider_name,
    )
    .await;
    wait_until_enabled(
        || STATIC_PROVIDER.enabled(tracelogging::Level::Verbose, 0),
        STATIC_PROVIDER_NAME,
    )
    .await;

    emit_dynamic_event(&dynamic_provider, DYNAMIC_EVENT_NAME);
    emit_dynamic_event(&guid_dynamic_provider, GUID_DYNAMIC_EVENT_NAME);
    emit_static_event();

    // Wait long enough for the kernel ETW buffer-flush timer to hand our
    // events to `ProcessTrace` and for the receiver's default batch
    // timer (~100ms) to forward them downstream.  Real-time sessions
    // deliver events in batches driven by the kernel timer (default
    // ~1s), not synchronously with `EventBuilder::write`.  5s is
    // conservative.
    time::sleep(Duration::from_secs(5)).await;

    let _ = STATIC_PROVIDER.unregister();
}

async fn wait_until_enabled(mut check: impl FnMut() -> bool, provider_name: &str) {
    for _ in 0..200 {
        if check() {
            return;
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    // Snapshot of running ETW sessions tells us whether the kernel
    // session started and whether a stale one is blocking us.
    print_running_etw_sessions();
    panic!(
        "ETW provider {provider_name} was not enabled within 10s. Common causes:\n\
         - the test is not running with Administrator privileges \
         (StartTraceA / EnableTraceEx2 require elevation);\n\
         - a stale ETW session is blocking the new one. Check the \
         `logman query -ets` output above and stop any leftover \
         `OtapEtwE2E-*` session via `logman stop <name> -ets`."
    );
}

/// Emit one TraceLogging event populated with the full breadth of scalar
/// in-types that one_collect's TDH decoder handles, so the validator can
/// pin down every branch in the receiver's value translation.
fn emit_dynamic_event(provider: &Pin<Box<tld::Provider>>, event_name: &str) {
    let r = tld::EventBuilder::new()
        .reset(event_name, tld::Level::Informational, 0x1, 0)
        // Unsigned and signed integers.
        .add_u32("MyAnswer", 42u32, tld::OutType::Default, 0)
        .add_u64("MyBigCount", 1_000_000_000_001u64, tld::OutType::Default, 0)
        .add_i32("MyDelta", -7i32, tld::OutType::Default, 0)
        // Floating point.
        .add_f64("MyPi", core::f64::consts::PI, tld::OutType::Default, 0)
        // Two distinct boolean encodings, exercised side by side so we lock
        // in both decoder mappings:
        //   * 4-byte Win32 `BOOL` / TraceLogging `Bool32` (`TDH_INTYPE_BOOLEAN`)
        //     -> one_collect maps it to `"u32"` -> `Int(1)`.
        //   * 1-byte boolean (`TDH_INTYPE_UINT8` + `OutType::Boolean`)
        //     -> one_collect maps it to `"u8"` -> `Int(1)`.
        .add_bool32("MyBool32Flag", 1, tld::OutType::Boolean, 0)
        .add_u8("MyU8Flag", 1u8, tld::OutType::Boolean, 0)
        // UTF-8 and UTF-16 string fields.
        .add_str8("MyMessage", b"hello-from-test", tld::OutType::Default, 0)
        .add_str16(
            "MyWideMessage",
            "wide\u{2603}"
                .encode_utf16()
                .collect::<Vec<u16>>()
                .as_slice(),
            tld::OutType::Default,
            0,
        )
        // FILETIME and GUID -- opaque scalar types with dedicated decoder
        // paths (filetime -> unix-epoch ns; guid -> hex string).
        .add_filetime(
            "MyFiletime",
            // 2024-01-01 00:00:00 UTC, as 100-ns ticks since 1601-01-01 UTC.
            133_485_408_000_000_000i64,
            tld::OutType::Default,
            0,
        )
        .add_guid(
            "MyGuid",
            &tld::Guid::from_name("OtapEtwE2E.NestedGuid"),
            tld::OutType::Default,
            0,
        )
        // Nested struct.  `add_struct(name, field_count, tag)` declares
        // a parent field; the next `field_count` `add_*` calls become
        // its children.  The one_collect TDH decoder flattens nested
        // fields into dotted names (`"MyNested.NestedAnswer"`,
        // `"MyNested.NestedFlag"`) -- see the `DecodedField::name` doc
        // comment in `session.rs`.
        .add_struct("MyNested", 2, 0)
        .add_u32("NestedAnswer", 99u32, tld::OutType::Default, 0)
        .add_str8(
            "NestedMessage",
            b"nested-from-dynamic",
            tld::OutType::Default,
            0,
        )
        .write(provider, None, None);
    assert_eq!(r, 0, "dynamic EventBuilder::write returned errno {r}");
}

/// Emit a small event through the static `tracelogging` macro path.
/// Coverage is intentionally minimal -- the producer is what's being
/// exercised here; the receiver/decoder code path is the same as for
/// the dynamic event.
fn emit_static_event() {
    let answer: u32 = 123;
    let flag: bool = true;
    let flag32: i32 = 1;
    let nested_answer: u32 = 456;
    let r = tracelogging::write_event!(
        STATIC_PROVIDER,
        "E2eStaticEvent", // == STATIC_EVENT_NAME (macro requires a string literal)
        level(Informational),
        keyword(0x1),
        u32("StaticAnswer", &answer),
        str8("StaticMessage", "hello-from-static".as_bytes()),
        // Two boolean encodings, mirroring the dynamic event:
        //   * `bool8` is a 1-byte boolean (`U8` + `Boolean`) -> decoded as "u8".
        //   * `bool32` is a 4-byte Win32 `BOOL` (`Bool32`) -> decoded as "u32".
        bool8("StaticFlag", &flag),
        bool32("StaticFlag32", &flag32),
        // Nested struct via the `struct("Name", { ... })` DSL.  The
        // decoder flattens these to `"StaticNested.NestedAnswer"` etc.
        struct("StaticNested", {
            u32("NestedAnswer", &nested_answer),
            str8("NestedMessage", "nested-from-static".as_bytes()),
        }),
    );
    assert_eq!(r, 0, "static write_event! returned errno {r}");
}

// -- Validation ----------------------------------------------------------

/// Drain downstream pdata until we have batches containing all three events,
/// then run comprehensive assertions on each.
fn producer_validation(
    dynamic_provider_name: String,
    guid_dynamic_provider_name: String,
) -> impl FnOnce(
    otel_arrow_dfe_engine::testing::receiver::NotSendValidateContext<OtapPdata>,
) -> Pin<Box<dyn Future<Output = ()>>> {
    move |mut ctx| {
        Box::pin(async move {
            // The kernel session may surface unrelated events while
            // enabling the providers, and our three events may end up in
            // one batch or consecutive batches depending on flush
            // timing. Collect everything until we've seen all three
            // event_names or hit the deadline.  15s safety margin for
            // busy CI runners.
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut batches: Vec<OtapArrowRecords> = Vec::new();
            let our_pid = std::process::id();

            loop {
                if has_event(&batches, DYNAMIC_EVENT_NAME, our_pid)
                    && has_event(&batches, STATIC_EVENT_NAME, our_pid)
                    && has_event(&batches, GUID_DYNAMIC_EVENT_NAME, our_pid)
                {
                    break;
                }
                if Instant::now() >= deadline {
                    break;
                }
                match time::timeout(Duration::from_millis(500), ctx.recv()).await {
                    Ok(Ok(pdata)) => {
                        let records = pdata
                            .payload_ref()
                            .otap_ref()
                            .expect("expected native OTAP payload from ETW receiver");
                        batches.push(records.clone());
                    }
                    Ok(Err(_)) | Err(_) => { /* channel error or timeout: keep polling */ }
                }
            }

            let (dyn_records, dyn_row) = locate_event(&batches, DYNAMIC_EVENT_NAME, our_pid);
            let (stc_records, stc_row) = locate_event(&batches, STATIC_EVENT_NAME, our_pid);

            assert_log_record_common(
                dyn_records,
                dyn_row,
                DYNAMIC_EVENT_NAME,
                &dynamic_provider_name,
                Some(&dynamic_provider_name),
            );
            assert_dynamic_user_attrs(dyn_records, dyn_row);

            assert_log_record_common(
                stc_records,
                stc_row,
                STATIC_EVENT_NAME,
                STATIC_PROVIDER_NAME,
                None,
            );
            assert_static_user_attrs(stc_records, stc_row);

            let (guid_records, guid_row) = locate_event(&batches, GUID_DYNAMIC_EVENT_NAME, our_pid);
            assert_log_record_common(
                guid_records,
                guid_row,
                GUID_DYNAMIC_EVENT_NAME,
                &guid_dynamic_provider_name,
                None,
            );
            assert_dynamic_user_attrs(guid_records, guid_row);
        })
    }
}

fn has_event(batches: &[OtapArrowRecords], event_name: &str, expected_pid: u32) -> bool {
    batches
        .iter()
        .any(|r| find_event_row(r, event_name, expected_pid).is_some())
}

fn locate_event<'a>(
    batches: &'a [OtapArrowRecords],
    event_name: &str,
    expected_pid: u32,
) -> (&'a OtapArrowRecords, usize) {
    batches
        .iter()
        .find_map(|r| find_event_row(r, event_name, expected_pid).map(|row| (r, row)))
        .unwrap_or_else(|| {
            panic!(
                "did not receive an Arrow batch containing event '{event_name}' \
                 from PID {expected_pid} via the ETW receiver within 15s"
            )
        })
}

/// Locate a Logs row whose `event_name` matches AND whose injected
/// `etw.process.id` attribute equals `expected_pid`.  Matching on PID in
/// addition to name prevents a concurrent test run (which would emit an
/// identically-named `STATIC_EVENT_NAME` from the same fixed static
/// provider) from being picked up as ours.
fn find_event_row(records: &OtapArrowRecords, expected: &str, expected_pid: u32) -> Option<usize> {
    let logs_rb = records.get(ArrowPayloadType::Logs)?;
    let attrs_rb = records.get(ArrowPayloadType::LogAttrs)?;
    let names = string_column(logs_rb, consts::EVENT_NAME);
    let log_ids = u16_column(logs_rb, consts::ID);
    let expected_pid_i64 = i64::from(expected_pid);

    for (row, name) in names.iter().enumerate() {
        if name.as_deref() != Some(expected) {
            continue;
        }
        let Some(log_id) = log_ids[row] else {
            continue;
        };
        let pid_attr = collect_attributes(attrs_rb, log_id)
            .into_iter()
            .find(|(k, _)| k == "etw.process.id")
            .map(|(_, v)| v);
        if let Some(AttrSnapshot::Int(pid)) = pid_attr
            && pid == expected_pid_i64
        {
            return Some(row);
        }
    }
    None
}

/// Field-by-field validation of the Logs/Resource/Scope columns and the
/// receiver-injected `etw.*` attributes -- the parts that are identical
/// for every event regardless of how it was produced.
fn assert_log_record_common(
    records: &OtapArrowRecords,
    row: usize,
    event_name: &str,
    provider_name: &str,
    expected_provider_name: Option<&str>,
) {
    let logs_rb = records
        .get(ArrowPayloadType::Logs)
        .expect("Logs payload should be present");
    let attrs_rb = records
        .get(ArrowPayloadType::LogAttrs)
        .expect("LogAttrs payload should be present");

    // -- Logs columns ----------------------------------------------------
    let names = string_column(logs_rb, consts::EVENT_NAME);
    assert_eq!(
        names[row].as_deref(),
        Some(event_name),
        "event_name mismatch at row {row}: got {:?}",
        names[row]
    );

    // body: ETW events have no body -- encoder appends Null for every
    // row, and `LogsBodyBuilder::finish()` returns None when all rows
    // are null, so the column is omitted from the schema entirely.
    assert!(
        logs_rb.column_by_name(consts::BODY).is_none(),
        "expected `body` column to be omitted (all-null), found {:?}",
        logs_rb.column_by_name(consts::BODY).map(|c| c.data_type())
    );

    // severity: producer wrote `Informational` (== ETW level 4), which
    // the encoder maps to OTel INFO (severity_number 9). `SeverityText`
    // carries the original ETW level name ("INFO") as known at the
    // source, per the OpenTelemetry logs data model ETW mapping.
    let severity_number = i32_column(logs_rb, consts::SEVERITY_NUMBER);
    assert_eq!(
        severity_number[row],
        Some(9),
        "severity_number at row {row}: expected Some(9 = INFO), got {:?}",
        severity_number[row]
    );
    let severity_text = string_column(logs_rb, consts::SEVERITY_TEXT);
    assert_eq!(
        severity_text[row].as_deref(),
        Some("INFO"),
        "severity_text at row {row}: expected Some(\"INFO\"), got {:?}",
        severity_text[row]
    );

    // time_unix_nano: receiver converts QPC ticks to Unix epoch ns; we
    // can't predict the exact value but it must be a recent wall-clock
    // instant (within the last hour).
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is past Unix epoch")
        .as_nanos() as i64;
    let one_hour_ns: i64 = 3_600 * 1_000_000_000;
    let time_unix_nano = timestamp_ns_column(logs_rb, consts::TIME_UNIX_NANO);
    let ts = time_unix_nano[row].expect("time_unix_nano must be present");
    assert!(
        (now_ns - one_hour_ns..=now_ns).contains(&ts),
        "time_unix_nano at row {row} = {ts} is not within the last hour (now = {now_ns})"
    );

    // observed_time_unix_nano: encoder sets this to "now" at flush time,
    // so it must be >= time_unix_nano and <= now.
    let observed = timestamp_ns_column(logs_rb, consts::OBSERVED_TIME_UNIX_NANO);
    let obs = observed[row].expect("observed_time_unix_nano must be present");
    assert!(
        obs >= ts && obs <= now_ns,
        "observed_time_unix_nano at row {row} = {obs} must satisfy \
         time_unix_nano ({ts}) <= observed <= now ({now_ns})"
    );

    // trace_id / span_id / flags / schema_url / dropped_attributes_count:
    // ETW receiver does not populate these.  The optional UInt32
    // builder elides columns whose every row equals the default value
    // (0), so dropped_attributes_count may appear as `Some(0)` or be
    // absent -- both are semantically equivalent.
    assert_all_null_at(logs_rb, consts::TRACE_ID, row);
    assert_all_null_at(logs_rb, consts::SPAN_ID, row);
    assert_all_null_at(logs_rb, consts::FLAGS, row);
    assert_all_null_at(logs_rb, consts::SCHEMA_URL, row);
    assert_u32_zero_or_null(logs_rb, consts::DROPPED_ATTRIBUTES_COUNT, row);

    // -- Resource & Scope structs ----------------------------------------
    // All defaulted fields; same elision behavior as above.
    let resource = struct_column(logs_rb, consts::RESOURCE);
    assert_u16_struct_field_zero_or_null(&resource, consts::ID, row);
    assert!(
        struct_field_is_null(&resource, consts::SCHEMA_URL, row),
        "resource.schema_url at row {row}: expected null"
    );
    assert_u32_struct_field_zero_or_null(&resource, consts::DROPPED_ATTRIBUTES_COUNT, row);

    let scope = struct_column(logs_rb, consts::SCOPE);
    assert_u16_struct_field_zero_or_null(&scope, consts::ID, row);
    assert!(
        struct_field_is_null(&scope, consts::NAME, row),
        "scope.name at row {row}: expected null"
    );
    assert!(
        struct_field_is_null(&scope, consts::VERSION, row),
        "scope.version at row {row}: expected null"
    );
    assert_u32_struct_field_zero_or_null(&scope, consts::DROPPED_ATTRIBUTES_COUNT, row);

    // -- Receiver-injected `etw.*` attributes ----------------------------
    let log_id =
        u16_column(logs_rb, consts::ID)[row].expect("log id must be present at the matched row");
    let attrs = collect_attributes(attrs_rb, log_id);

    // TraceLogging events have no manifest, so id/opcode/version are 0.
    assert_attr_int(&attrs, "etw.event.id", 0);
    assert_attr_int(&attrs, "etw.opcode", 0);
    assert_attr_int(&attrs, "etw.version", 0);
    // TraceLogging unconditionally OR-s a high-bit metadata marker
    // (`0x8000_0000_0000_0000`) into the keyword mask alongside the
    // application's bits.  The encoder saturates u64 -> i64, so the
    // value clamps to `i64::MAX` whenever bit 63 is set.
    assert_attr_keywords_includes_bit(&attrs, "etw.keywords", 0x1);
    assert_attr_int(&attrs, "etw.process.id", i64::from(std::process::id()));
    assert!(
        attrs.iter().any(|(k, _)| k == "etw.thread.id"),
        "expected attribute key 'etw.thread.id' in LogAttrs; got keys = {:?}",
        attr_keys(&attrs)
    );
    // provider_id is a 16-byte GUID rendered as a 36-char dashed hex
    // string (lowercase) by the encoder's `format_guid`. The little-endian
    // Data1/2/3 fields are byte-swapped into canonical GUID form once at the
    // session boundary (`CanonicalGuid`), so `format_guid` just renders it.
    // Check the shape as well as the exact subscribed provider GUID below.
    let provider_id = attr_str(&attrs, "etw.provider.id");
    assert_eq!(
        provider_id.len(),
        36,
        "etw.provider.id should be a 36-char GUID, got {provider_id:?}"
    );
    assert!(
        provider_id
            .chars()
            .all(|c| c == '-' || c.is_ascii_hexdigit()),
        "etw.provider.id contains non-hex/non-dash characters: {provider_id:?}"
    );
    assert_eq!(
        provider_id,
        session::CanonicalGuid::from(one_collect::guid_from_provider_name(provider_name))
            .to_string(),
        "captured provider GUID must match the subscribed provider"
    );
    match expected_provider_name {
        Some(name) => assert_attr_str(&attrs, "etw.provider.name", name),
        None => assert!(
            !attrs.iter().any(|(key, _)| key == "etw.provider.name"),
            "unregistered GUID-only providers must omit etw.provider.name"
        ),
    }
}

/// Per-attribute assertions for the rich `tracelogging_dynamic` event.
/// Each assertion pins the OTAP value-type discriminator as well as the
/// value, so a regression in the encoder/decoder mapping surfaces as an
/// explicit type mismatch rather than a silent change of column.
fn assert_dynamic_user_attrs(records: &OtapArrowRecords, row: usize) {
    let attrs = collect_user_attrs(records, row);

    assert_attr_int(&attrs, "MyAnswer", 42);
    // u64 -> i64 (saturating; in range, so exact).
    assert_attr_int(&attrs, "MyBigCount", 1_000_000_000_001);
    assert_attr_int(&attrs, "MyDelta", -7);
    assert_attr_double(&attrs, "MyPi", core::f64::consts::PI);
    // Two boolean encodings, both surfacing as `Int(1)` but via distinct
    // decoder paths (see `session.rs::interpret_field_value`):
    //   * `MyBool32Flag` -- Win32 `BOOL` / TraceLogging `Bool32`
    //     (`TDH_INTYPE_BOOLEAN`) -> mapped to a 4-byte `"u32"`.
    //   * `MyU8Flag` -- 1-byte boolean (`TDH_INTYPE_UINT8` + `OutType::Boolean`)
    //     -> mapped to `"u8"`.
    assert_attr_int(&attrs, "MyBool32Flag", 1);
    assert_attr_int(&attrs, "MyU8Flag", 1);
    assert_attr_str(&attrs, "MyMessage", "hello-from-test");
    assert_attr_str(&attrs, "MyWideMessage", "wide\u{2603}");
    // FILETIME 2024-01-01 00:00:00 UTC -> Unix-epoch ns.
    assert_attr_int(&attrs, "MyFiletime", 1_704_067_200_000_000_000);
    // GUID is opaque to the TDH decoder -> rendered by the encoder as a
    // lowercase hex string (no dashes), length 2*16 = 32.
    let guid_hex = attr_str(&attrs, "MyGuid");
    assert_eq!(
        guid_hex.len(),
        32,
        "MyGuid hex render should have 32 chars, got {guid_hex:?}"
    );
    assert!(
        guid_hex.chars().all(|c| c.is_ascii_hexdigit()),
        "MyGuid hex render contains non-hex characters: {guid_hex:?}"
    );
    // Nested-struct children are surfaced as flat `parent.child` keys.
    assert_attr_int(&attrs, "MyNested.NestedAnswer", 99);
    assert_attr_str(&attrs, "MyNested.NestedMessage", "nested-from-dynamic");
}

/// Per-attribute assertions for the smaller static `tracelogging` event.
fn assert_static_user_attrs(records: &OtapArrowRecords, row: usize) {
    let attrs = collect_user_attrs(records, row);

    assert_attr_int(&attrs, "StaticAnswer", 123);
    assert_attr_str(&attrs, "StaticMessage", "hello-from-static");
    // Two boolean encodings, mirroring the dynamic event:
    //   * `StaticFlag` (`bool8`) is a 1-byte boolean (`TDH_INTYPE_UINT8`
    //     + `OutType::Boolean`), which one_collect maps to `"u8"` -> `Int(1)`.
    //   * `StaticFlag32` (`bool32`) is a 4-byte Win32 `BOOL` / `Bool32`
    //     (`TDH_INTYPE_BOOLEAN`), which one_collect maps to `"u32"` -> `Int(1)`.
    assert_attr_int(&attrs, "StaticFlag", 1);
    assert_attr_int(&attrs, "StaticFlag32", 1);
    // Nested-struct children: same flattening as for the dynamic event.
    assert_attr_int(&attrs, "StaticNested.NestedAnswer", 456);
    assert_attr_str(&attrs, "StaticNested.NestedMessage", "nested-from-static");
}

/// Collect non-`etw.*` attributes for the log row at index `row`.
fn collect_user_attrs(records: &OtapArrowRecords, row: usize) -> Vec<(String, AttrSnapshot)> {
    let logs_rb = records
        .get(ArrowPayloadType::Logs)
        .expect("Logs payload should be present");
    let attrs_rb = records
        .get(ArrowPayloadType::LogAttrs)
        .expect("LogAttrs payload should be present");
    let log_id =
        u16_column(logs_rb, consts::ID)[row].expect("log id must be present at the matched row");
    collect_attributes(attrs_rb, log_id)
        .into_iter()
        .filter(|(k, _)| !k.starts_with("etw."))
        .collect()
}

// -- Attribute snapshot + collection -------------------------------------

/// Snapshot of a single attribute value, mapping the OTAP `type`
/// discriminator to a typed Rust value so the assertion helpers can
/// pattern-match exhaustively.
#[derive(Debug, Clone, PartialEq)]
enum AttrSnapshot {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

/// Collect every (key, value) pair from `attrs_rb` whose `parent_id`
/// equals `log_id`.  Reads the `type` discriminator column to decide
/// which value column to pull from.
fn collect_attributes(
    attrs_rb: &arrow::array::RecordBatch,
    log_id: u16,
) -> Vec<(String, AttrSnapshot)> {
    let parent_ids = u16_column(attrs_rb, consts::PARENT_ID);
    let keys = string_column(attrs_rb, consts::ATTRIBUTE_KEY);
    let types = attrs_rb
        .column_by_name(consts::ATTRIBUTE_TYPE)
        .expect("LogAttrs must have a `type` column")
        .as_any()
        .downcast_ref::<arrow::array::UInt8Array>()
        .expect("LogAttrs.type must be a UInt8Array");

    let mut out = Vec::new();
    for i in 0..attrs_rb.num_rows() {
        if parent_ids[i] != Some(log_id) {
            continue;
        }
        let key = keys[i].clone().unwrap_or_default();
        let snap = match types.value(i) {
            // AttributeValueType::Str
            1 => {
                let v = bytes_column(attrs_rb, consts::ATTRIBUTE_STR)[i].clone();
                AttrSnapshot::Str(
                    v.map(|b| String::from_utf8_lossy(&b).into_owned())
                        .unwrap_or_default(),
                )
            }
            // AttributeValueType::Int
            2 => AttrSnapshot::Int(
                i64_column(attrs_rb, consts::ATTRIBUTE_INT)[i]
                    .expect("int attribute row must have a value"),
            ),
            // AttributeValueType::Double
            3 => AttrSnapshot::Double(
                f64_column(attrs_rb, consts::ATTRIBUTE_DOUBLE)[i]
                    .expect("double attribute row must have a value"),
            ),
            // AttributeValueType::Bool
            4 => AttrSnapshot::Bool(
                bool_column(attrs_rb, consts::ATTRIBUTE_BOOL)[i]
                    .expect("bool attribute row must have a value"),
            ),
            // AttributeValueType::Bytes
            7 => AttrSnapshot::Bytes(
                bytes_column(attrs_rb, consts::ATTRIBUTE_BYTES)[i]
                    .clone()
                    .expect("bytes attribute row must have a value"),
            ),
            other => panic!(
                "unexpected attribute value-type discriminator {other} at row {i} \
                 (key = {key:?}); ETW receiver should only emit Str/Int/Double/Bool/Bytes"
            ),
        };
        out.push((key, snap));
    }
    out
}

fn attr_keys(attrs: &[(String, AttrSnapshot)]) -> Vec<&str> {
    attrs.iter().map(|(k, _)| k.as_str()).collect()
}

fn attr_str(attrs: &[(String, AttrSnapshot)], key: &str) -> String {
    match attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v) {
        Some(AttrSnapshot::Str(s)) => s.clone(),
        Some(other) => panic!("attribute '{key}' expected to be Str(_), got {other:?}"),
        None => panic!(
            "expected attribute '{key}' not found; got keys = {:?}",
            attr_keys(attrs)
        ),
    }
}

fn assert_attr_int(attrs: &[(String, AttrSnapshot)], key: &str, expected: i64) {
    match attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v) {
        Some(AttrSnapshot::Int(actual)) => assert_eq!(
            *actual, expected,
            "attribute '{key}' int value mismatch: expected {expected}, got {actual}"
        ),
        Some(other) => panic!("attribute '{key}' expected to be Int({expected}), got {other:?}"),
        None => panic!(
            "expected attribute '{key}' not found; got keys = {:?}",
            attr_keys(attrs)
        ),
    }
}

fn assert_attr_double(attrs: &[(String, AttrSnapshot)], key: &str, expected: f64) {
    match attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v) {
        Some(AttrSnapshot::Double(actual)) => assert!(
            (*actual - expected).abs() < f64::EPSILON,
            "attribute '{key}' double value mismatch: expected {expected}, got {actual}"
        ),
        Some(other) => panic!("attribute '{key}' expected to be Double({expected}), got {other:?}"),
        None => panic!(
            "expected attribute '{key}' not found; got keys = {:?}",
            attr_keys(attrs)
        ),
    }
}

fn assert_attr_str(attrs: &[(String, AttrSnapshot)], key: &str, expected: &str) {
    match attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v) {
        Some(AttrSnapshot::Str(actual)) => assert_eq!(
            actual, expected,
            "attribute '{key}' str value mismatch: expected {expected:?}, got {actual:?}"
        ),
        Some(other) => panic!("attribute '{key}' expected to be Str({expected:?}), got {other:?}"),
        None => panic!(
            "expected attribute '{key}' not found; got keys = {:?}",
            attr_keys(attrs)
        ),
    }
}

/// Assert that an `Int`-typed attribute exists and that *either* the
/// raw `bit` is set in the value, *or* the value is `i64::MAX` (the
/// saturating cast result produced when the original u64 had bit 63
/// set -- TraceLogging unconditionally ORs the metadata marker
/// `0x8000_0000_0000_0000` into the keyword mask).
fn assert_attr_keywords_includes_bit(attrs: &[(String, AttrSnapshot)], key: &str, bit: i64) {
    match attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v) {
        Some(AttrSnapshot::Int(actual)) => {
            let ok = *actual == i64::MAX || (*actual & bit) == bit;
            assert!(
                ok,
                "attribute '{key}' (= {actual:#x}) does not include keyword bit \
                 {bit:#x} and is not i64::MAX (saturated marker)"
            );
        }
        Some(other) => panic!("attribute '{key}' expected to be Int(_), got {other:?}"),
        None => panic!(
            "expected attribute '{key}' not found; got keys = {:?}",
            attr_keys(attrs)
        ),
    }
}

// -- Column-extraction helpers -------------------------------------------

/// Read a UTF-8 / binary column as `Vec<Option<String>>`, transparently
/// handling plain Utf8/Binary and `Dictionary(UInt8|UInt16, Utf8|Binary)`.
fn string_column(rb: &arrow::array::RecordBatch, name: &str) -> Vec<Option<String>> {
    let Some(col) = rb.column_by_name(name) else {
        return vec![None; rb.num_rows()];
    };
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return (0..arr.len())
            .map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_owned()))
            .collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
        return (0..arr.len())
            .map(|i| (!arr.is_null(i)).then(|| String::from_utf8_lossy(arr.value(i)).into_owned()))
            .collect();
    }
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt8Type>>()
    {
        return resolve_dict_strings(dict.keys(), dict.values(), dict.len());
    }
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>()
    {
        return resolve_dict_strings(dict.keys(), dict.values(), dict.len());
    }
    panic!(
        "column `{name}` has unsupported data type {:?} for string_column()",
        col.data_type()
    );
}

/// Read a binary column as `Vec<Option<Vec<u8>>>`, transparently
/// handling plain Binary and `Dictionary(UInt8|UInt16, Binary)`.
fn bytes_column(rb: &arrow::array::RecordBatch, name: &str) -> Vec<Option<Vec<u8>>> {
    let Some(col) = rb.column_by_name(name) else {
        return vec![None; rb.num_rows()];
    };
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
        return (0..arr.len())
            .map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_vec()))
            .collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return (0..arr.len())
            .map(|i| (!arr.is_null(i)).then(|| arr.value(i).as_bytes().to_vec()))
            .collect();
    }
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>()
    {
        let values_str = dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>();
        let values_utf8 = dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>();
        return (0..dict.len())
            .map(|i| {
                if dict.is_null(i) {
                    return None;
                }
                let k = dict.keys().value(i) as usize;
                if let Some(v) = values_str {
                    Some(v.value(k).to_vec())
                } else {
                    values_utf8.map(|v| v.value(k).as_bytes().to_vec())
                }
            })
            .collect();
    }
    panic!(
        "column `{name}` has unsupported data type {:?} for bytes_column()",
        col.data_type()
    );
}

fn resolve_dict_strings<K>(
    keys: &arrow::array::PrimitiveArray<K>,
    values: &dyn Array,
    len: usize,
) -> Vec<Option<String>>
where
    K: arrow::datatypes::ArrowPrimitiveType,
    K::Native: Into<usize>,
{
    let v_str = values.as_any().downcast_ref::<arrow::array::StringArray>();
    let v_bin = values.as_any().downcast_ref::<arrow::array::BinaryArray>();
    (0..len)
        .map(|i| {
            if keys.is_null(i) {
                return None;
            }
            let k: usize = keys.value(i).into();
            if let Some(v) = v_str {
                Some(v.value(k).to_owned())
            } else {
                v_bin.map(|v| String::from_utf8_lossy(v.value(k)).into_owned())
            }
        })
        .collect()
}

fn i32_column(rb: &arrow::array::RecordBatch, name: &str) -> Vec<Option<i32>> {
    let Some(col) = rb.column_by_name(name) else {
        return vec![None; rb.num_rows()];
    };
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return (0..arr.len())
            .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
            .collect();
    }
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt8Type>>()
    {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("severity_number dict values should be Int32");
        return (0..dict.len())
            .map(|i| {
                if dict.is_null(i) {
                    return None;
                }
                let k = dict.keys().value(i) as usize;
                Some(values.value(k))
            })
            .collect();
    }
    panic!(
        "column `{name}` has unsupported data type {:?} for i32_column()",
        col.data_type()
    );
}

fn i64_column(rb: &arrow::array::RecordBatch, name: &str) -> Vec<Option<i64>> {
    let Some(col) = rb.column_by_name(name) else {
        return vec![None; rb.num_rows()];
    };
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return (0..arr.len())
            .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
            .collect();
    }
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::UInt16Type>>()
    {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int_value dict values should be Int64");
        return (0..dict.len())
            .map(|i| {
                if dict.is_null(i) {
                    return None;
                }
                let k = dict.keys().value(i) as usize;
                Some(values.value(k))
            })
            .collect();
    }
    panic!(
        "column `{name}` has unsupported data type {:?} for i64_column()",
        col.data_type()
    );
}

fn f64_column(rb: &arrow::array::RecordBatch, name: &str) -> Vec<Option<f64>> {
    let col = rb
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column `{name}` missing"));
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap_or_else(|| {
            panic!(
                "column `{name}` has unsupported data type {:?}",
                col.data_type()
            )
        });
    (0..arr.len())
        .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
        .collect()
}

fn bool_column(rb: &arrow::array::RecordBatch, name: &str) -> Vec<Option<bool>> {
    let col = rb
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column `{name}` missing"));
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap_or_else(|| {
            panic!(
                "column `{name}` has unsupported data type {:?}",
                col.data_type()
            )
        });
    (0..arr.len())
        .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
        .collect()
}

fn u16_column(rb: &arrow::array::RecordBatch, name: &str) -> Vec<Option<u16>> {
    let Some(col) = rb.column_by_name(name) else {
        return vec![None; rb.num_rows()];
    };
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt16Array>() {
        return (0..arr.len())
            .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
            .collect();
    }
    panic!(
        "column `{name}` has unsupported data type {:?} for u16_column()",
        col.data_type()
    );
}

fn timestamp_ns_column(rb: &arrow::array::RecordBatch, name: &str) -> Vec<Option<i64>> {
    let col = rb
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column `{name}` missing"));
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .unwrap_or_else(|| {
            panic!(
                "column `{name}` has unsupported data type {:?} for timestamp_ns_column()",
                col.data_type()
            )
        });
    (0..arr.len())
        .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
        .collect()
}

/// Assert that the column `name` is either absent or null at `row`.
fn assert_all_null_at(rb: &arrow::array::RecordBatch, name: &str, row: usize) {
    let Some(col) = rb.column_by_name(name) else {
        return;
    };
    assert!(
        col.is_null(row),
        "column `{name}` at row {row}: expected null, was non-null"
    );
}

// -- Struct-column helpers -----------------------------------------------

fn struct_column(rb: &arrow::array::RecordBatch, name: &str) -> arrow::array::StructArray {
    let col = rb
        .column_by_name(name)
        .unwrap_or_else(|| panic!("struct column `{name}` missing"));
    col.as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap_or_else(|| {
            panic!(
                "column `{name}` is not a StructArray; data type = {:?}",
                col.data_type()
            )
        })
        .clone()
}

/// Assert that a top-level UInt32 column is either absent (omitted by
/// the all-zeros default-value optimization), null at `row`, or equal
/// to `Some(0)`.
fn assert_u32_zero_or_null(rb: &arrow::array::RecordBatch, name: &str, row: usize) {
    let Some(col) = rb.column_by_name(name) else {
        return;
    };
    if col.is_null(row) {
        return;
    }
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::UInt32Array>()
        .unwrap_or_else(|| {
            panic!(
                "column `{name}` is not UInt32; data type = {:?}",
                col.data_type()
            )
        });
    assert_eq!(
        arr.value(row),
        0,
        "column `{name}` at row {row}: expected 0 or null, got {}",
        arr.value(row)
    );
}

/// Struct-field analogue of [`assert_u32_zero_or_null`].
fn assert_u32_struct_field_zero_or_null(s: &arrow::array::StructArray, field: &str, row: usize) {
    let Some(col) = s.column_by_name(field) else {
        return;
    };
    if col.is_null(row) {
        return;
    }
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::UInt32Array>()
        .unwrap_or_else(|| {
            panic!(
                "struct field `{field}` is not UInt32; data type = {:?}",
                col.data_type()
            )
        });
    assert_eq!(
        arr.value(row),
        0,
        "struct field `{field}` at row {row}: expected 0 or null, got {}",
        arr.value(row)
    );
}

/// UInt16 struct-field analogue of [`assert_u32_zero_or_null`].
fn assert_u16_struct_field_zero_or_null(s: &arrow::array::StructArray, field: &str, row: usize) {
    let Some(col) = s.column_by_name(field) else {
        return;
    };
    if col.is_null(row) {
        return;
    }
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::UInt16Array>()
        .unwrap_or_else(|| {
            panic!(
                "struct field `{field}` is not UInt16; data type = {:?}",
                col.data_type()
            )
        });
    assert_eq!(
        arr.value(row),
        0,
        "struct field `{field}` at row {row}: expected 0 or null, got {}",
        arr.value(row)
    );
}

fn struct_field_is_null(s: &arrow::array::StructArray, field: &str, row: usize) -> bool {
    match s.column_by_name(field) {
        Some(c) => c.is_null(row),
        None => true,
    }
}

// -- Diagnostic / privilege helpers --------------------------------------

/// True when the current process is running with Administrator
/// privileges (high integrity level / member of the local
/// Administrators group).  Implemented via `shell32!IsUserAnAdmin`,
/// which is available on every supported Windows version and does not
/// itself require elevation.
#[allow(unsafe_code)]
fn is_process_elevated() -> bool {
    #[link(name = "shell32")]
    unsafe extern "system" {
        fn IsUserAnAdmin() -> i32;
    }
    // SAFETY: takes no arguments, returns BOOL, safe from any thread.
    (unsafe { IsUserAnAdmin() }) != 0
}

/// Print the list of running ETW sessions to stderr so test failures
/// can show whether a stale or duplicate `OtapEtwE2E-*` session is
/// blocking the receiver.  Best-effort: if `logman` is missing or
/// fails, log a short notice and return.
#[allow(clippy::print_stderr)]
fn print_running_etw_sessions() {
    eprintln!("=== Active ETW sessions (logman query -ets) ===");
    match std::process::Command::new("logman")
        .args(["query", "-ets"])
        .output()
    {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stdout.trim().is_empty() {
                eprintln!("{stdout}");
            }
            if !stderr.trim().is_empty() {
                eprintln!("(logman stderr) {stderr}");
            }
        }
        Err(e) => eprintln!("(failed to invoke logman: {e})"),
    }
    eprintln!("=== end of sessions ===");
}

/// Stop every `OtapEtwE2E-*` ETW session left behind by a previous
/// run that died without unregistering.  ETW real-time sessions
/// persist across process exits -- Windows only cleans them up on
/// reboot or via an explicit `ControlTrace(STOP)` call.
///
/// We have to be broad rather than per-PID: `EtwSession::new()` in
/// `one_collect` uses a fixed internal session GUID, so any leftover
/// `OtapEtwE2E-*` session -- regardless of the PID suffix in its name
/// -- occupies that GUID and makes `StartTraceW` fail with
/// `ERROR_ALREADY_EXISTS` (183).  The same constraint means two copies
/// of this test physically cannot run concurrently on the same
/// machine, so this sweep never disrupts a peer that would otherwise
/// have succeeded.
#[allow(clippy::print_stderr)]
fn cleanup_stale_otap_etw_sessions() {
    let output = match std::process::Command::new("logman")
        .args(["query", "-ets"])
        .output()
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("failed to invoke `logman query -ets`: {e}");
            return;
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        // `logman query -ets` prints `<name>  <type>  <status>` per
        // session; the name is the first whitespace-delimited token.
        let Some(name) = line.split_whitespace().next() else {
            continue;
        };
        if !name.starts_with("OtapEtwE2E-") {
            continue;
        }
        match std::process::Command::new("logman")
            .args(["stop", name, "-ets"])
            .output()
        {
            Ok(out) if out.status.success() => {
                eprintln!("cleaned up stale ETW session {name}");
            }
            Ok(_) => {
                // Session disappeared between query and stop -- fine.
            }
            Err(e) => {
                eprintln!("failed to invoke `logman stop {name}`: {e}");
            }
        }
    }
}

// -- EventSource in-band manifest ----------------------------------------
//
// Manifest-mode .NET `EventSource` providers emit their manifest in-band as
// chunked `Id 0xFFFE / Opcode 0xFE / Task 0xFFFE` events. This test emulates
// that protocol with raw `EventRegister`/`EventWrite` (exactly the bytes
// `EventSource.SendManifest` writes), so it needs no .NET runtime.

const ES_SCALARS_EVENT: &str = "EsScalars";
const ES_BLOB_EVENT: &str = "EsBlob";
const ES_PING_EVENT: &str = "EsPing";
const ES_MIXED_TL_EVENT: &str = "EsMixedTl";
/// `EVENT_DESCRIPTOR.Id` of EventSource manifest chunk events.
const ES_MANIFEST_EVENT_ID: i64 = 0xFFFE;

/// Scenario: A GUID-only subscription to a manifest-mode EventSource
/// provider that is not registered with the OS receives the in-band
/// manifest, its payload events, and a self-describing (TraceLogging) event
/// under the same provider GUID.
/// Guarantees: Payload events are decoded from the in-band manifest (event
/// name from `symbol`, typed attributes including `byte[]`), the provider
/// name is learned from the manifest, the TraceLogging event still decodes,
/// and no manifest chunk is forwarded as a log record.
#[test]
#[ignore = "requires Administrator privileges and creates a real ETW kernel session; run explicitly with `-- --ignored`"]
fn etw_receiver_decodes_eventsource_inband_manifest_end_to_end() {
    assert!(
        is_process_elevated(),
        "ETW receiver e2e test requires Administrator privileges."
    );

    let pid = std::process::id();
    let provider_name = format!("OtapEtwE2E.EventSource.{pid}");
    // Distinct from the TraceLogging test's session name: sessions live in a
    // process-global registry until the process exits.
    let session_name = format!("OtapEtwE2E-ES-{pid}");
    cleanup_stale_otap_etw_sessions();

    // GUID-only and unregistered: the provider name can only come from the
    // in-band manifest.
    let provider_guid = one_collect::guid_from_provider_name(&provider_name);
    let config_json = serde_json::json!({
        "session_name": session_name,
        "providers": [
            { "guid": session::CanonicalGuid::from(provider_guid).to_string(), "level": "verbose" },
        ],
    });

    let (pipeline_ctx, _registry) = test_pipeline_ctx();
    let receiver = EtwReceiver::from_config(pipeline_ctx, &config_json)
        .expect("EtwReceiver::from_config should succeed (Administrator required)");

    let test_runtime = TestRuntime::<OtapPdata>::new();
    let node_config = Arc::new(NodeUserConfig::new_receiver_config(ETW_RECEIVER_URN));
    let receiver_wrapper = ReceiverWrapper::local(
        receiver,
        test_node(test_runtime.config().name.clone()),
        node_config,
        test_runtime.config(),
    );

    let producer_name = provider_name.clone();
    test_runtime
        .set_receiver(receiver_wrapper)
        .run_test(move |ctx| {
            Box::pin(run_then_shutdown(ctx, async move {
                run_eventsource_producer(&producer_name).await;
            })) as Pin<Box<dyn Future<Output = ()>>>
        })
        .run_validation(eventsource_validation(provider_name));
}

/// Runs `body`, then always sends `DrainIngress` + `Shutdown` (re-raising a
/// panic from `body` afterwards) so a failed assertion cannot leave the
/// receiver running and hang validation.
async fn run_then_shutdown(
    ctx: otel_arrow_dfe_engine::testing::receiver::TestContext<OtapPdata>,
    body: impl Future<Output = ()>,
) {
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    let outcome = AssertUnwindSafe(body).catch_unwind().await;

    let _ = ctx
        .send_control_msg(NodeControlMsg::DrainIngress {
            deadline: Instant::now() + Duration::from_secs(5),
            reason: "Test drain".to_owned(),
        })
        .await;
    let _ = ctx.send_shutdown(Instant::now(), "Test").await;

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// A manifest in the exact shape `EventSource`'s `ManifestBuilder` emits:
/// `symbol` names, a parameterless event without `template`, and `byte[]`
/// as a `<name>Size` u32 followed by `win:Binary length="<name>Size"`.
fn eventsource_manifest(provider_name: &str, provider_guid: &str) -> String {
    format!(
        r#"<instrumentationManifest xmlns="http://schemas.microsoft.com/win/2004/08/events">
 <instrumentation xmlns:win="http://manifests.microsoft.com/win/2004/08/windows/events">
  <events xmlns="http://schemas.microsoft.com/win/2004/08/events">
<provider name="{provider_name}" guid="{{{provider_guid}}}" symbol="OtapEtwE2EEventSource">
 <tasks>
  <task name="{ES_SCALARS_EVENT}" value="65533"/>
  <task name="{ES_BLOB_EVENT}" value="65532"/>
  <task name="{ES_PING_EVENT}" value="65531"/>
 </tasks>
 <events>
  <event value="1" version="0" level="win:Informational" symbol="{ES_SCALARS_EVENT}" task="{ES_SCALARS_EVENT}" template="{ES_SCALARS_EVENT}Args"/>
  <event value="2" version="0" level="win:Informational" symbol="{ES_BLOB_EVENT}" task="{ES_BLOB_EVENT}" template="{ES_BLOB_EVENT}Args"/>
  <event value="3" version="0" level="win:Informational" symbol="{ES_PING_EVENT}" task="{ES_PING_EVENT}"/>
 </events>
 <templates>
  <template tid="{ES_SCALARS_EVENT}Args">
   <data name="Count" inType="win:Int32"/>
   <data name="Big" inType="win:UInt64"/>
   <data name="Ratio" inType="win:Double"/>
   <data name="Flag" inType="win:Boolean"/>
   <data name="Message" inType="win:UnicodeString"/>
   <data name="After" inType="win:Int32"/>
  </template>
  <template tid="{ES_BLOB_EVENT}Args">
   <data name="before" inType="win:Int32"/>
   <data name="dataSize" inType="win:UInt32"/>
   <data name="data" inType="win:Binary" length="dataSize"/>
   <data name="after" inType="win:Int32"/>
  </template>
 </templates>
</provider>
</events>
</instrumentation>
</instrumentationManifest>"#
    )
}

/// Raw ETW registration that writes EventSource-protocol events.
struct EventSourceEmitter {
    handle: i64,
}

impl EventSourceEmitter {
    #[allow(unsafe_code)]
    fn register(guid: one_collect::Guid) -> Self {
        use windows_sys::Win32::System::Diagnostics::Etw::EventRegister;

        let guid = windows_sys::core::GUID {
            data1: guid.data1,
            data2: guid.data2,
            data3: guid.data3,
            data4: guid.data4,
        };
        let mut handle: i64 = 0;
        // SAFETY: `guid` and `handle` outlive the call; no enable callback.
        let status = unsafe { EventRegister(&guid, None, std::ptr::null(), &mut handle) };
        assert_eq!(status, 0, "EventRegister returned {status}");
        Self { handle }
    }

    #[allow(unsafe_code)]
    fn write(&self, id: u16, level: u8, opcode: u8, task: u16, keyword: u64, payload: &[u8]) {
        use windows_sys::Win32::System::Diagnostics::Etw::{
            EVENT_DATA_DESCRIPTOR, EVENT_DESCRIPTOR, EventWrite,
        };

        let desc = EVENT_DESCRIPTOR {
            Id: id,
            Version: if id == 0xFFFE { 1 } else { 0 },
            Channel: 0,
            Level: level,
            Opcode: opcode,
            Task: task,
            Keyword: keyword,
        };
        let data = EVENT_DATA_DESCRIPTOR {
            Ptr: payload.as_ptr() as u64,
            Size: payload.len() as u32,
            // SAFETY: the reserved union must be zero-initialised.
            Anonymous: unsafe { std::mem::zeroed() },
        };
        // SAFETY: live registration; `desc`, `data` and `payload` outlive
        // the call.
        let status = unsafe { EventWrite(self.handle, &desc, 1, &data) };
        assert_eq!(status, 0, "EventWrite(id={id}) returned {status}");
    }

    /// Writes `manifest` exactly as `EventSource.SendManifest` does: one
    /// event per chunk, each prefixed by the 8-byte `ManifestEnvelope`.
    fn write_manifest(&self, manifest: &[u8], chunk_size: usize) {
        let total = manifest.len().div_ceil(chunk_size) as u16;
        for (n, chunk) in manifest.chunks(chunk_size).enumerate() {
            let mut payload = vec![1u8, 1, 0, 0x5B];
            payload.extend_from_slice(&total.to_le_bytes());
            payload.extend_from_slice(&(n as u16).to_le_bytes());
            payload.extend_from_slice(chunk);
            self.write(0xFFFE, 0, 0xFE, 0xFFFE, 0x00FF_FFFF_FFFF_FFFF, &payload);
        }
    }

    /// Writes a payload event the way EventSource does (Informational).
    fn write_event(&self, id: u16, task: u16, payload: &[u8]) {
        self.write(id, 4, 0, task, 0xF000_0000_0000, payload);
    }
}

impl Drop for EventSourceEmitter {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        // SAFETY: `handle` came from a successful `EventRegister`.
        let _ =
            unsafe { windows_sys::Win32::System::Diagnostics::Etw::EventUnregister(self.handle) };
    }
}

fn utf16z(s: &str) -> Vec<u8> {
    let mut out: Vec<u8> = s.encode_utf16().flat_map(u16::to_le_bytes).collect();
    out.extend_from_slice(&[0, 0]);
    out
}

async fn run_eventsource_producer(provider_name: &str) {
    let provider_guid = one_collect::guid_from_provider_name(provider_name);

    // A TraceLogging registration under the same name has the same GUID
    // (the EventSource name hash); it doubles as the "is enabled" probe and
    // writes the mixed-mode self-describing event.
    let tl_provider = Box::pin(tld::Provider::new(provider_name, &tld::Provider::options()));
    // SAFETY: pinned for the rest of this function; unregisters on drop.
    #[allow(unsafe_code)]
    let r = unsafe { tl_provider.as_ref().register() };
    assert_eq!(r, 0, "tld Provider::register returned errno {r}");
    let emitter = EventSourceEmitter::register(provider_guid);

    wait_until_enabled(
        || tl_provider.enabled(tld::Level::Verbose, 0),
        provider_name,
    )
    .await;

    let manifest = eventsource_manifest(
        provider_name,
        &session::CanonicalGuid::from(provider_guid).to_string(),
    );

    let mut scalars = Vec::new();
    scalars.extend_from_slice(&(-7i32).to_le_bytes());
    scalars.extend_from_slice(&1_000_000_000_001u64.to_le_bytes());
    scalars.extend_from_slice(&2.5f64.to_le_bytes());
    scalars.extend_from_slice(&1i32.to_le_bytes()); // win:Boolean (4-byte BOOL)
    scalars.extend(utf16z("hello-eventsource"));
    scalars.extend_from_slice(&77i32.to_le_bytes());

    let mut blob = 7i32.to_le_bytes().to_vec();
    blob.extend_from_slice(&5u32.to_le_bytes());
    blob.extend_from_slice(&[1, 2, 3, 4, 5]);
    blob.extend_from_slice(&9i32.to_le_bytes());

    // Like EventSource on enable: manifest first (several 512-byte chunks),
    // then payload events. Repeated a few times (identical re-sends, as
    // EventSource does on every enable) to tolerate startup races.
    for _ in 0..3 {
        emitter.write_manifest(manifest.as_bytes(), 512);
        emitter.write_event(1, 65533, &scalars);
        emitter.write_event(2, 65532, &blob);
        emitter.write_event(3, 65531, &[]);
        let r = tld::EventBuilder::new()
            .reset(ES_MIXED_TL_EVENT, tld::Level::Informational, 0x1, 0)
            .add_i32("Value", 4242, tld::OutType::Default, 0)
            .write(&tl_provider, None, None);
        assert_eq!(r, 0, "tld EventBuilder::write returned errno {r}");
        time::sleep(Duration::from_millis(200)).await;
    }

    // Allow the kernel flush timer and the receiver's batch timer to run.
    time::sleep(Duration::from_secs(5)).await;
}

fn eventsource_validation(
    provider_name: String,
) -> impl FnOnce(
    otel_arrow_dfe_engine::testing::receiver::NotSendValidateContext<OtapPdata>,
) -> Pin<Box<dyn Future<Output = ()>>> {
    move |mut ctx| {
        Box::pin(async move {
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut batches: Vec<OtapArrowRecords> = Vec::new();
            let our_pid = std::process::id();
            let expected = [
                ES_SCALARS_EVENT,
                ES_BLOB_EVENT,
                ES_PING_EVENT,
                ES_MIXED_TL_EVENT,
            ];

            while Instant::now() < deadline
                && !expected.iter().all(|e| has_event(&batches, e, our_pid))
            {
                if let Ok(Ok(pdata)) = time::timeout(Duration::from_millis(500), ctx.recv()).await {
                    let records = pdata
                        .payload_ref()
                        .otap_ref()
                        .expect("expected native OTAP payload from ETW receiver");
                    batches.push(records.clone());
                }
            }

            let provider_id =
                session::CanonicalGuid::from(one_collect::guid_from_provider_name(&provider_name))
                    .to_string();

            // In-band manifest events: event id, provider identity (name
            // learned from the manifest), and typed attributes.
            let check_inband = |event: &str, event_id: i64| {
                let (records, row) = locate_event(&batches, event, our_pid);
                let attrs = collect_attributes(
                    records.get(ArrowPayloadType::LogAttrs).expect("LogAttrs"),
                    u16_column(
                        records.get(ArrowPayloadType::Logs).expect("Logs"),
                        consts::ID,
                    )[row]
                        .expect("log id"),
                );
                assert_attr_int(&attrs, "etw.event.id", event_id);
                assert_attr_str(&attrs, "etw.provider.id", &provider_id);
                assert_attr_str(&attrs, "etw.provider.name", &provider_name);
                collect_user_attrs(records, row)
            };

            let attrs = check_inband(ES_SCALARS_EVENT, 1);
            assert_attr_int(&attrs, "Count", -7);
            assert_attr_int(&attrs, "Big", 1_000_000_000_001);
            assert_attr_double(&attrs, "Ratio", 2.5);
            assert_attr_int(&attrs, "Flag", 1);
            assert_attr_str(&attrs, "Message", "hello-eventsource");
            assert_attr_int(&attrs, "After", 77);

            let attrs = check_inband(ES_BLOB_EVENT, 2);
            assert_attr_int(&attrs, "before", 7);
            // `byte[]` decodes to Bytes, which the encoder renders as hex.
            assert_attr_str(&attrs, "data", "0102030405");
            assert_attr_int(&attrs, "after", 9);
            assert!(
                !attrs.iter().any(|(k, _)| k == "dataSize"),
                "the byte[] length field is folded into `data`"
            );

            let attrs = check_inband(ES_PING_EVENT, 3);
            assert!(
                attrs.is_empty(),
                "parameterless event has no attributes: {attrs:?}"
            );

            // Mixed mode: the self-describing event under the same GUID.
            let (records, row) = locate_event(&batches, ES_MIXED_TL_EVENT, our_pid);
            assert_attr_int(&collect_user_attrs(records, row), "Value", 4242);

            // Manifest chunks are metadata and must never be forwarded.
            let forwarded_chunks = batches
                .iter()
                .flat_map(|records| {
                    let logs = records.get(ArrowPayloadType::Logs);
                    let attrs = records.get(ArrowPayloadType::LogAttrs);
                    logs.zip(attrs)
                        .map(|(logs, attrs)| {
                            u16_column(logs, consts::ID)
                                .into_iter()
                                .flatten()
                                .map(|id| collect_attributes(attrs, id))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .filter(|attrs| {
                    attrs.iter().any(|(k, v)| {
                        k == "etw.event.id" && *v == AttrSnapshot::Int(ES_MANIFEST_EVENT_ID)
                    })
                })
                .count();
            assert_eq!(
                forwarded_chunks, 0,
                "manifest chunk events must not be forwarded"
            );
        })
    }
}
