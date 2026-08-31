---
title: Error Resolution — DP-700 Exam-Ready Notes
topic: 10
domain: Domain 3 — Monitor and optimize an analytics solution (30–35%)
source: certification/10-error-resolution/
tags: [dp-700, exam-ready, error-resolution, troubleshooting, pipelines, dataflow-gen2, spark, tsql, warehouse, eventhouse, eventstream, onelake-shortcuts]
---

# 10. Error Resolution

> **Exam domain:** Domain 3 — Monitor and optimize an analytics solution (30–35%)
> **Source:** `certification/10-error-resolution/` — 5 files condensed (index + 4 subtopics)
> **Why the exam cares:** The blueprint bullet "Identify and resolve errors" names seven surfaces by name — pipeline, Dataflow Gen2, notebook, Eventhouse, Eventstream, T-SQL (Warehouse), OneLake shortcut. Questions give you a symptom or an exact error string and expect you to pick the *documented* fix, not a plausible-sounding one. Half the marks hinge on one distinction: is this failure transient (retry) or permanent (fix the config/data)?

---

## Orientation — the 60-second version

Fabric is a single SaaS analytics platform, but under the hood it is many engines stitched together: Data Factory pipelines, the Power Query mashup engine (Dataflow Gen2), Spark (notebooks), a T-SQL Warehouse, a Kusto/KQL Eventhouse, an Eventstream router, and OneLake — the one storage layer everything reads and writes. Each engine keeps its own error vocabulary, so the same underlying problem ("my credential died") appears as `LSROBOTokenFailure` in a pipeline, `403 Forbidden` in Spark, `DataAccessNotAuthorized` in Eventhouse, and a plain HTTP 403 through a shortcut.

Topic 09 taught you *where to look*. This topic is *what you are looking at once you get there*. The pattern is identical everywhere: find the exact error code, classify it (permanent vs transient, user vs system, auth vs data), apply the documented fix.

The highest-value classification is retry-worthiness. Retry fixes exactly one class: transient/infrastructure failures (a bad node, throttling, a transient download). It never fixes permanent failures (bad format, missing permission, schema mismatch) — retrying those just burns capacity. Fabric encodes this for you: `failureType` (`UserError`/`SystemError`) in pipeline output JSON, `FailureKind` (`Permanent`/`Transient`) in Eventhouse ingestion failures, and exit codes in Spark.

A second theme: **auth failures cluster around token lifetime, not wrong credentials** — OAuth expiry, Conditional Access changes, and a 30–60 minute delegated-shortcut token cache all present as a generic "unauthorized".

Finally: "it failed" and "it's slow" are different questions. A hard failure has a code you look up here. Degraded-but-succeeding behaviour (query folding silently disabled, ingestion batching latency) is a performance topic, not an error.

## New terms in this topic

| Term | What it actually is |
| :--- | :--- |
| **OneLake** | The single tenant-wide data lake every Fabric item reads and writes. One copy of the data, many engines on top. |
| **Pipeline (Data Factory)** | The orchestration item — a graph of activities (Copy, Notebook, Execute Pipeline, Web, Fail, ForEach). Each activity emits an Output JSON when it fails. |
| **Dataflow Gen2** | Low-code ETL built on the Power Query / M mashup engine. Refreshes on a schedule, optionally writing to a Lakehouse/Warehouse destination. |
| **Staging Lakehouse** | A hidden Lakehouse Dataflow Gen2 uses to materialise intermediate query results so later queries can reference them. Reading it back uses TDS (port 1433). |
| **TDS (Tabular Data Stream)** | The wire protocol SQL Server / Fabric SQL engines speak, carried on **TCP 1433**. Many corporate proxies pass generic HTTP/TCP but don't support TDS at all — which is why a gateway can write over HTTPS 443 yet fail to read staged data back. |
| **Query folding** | Power Query pushing transformation steps down to the source (SQL pushdown) instead of pulling all rows into the mashup engine. Losing it is a performance problem, not an error. |
| **On-premises data gateway** | An agent installed on a customer server that lets Fabric reach data behind a corporate firewall. |
| **Dataflows connector** | The connector a downstream item uses to read a Dataflow Gen2's results through an internal API, rather than reading its Lakehouse destination directly. |
| **Fail activity** | A pipeline activity that deliberately fails with an `errorCode` and `message` you author — used to turn a data-validation check into a meaningful run-history failure. |
| **Capacity / SKU** | The purchased compute pool (F-SKU) a workspace runs on. Everything — Spark, Warehouse, Eventhouse — draws Capacity Units (CU) from it. |
| **Spark VCore** | Fabric's Spark compute unit. 1 capacity unit = 2 Spark VCores; bursting allows up to 3× purchased VCores when capacity is idle. |
| **Autoscale Billing** | A setting that moves Spark off shared capacity onto separately-billed autoscaling compute. |
| **`%%configure`** | A notebook magic cell that sets session-level Spark config and restarts the session. Must be the first cell. |
| **Spark UI** | Per-application diagnostic web UI (Executors / Stages / Storage / Environment tabs), reached from Monitor hub → application → Jobs tab → job description. |
| **AQE (Adaptive Query Execution)** | Spark's runtime re-planner — it re-shapes joins and partitions using real statistics as the query runs. **On by default in Fabric**; its skew-join handling (`spark.sql.adaptive.skewJoin.enabled`) is the first documented fix for executor OOM caused by skew. |
| **`AnalysisException`** | The Spark error raised during the **query analysis phase**, before any data is read — a missing table, unresolved column, ambiguous reference, or type mismatch. It fails fast, so no compute is wasted. |
| **Warehouse / SQL analytics endpoint** | The T-SQL engine over OneLake Delta tables. The Warehouse is read-write; the SQL analytics endpoint is the auto-generated read-only T-SQL view of a Lakehouse. |
| **Snapshot isolation** | The only transaction isolation level Fabric Warehouse offers. Readers never block writers; concurrent writers to one table conflict at commit. |
| **`COPY INTO`** | The Warehouse bulk-load T-SQL statement, with `ERRORFILE`/`MAXERRORS` options for rejected-row handling. |
| **Query insights** | Warehouse DMV-style views (`queryinsights.exec_requests_history` and friends) holding historical query text, duration and resource use. |
| **Eventhouse** | The Fabric item hosting KQL databases for high-volume time-series/event data. Ingests via queued or streaming paths. |
| **Update policy** | An Eventhouse rule that automatically runs a KQL query over newly ingested rows in a source table and writes the result to a derived table — Kusto's built-in transform, no pipeline needed. |
| **Eventstream** | The no-code router that pulls events from sources (Event Hubs, etc.), optionally transforms them with processors, and pushes them to destinations (Lakehouse, Eventhouse, Activator). |
| **Data insights / Runtime logs** | Eventstream's two in-editor monitoring panels — throughput/status metrics, and engine-level logs respectively. |
| **Activator** | The Fabric item that watches a stream and fires rules/alerts when a condition is met. |
| **Workspace monitoring** | An opt-in feature that provisions a monitoring Eventhouse holding KQL log tables (Ingestion results logs, Command logs, Data operation logs) for durable, cross-item diagnostics. |
| **Shortcut** | A pointer in OneLake to data living elsewhere (another OneLake path, S3, ADLS Gen2, GCS, Blob, Dataverse, OneDrive/SharePoint) — no copy is made. |
| **Passthrough vs delegated authentication** | Passthrough sends the calling user's identity to the target; delegated uses a configured connection identity instead. |
| **OneLake security (RLS/CLS/OLS)** | Row-, column- and object-level security defined at the OneLake layer so every engine sees the same rules — as opposed to SQL-layer RLS/CLS/DDM, which only applies inside the Warehouse's own TDS execution context. |
| **Direct Lake** | A Power BI semantic model mode that reads Delta files in OneLake directly. "Direct Lake over SQL" goes via the SQL endpoint (owner identity); "Direct Lake over OneLake" passes the calling user through. |
| **Capacity Metrics app** | The admin Power BI app showing CU consumption, throttling and per-item/per-timepoint compute. |

## How the pieces fit

- **Four error surfaces, one triage pattern:** find the code → classify (permanent/transient, user/system, auth/data) → apply the documented fix.
- **§1 Pipeline & Dataflow Gen2** — orchestration and low-code ETL. Diagnostics live in the activity **Output JSON** (`errorCode`, `message`, `failureType`, `target`) and in the dataflow's **refresh history + downloadable mashup logs**.
- **§2 Notebook & T-SQL** — the two code surfaces. Spark diagnoses by **exit code** in the Spark UI Executors tab; Warehouse diagnoses by **T-SQL error number** and by `COPY INTO` rejected-row files.
- **§3 Eventhouse & Eventstream** — real-time. Eventhouse diagnoses by `.show ingestion failures` (`FailureKind`, `ErrorCode`); Eventstream splits into **Data insights** (is data flowing?) and **Runtime logs** (why did the engine do that?).
- **§4 OneLake shortcuts** — the identity layer. Almost every shortcut "connectivity" error is really auth mode, permission or cached-token timing.
- **Cross-cutting: capacity.** `CapacityLimitExceeded`, HTTP 430 / `TooManyRequestsForCapacity`, error 2003 and Dataflow refresh throttling are all one problem — the capacity, not the code. Diagnose in the Capacity Metrics app.
- **Cross-cutting: token lifetime.** `LSROBOTokenFailure`, Databricks 90-day tokens, and the 30–60 minute delegated shortcut token cache all look like "unauthorized" but need different fixes.
- **Cross-cutting: silent failure.** Query folding loss, non-transactional update-policy failures, delegated-mode SQL ignoring OneLake RLS, and Eventstream processor type mismatches all *succeed* while producing wrong or missing data.
- **Boundary with topic 09:** 09 tells you which surface to open. **Boundary with topic 11:** anything that succeeds-but-slowly is a performance question, not an error.

---

## 1. Pipeline and Dataflow Gen2 Errors
*Source: `01-pipeline-dataflow-errors.md`*

### Reading the pipeline activity failure output

Every failed pipeline activity writes an **Output** JSON payload, visible from the activity's monitoring row (select **Output** next to the run).

```json
{
  "errorCode": "InvalidTemplate",
  "message": "The template function 'dataset' is not defined or not valid.",
  "failureType": "UserError",
  "target": "Copy_DeltaData"
}
```

| Field | Meaning |
| :--- | :--- |
| `errorCode` | Short machine-readable identifier for the failure class (e.g. `InvalidTemplate`, or a numeric connector code like `2003`) |
| `message` | Human-readable description — often includes the exact bad value or a suggestion |
| `failureType` | `UserError` (fix the pipeline/config) or `SystemError` (transient, usually safe to retry) |
| `target` | The activity name that failed — useful when the failure originates several activities deep in a `ForEach` or nested pipeline |

The **Fail activity** lets you author your own `errorCode` and `message`; these appear in run history and logs exactly like a system-generated failure, which is how a conditional data-validation failure gets a meaningful message instead of a generic downstream error.

> 🧠 **Mental model —** `failureType` is the **triage tag** (who owns the fix: you, or "try again"). `errorCode` is the **specific complaint** (what to fix). Check `failureType` first — a `SystemError` from a transient network blip needs a rerun, not a pipeline edit.

### Connection, authentication and gateway errors

| Error / message | Cause | Resolution |
| :--- | :--- | :--- |
| **`LSROBOTokenFailure`** — "Access has been blocked by Conditional Access policies" / "provided grant has expired" | The pipeline's stored refresh token can no longer get a new access token — user's device left the tenant, password changed, or Conditional Access requirements changed | Update and save the affected pipeline (refreshes its auth context) via the Fabric portal, or use the published PowerShell scripts for bulk updates |
| Databricks **Error code 3200** — "Error 403… The Databricks access token has expired" | Azure Databricks access tokens default to **90-day** validity | Generate a new Databricks token and update the connection |
| Databricks **Error code 3208** — "An error occurred while sending the request" | Network connection to the Databricks service was interrupted | Verify network reliability from the runtime; usually resolves on retry |
| Web activity **Error Code 2403** — "Get access token from MSI failed" | The resource URL for a managed-identity token request is wrong or unreachable | Verify the resource URL matches what the managed identity is scoped to |
| Dataflow Gen2 gateway refresh — "A network-related or instance-specific error occurred… TCP Provider, error: 0" | On-prem gateway can't reach the **dataflow staging Lakehouse** over **port 1433** (TDS protocol) — firewall/proxy blocking outbound TCP on that port | Open outbound TCP 1433 to `*.datawarehouse.pbidedicated.windows.net`, `*.datawarehouse.fabric.microsoft.com`, `*.dfs.fabric.microsoft.com` on the gateway server; or combine referencing queries / disable staging as a workaround |

> ⚠️ **Trap —** Do not treat every gateway "network-related error" as generic connectivity. The dataflow engine **writes** to a Lakehouse over HTTPS (port 443) but **reads staged data back over TDS (port 1433)**. So the *first* query in a chain can succeed while a query that *references* it fails, even though both point at the same OneLake instance. Many corporate proxies pass generic HTTP/TCP but don't support the TDS protocol at all — which is why "combine the queries" or "disable staging" are documented workarounds, not just firewall fixes.

### Staging, timeout and throttling errors

| Error / message | Cause | Resolution |
| :--- | :--- | :--- |
| Dataflow Gen2 — "The dataflow refresh failed due to insufficient permissions for accessing staging artifacts" | The user who created the **first** dataflow in the workspace hasn't signed in for 90+ days, or has left the organization | That user signs back into Fabric; if they've left, open a support ticket |
| Dataflow Gen2 — "The key didn't match any rows in the table" (via the Dataflows connector) | Intermittent timeout in the internal API a downstream item uses to read a Dataflow Gen2's results through the **Dataflows connector** — a misleading message, not missing data | Configure a data destination (Lakehouse/Warehouse) on the source dataflow and have downstream items read directly from OneLake instead of via the Dataflows connector |
| Web activity **Error Code 2003** — "substantial concurrent external activity executions… causing failures due to throttling" | Too many pipelines triggered concurrently against the same subscription/region limit | Reduce concurrency; stagger trigger times across pipelines |
| Capacity — **`CapacityLimitExceeded`** / "Your organization's Fabric compute capacity has exceeded its limits" | Capacity moved past throttling into the **rejection phase** — not enough resources to admit the operation | Use the Capacity Metrics app's Compute/Timepoint pages to find the consuming item; scale up, enable autoscale, or reschedule the offending job |
| Web activity **Error Code 2001/2002** — payload/output size limits | Activity output exceeds **~4 MB**, or the combined activity/data/connection payload exceeds **896 KB** | Reduce parameter values passed between activities; avoid passing large data through control-flow parameters |
| **"Activity stuck"** — barely any progress for far longer than normal | The activity (often Copy or Data Flow) has stalled | Cancel and retry; for Copy activities, apply performance-tuning guidance |

> 🔑 **Exam fact —** Throttling surfaces as **HTTP 429 or HTTP 430**, or as error code **2003** (subscription-level concurrency) — all three are the same "too much at once" problem, not an authentication or configuration fault. Dataflow Gen2 layers its own **300-refreshes-per-24-hours** limit plus **burst protection** on top of that.

**Dataflow Gen2 refresh limits (memorise):** up to **300 refreshes per 24-hour rolling window** (**150** for non-CI/CD dataflows); a single query evaluation capped at **8 hours**; total refresh time capped at **24 hours**; max **50** staged/output-destination queries per dataflow. A short **burst** of refresh requests (many within **60 seconds**) can trigger throttling even when the daily total is well under the cap. Three consecutive scheduled-refresh failures over a defined window — **72 hours with 100% failure and ≥6 attempts**, or **168 hours with 100% failure and ≥5 attempts** — automatically pauses the schedule and emails the dataflow owner.

### Parameter and expression errors

| Error / message | Cause | Resolution |
| :--- | :--- | :--- |
| Web activity **Error Code 2105** — "The value type '…' is not expected type '…'" | Dynamic content expression produces data that doesn't match the key's expected JSON type | Fix the dynamic content expression to produce the correct type |
| "the result of the evaluation of 'foreach' expression … is of type 'String'. The result must be a valid array" | An **Execute Pipeline** activity parameter typed as array is actually passed to the child pipeline as a string | Wrap the value with the `createArray()` expression function before passing it to the child pipeline |
| Common **Error code 2103 / 2104 / 2105** (connector-agnostic) | Missing required property, wrong property type, or malformed JSON in an activity/connection definition | Match the value to the documented type and required-property list for that connector |

### Other connector-specific error codes

The exam is more likely to test the *pattern* (a numeric `errorCode` range belongs to one connector family, with consistent cause/fix pairs) than any single code.

| Connector | Error code | Message pattern | Cause |
| :--- | :--- | :--- | :--- |
| Azure Functions | 3603 | "Response Content is not a valid JObject" | The called function didn't return a JSON payload — Fabric Function activities only support JSON responses |
| Azure Functions | 3608 | "Call to provided Azure function '…' failed with status-'…'" | The function itself returned an error status — check the function's own logs |
| Azure Batch | 2502 | "Cannot access user storage account; please check storage account settings" | Incorrect storage account name or access key in the Batch connection |
| Azure Batch | 2507 | "The folder path does not exist or is empty" | No executable files exist at the specified `folderPath` in the storage account |
| Azure Machine Learning | 4121 | "Request sent to Azure Machine Learning… failed with http status code…" | The credential used to access Azure ML has expired |
| Azure Machine Learning | 4124 | Same pattern, different underlying cause | The published Azure ML pipeline endpoint referenced by the connection doesn't exist |

> 🧠 **Mental model —** Pipeline error codes are **zip-coded by connector**: 3200s = Azure Databricks, 3600s = Functions, 4100s = Azure ML. Recognising the *family* narrows the fix to that connector's documented cause/recommendation pairs.

### Recovery actions: rerun and retry

- Pipeline monitoring supports **rerun the entire pipeline** or **rerun only from the failed activity** — the latter skips every already-succeeded activity and resumes exactly where the failure occurred.
- Rerun-from-failure assumes upstream activities are idempotent or wrote data that shouldn't be duplicated by a second full pass. For **non-idempotent sinks** (an append-only Copy activity with no dedup logic), rerun-from-failure avoids the duplicate-write risk a full rerun introduces.
- Activities also support built-in **retry** configuration (retry count and interval) on the activity's **General** tab — automatic, fires without leaving the pipeline run, separate from a manual rerun. Use it for flaky external systems (a REST API with occasional 5xx). It is **not** a substitute for fixing a broken configuration: a `UserError` fails identically on every retry attempt.

### Dataflow Gen2 refresh diagnostics

- **Download detailed logs** on the run's detail screen returns a zipped bundle of mashup-engine logs — available a few minutes after the refresh completes, **retained for 28 days**, and the standard artifact to attach to a Microsoft support case.
- Gateway-based refreshes require **Admin consent for gateway diagnostics** enabled at **both the tenant and gateway level** before this button works.
- **Refresh cancellation outcomes differ by destination:** a query loading to **staging** keeps the data from the last successful refresh if cancelled mid-run; a query loading to a **data destination** (Lakehouse/Warehouse) keeps whatever was written up to the cancellation point.

### Query folding failures

Query folding pushes Power Query transformation steps back to the source (SQL pushdown) instead of pulling all rows into the mashup engine. A folding failure is **not** a hard error — the dataflow still refreshes successfully — but performance degrades sharply because every step now runs client-side against the full unfiltered dataset. Common folding breakers: a custom column using an M function with no source-side equivalent; mixing data from two different connections in one query; a `Table.Buffer` call that intentionally materialises the table and stops folding from that point forward.

> ⚠️ **Trap —** Don't hunt for a query-folding *error*. Folding failures never throw `DataFormat.Error` or `DataSource.Error` — the refresh succeeds, just slower. If a refresh is taking far longer than its historical average (visible on the Monitoring hub dashboard) with **no error in the run**, suspect folding loss and check the query's step-by-step Power Query diagnostics rather than looking for a message in refresh history.

### Power Query error patterns

| Error family | What it means | Typical trigger |
| :--- | :--- | :--- |
| **`DataFormat.Error`** | The data doesn't match the shape/type Power Query expected | An older file type (`.xls`/`.xlsb` instead of `.xlsx`/CSV); a value like `#N/A` that can't convert to the target column type, producing **"We couldn't convert to Number"**-style failures |
| **`DataSource.Error`** | The connection to the source or destination itself failed | "Unable to read data from the transport connection: An existing connection was forcibly closed by the remote host" (intermittent network drop); a gateway not configured to reach the destination directly |
| **`Expression.Error`** | The M expression / mashup logic itself is invalid | A referenced step or column no longer exists; a type mismatch inside a custom M function; a syntax error from manual M edits |

- For a `DataFormat.Error` on a specific row: select all columns in the dataflow editor and use **Keep Rows → Keep Errors** to isolate exactly which rows are malformed before deciding to filter, transform, or fix at source.
- For intermittent `DataSource.Error` failures: enable **verbose diagnostics** in the dataflow's settings to capture detailed tracing for the specific failure window.

### Symptom → cause → where to look (§1)

| Symptom | Most likely cause | Where to look / fix |
| :--- | :--- | :--- |
| Pipeline ran fine for months, now fails with no pipeline changes | Stale auth token — `LSROBOTokenFailure` (password change, device removal, Conditional Access update) or Databricks token expiry | Activity Output JSON `message`; update and save the pipeline (bulk-update script for many pipelines) |
| Dataflow query succeeds standalone but fails when another query references its staged output | Gateway firewall blocking outbound TCP 1433 (TDS read-back) | Gateway server firewall rules; or combine/de-stage the referencing queries |
| Many pipelines start failing at once with throttling-flavoured messages | Error 2003 or `CapacityLimitExceeded` | Capacity Metrics app Compute/Timepoint pages |
| Dataflow refresh duration creeping upward, no error in refresh history | Query folding silently lost | Power Query step-by-step diagnostics, **not** refresh history |
| A specific row/value breaks refresh every time | `DataFormat.Error` | Keep Rows → Keep Errors in the dataflow editor |
| Downstream item reading a Dataflow Gen2 intermittently reports "key didn't match any rows" | Internal API timeout in the **Dataflows connector**, not missing data | Point downstream items at the dataflow's Lakehouse/Warehouse destination instead |
| Scheduled dataflow refresh silently stops running | 100% failure rate over the 72h/168h window auto-paused the schedule | Fix the underlying failure, then manually re-enable the schedule |
| Generic connector error code with no obvious fix | Numeric `errorCode` maps to a documented connector-specific cause/recommendation pair | Look up the exact code in the pipeline troubleshooting guide rather than guessing from the message |

---

## 2. Notebook and T-SQL Errors
*Source: `02-notebook-tsql-errors.md`*

### Spark session and capacity errors

Fabric allocates Spark compute from capacity in **Spark VCores**: **1 capacity unit = 2 Spark VCores**, with **bursting** allowing up to **3×** the purchased VCores when capacity is otherwise idle. When a notebook or lakehouse job is submitted while capacity is fully used:

```text
[TooManyRequestsForCapacity] HTTP Response code 430: This Spark job can't be
run because you have hit a Spark compute or API rate limit. To run this Spark
job, cancel an active Spark job through the Monitoring hub, or choose a larger
capacity SKU or try again later.
```

- Jobs triggered from **pipelines, the scheduler, or Spark Job Definitions** are automatically **queued (FIFO)** and retried as capacity frees up — but **queue expiration is 24 hours from submission**, after which the job is dropped and must be resubmitted.
- Queueing does **not** apply to **interactive notebook runs** or **notebook jobs submitted via the public API** — those fail immediately instead of queueing.
- If the capacity itself is in a **throttled** state, new Spark jobs are **rejected outright** rather than queued.

| Error / message | Cause | Resolution |
| :--- | :--- | :--- |
| `TooManyRequestsForCapacity` (HTTP 430) | Capacity's available Spark VCores fully consumed | Cancel an active job via Monitor hub, wait for the queue, choose a larger SKU, or enable Autoscale Billing to move Spark off shared capacity entirely |
| `403 Forbidden` / "User is not authorized… requires ReadAll permission" | Missing workspace role (need **Contributor+**) or missing item-level `ReadData`/`ReadAll` permission on the Lakehouse/notebook | Grant the correct workspace role and item permission; for Spark specifically grant **ReadAll**, not just **ReadData** |
| `SparkContextInitializationTimedOut` | Driver failed to initialize within the timeout — often large/numerous custom libraries slowing startup, or resource contention | Reduce custom library count/size; verify sufficient free capacity; check for VNet/private-endpoint connectivity delays |
| `SparkSubmitProcessTimedOut` / `PersonalizationFailed` | `spark-submit` took too long to start, or custom environment/library setup failed during session personalization | Reduce large custom libraries; remove recently added packages one at a time to isolate the failure |
| `Spark_System_YARNApplication_KilledByTrustedServiceUser` (**exit code 13**) | Invalid Spark configuration (bad units, out-of-range value) killed the session on startup | Review `%%configure` values — e.g. `spark.rpc.message.maxSize` must be a plain integer, capped at **2047 MB** |

> 🧠 **Mental model —** Spark admission is three doors in sequence. **Queueing** = a waiting room (you get in within 24 hours once a seat frees). **Throttling** = a bouncer slowing the door (requests delayed, not dropped). **Rejection** (`CapacityLimitExceeded`) = the door closed entirely — the platform is past throttling, there is truly no room, the request fails outright rather than waiting.

### Out-of-memory: driver vs executor

Exit codes are the entry point for any Spark memory investigation, read from the **Executors** tab of the Spark UI (Monitor hub → application → **Jobs** tab → select a job description).

| Exit code | Meaning | Most likely cause |
| :--- | :--- | :--- |
| **137** | Killed by OS (`SIGKILL`) | Out of memory — container exceeded its memory limit |
| **143** | Terminated (`SIGTERM`) | Timeout, preemption, or a normal dynamic-allocation scale-down |
| **134** | Aborted (`SIGABRT`) | JVM crash or native memory corruption |
| **1** | General error | User code exception, misconfiguration, or missing dependency |
| **-100** | Container preempted/lost | Node preemption or platform-side container loss |

Container memory = `spark.executor.memory + spark.executor.memoryOverhead`. In Fabric, `memoryOverhead` defaults to a fixed **384 MB regardless of node size** (unlike open-source Spark's `max(384 MB, 10% of executor memory)`), which is frequently too small for PySpark UDFs, large shuffles, or native libraries.

**Driver OOM** — pulling too much data to the driver process:

- `.collect()`, `.toPandas()`, or `display(df)` without a `.limit(N)` first — all three pull the *entire* result set into driver memory
- A broadcast join where Spark auto-broadcasts a table too large for the configured threshold
- Very complex query plans under Adaptive Query Execution (AQE), which regenerates plan text on every plan change — mitigate with `spark.sql.maxPlanStringLength`

**Executor OOM** — uneven or oversized partitions:

- **Data skew** — a handful of tasks process **10–100×** more input than the rest (confirm in the Spark UI **Stages** tab, sorted by input size); fix with `spark.sql.adaptive.skewJoin.enabled` (AQE is **on by default** in Fabric) or manual salting
- **Too few partitions** for the data volume — repartition toward **128–256 MB per partition**
- **Over-caching** — multiple large `.cache()`/`.persist()` calls without `.unpersist()`
- **PySpark/Pandas UDFs** — a separate Python process competes with the JVM executor for the same node memory; prefer built-in Spark SQL functions, or reduce `spark.sql.execution.arrow.maxRecordsPerBatch`

> ⚠️ **Trap —** Reflexively increasing `spark.executor.memory` for every OOM. If the Spark UI **Stages** tab shows a handful of tasks with dramatically larger input than the rest, the problem is **skew** — more memory per executor doesn't fix it, because the oversized partition still lands entirely on one executor. Enable AQE skew-join handling or repartition first; only scale memory once partition sizes are balanced.

> 🧠 **Mental model —** `spark.executor.*`, `spark.driver.*`, `spark.network.*` and `spark.yarn.*` are **poured at session cast time** — read once when the session/executors launch, unreshapeable afterwards with `spark.conf.set()`. Put them in a `%%configure` cell as literally the **first cell** in the notebook (it restarts the session). Only `spark.sql.*` settings (AQE, shuffle partitions, broadcast threshold) are **still-wet clay** — changeable mid-session with `spark.conf.set()`.

```python
%%configure
# must be the FIRST cell — restarts the session.
# spark.executor.* / spark.driver.* / spark.network.* / spark.yarn.* belong here.
# A bad value here kills the session on startup: exit code 13,
# Spark_System_YARNApplication_KilledByTrustedServiceUser.
{ "conf": { "spark.rpc.message.maxSize": 512 } }   # plain integer, max 2047 (MB)
```

```python
# mid-session: spark.sql.* only
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### AnalysisException patterns

`AnalysisException` fires during Spark's **query analysis phase**, before any data is processed — almost always a user-side authoring issue (typo, missing table, incompatible type). Because it fails early, no compute is wasted.

| Scenario | Message pattern | Fix |
| :--- | :--- | :--- |
| Table/view not found | `Table or view not found: my_table` | Check spelling/case; use a fully-qualified `catalog.schema.table` name; re-create expired temp views |
| Column not found | `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` cannot be resolved, with suggested valid columns listed | Compare against `df.printSchema()`; check for an upstream rename/drop |
| Ambiguous reference | `[AMBIGUOUS_REFERENCE] Reference 'X' is ambiguous` | Qualify the column with its table alias (`a.id` vs `b.id`) after a join |
| Data type mismatch | `Data type mismatch: differing types in '(col_a = col_b)': int vs string` | Explicitly `.cast()` to a common type before comparing/joining |
| Schema mismatch on write | `A schema mismatch detected when writing to the Delta table` | Compare `df.printSchema()` against `DESCRIBE my_table`; align columns/types, or use `.option("mergeSchema", "true")` for intentional evolution |
| Path doesn't exist | `Path does not exist: abfss://...` | Verify the path in the Lakehouse explorer; a "path not found" sometimes really means "access denied" |

### Session, library and runtime errors

| Error / message | Cause | Resolution |
| :--- | :--- | :--- |
| `Spark_User_Conda_PipFailed` | Library install failed during environment setup — bad package name/version, a conflicting pre-installed package version, or a missing system-level dependency | Verify the package on PyPI; remove version pins to let pip resolve compatibility; check the Fabric runtime's pre-installed package list |
| `Spark_System_MetaStore_HiveException` — "lock acquisition timed out" | Concurrent DDL (`ALTER`/`DROP`/`CREATE`) from multiple sessions against the same table | Avoid concurrent DDL on one table; retry after the other session's operation completes |
| `Spark_Ambiguous_UserApp_NullPointer` / Python `KeyError`/`TypeError`/`AttributeError` | Nulls reaching a UDF, a missing dictionary key, or an API method called on the wrong object type (e.g. calling `.show()` on the *list* returned by `.collect()`) | Filter nulls before UDF calls; use `.get(key, default)`; check Spark version migration notes for renamed/removed methods |
| `INCONSISTENT_BEHAVIOR_CROSS_VERSION` — datetime values shift after a runtime upgrade | Spark 3.0+ switched to the Proleptic Gregorian calendar; historical dates written under the old (Julian-hybrid) behaviour read back differently | Set `spark.sql.parquet.datetimeRebaseModeInRead/Write` to `CORRECTED`, but validate on a sample first — `CORRECTED` on `LEGACY`-written historical data (pre-1582) can **silently shift values** |

**Spark UI tabs and what each answers:** **Executors** = exit codes and memory · **Stages** = skew/duration · **Storage** = cached DataFrame size vs node RAM · **Environment** = the active configuration. For text-level detail beyond the graphs — the exact stack trace, `stdout`/`stderr` — download **Driver**, **Livy** or **Prelaunch** logs from the application's **Logs** tab. Logs may be **unavailable** if the job never left the queue or cluster creation failed; in that case check capacity utilization instead.

### T-SQL Warehouse: unsupported syntax and data-type errors

Fabric Warehouse and the SQL analytics endpoint support a **subset** of T-SQL. Attempting unsupported commands can *appear to succeed* while corrupting warehouse state, so don't rely on trial and error.

**Not supported (memorise):** `BULK LOAD`, `CREATE USER`, materialized views, `PREDICT`, recursive queries, `SELECT…FOR XML`, `SET ROWCOUNT`, `SET TRANSACTION ISOLATION LEVEL`, `sp_showspaceused`, synonyms, triggers, and the vector data type / vector search functions.

`ALTER TABLE` support is limited to: adding **nullable** columns, dropping columns, `NOT ENFORCED` key constraints, and (**preview**) `ALTER COLUMN`. Every other `ALTER TABLE` variant is blocked.

| Error / message | Cause | Resolution |
| :--- | :--- | :--- |
| `SET TRANSACTION ISOLATION LEVEL` runs without error but has no effect | Fabric Warehouse enforces **snapshot isolation on every transaction**; isolation-level changes are **silently ignored** | Don't attempt to change isolation level — design around snapshot isolation's behaviour instead |
| `tempdb` space exhaustion during a large query | Missing/stale column statistics, or a query with heavy `GROUP BY`/`ORDER BY`/`JOIN` returning a huge result set | Verify and refresh statistics; reduce grouping/ordering columns; avoid running concurrently with other heavy workloads |
| A `SELECT` completes on the backend but the client never receives results | Front-end/client-side issue delivering the result set, not a query failure | Retry from a different client (SSMS, VS Code MSSQL extension, Fabric SQL query editor); use `CTAS` to send results to a table instead of back to the client, to isolate backend vs client |

```sql
-- isolate backend vs client: land the result instead of returning it
CREATE TABLE dbo.diag_result AS SELECT ... ;
```

### Transaction and locking conflicts: snapshot isolation

Fabric Warehouse uses **table-level locking** for every statement, regardless of how many rows are touched:

- `SELECT` → Schema-Stability (`Sch-S`)
- `INSERT` / `UPDATE` / `DELETE` / `MERGE` / `COPY INTO` → Intent Exclusive (`IX`)
- DDL → Schema-Modification (`Sch-M`)

Snapshot isolation means readers never block writers and writers never block readers — but **two transactions writing to the same table can still conflict**, evaluated at commit time:

| Error | Message | Trigger |
| :--- | :--- | :--- |
| **24556** | "Snapshot isolation transaction aborted due to update conflict… can cause update conflicts if rows in that table have been deleted or updated by another concurrent transaction. Retry the transaction." | Two transactions both attempt `UPDATE`/`DELETE`/`MERGE`/`TRUNCATE` against the same table |
| **24706** | "Snapshot isolation transaction aborted due to update conflict… cannot use snapshot isolation to access table… to update, delete, or insert the row that has been modified or deleted by another transaction. Please retry the transaction." | Same conflict class, triggered on insert/update/delete against a row another transaction already touched |

- Conflicts are evaluated at the **table level**, not the individual parquet file — so even a `MERGE` that only *appends* new rows can register as a write-write conflict if it isn't the first transaction to commit.
- `INSERT` statements always create new parquet files, so they conflict **less often** than `UPDATE`/`DELETE`/`MERGE`/`TRUNCATE`.
- The documented fix for both errors is identical: **retry the failed transaction** (ideally with exponential backoff). There is no isolation-level workaround, because snapshot isolation is the only level Fabric Warehouse offers.

```sql
MERGE INTO dbo.dim_customer AS t
USING stg.customer_delta AS s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.name = s.name
WHEN NOT MATCHED THEN INSERT (customer_id, name) VALUES (s.customer_id, s.name);
-- concurrent MERGE on the same table -> error 24556 / 24706 -> retry with backoff
```

> 🧠 **Mental model —** Every Warehouse table is a **single shared whiteboard, photographed at the start of each transaction**. Two people can read it freely without blocking each other. But if two people erase and rewrite the same whiteboard at once, only the first to finish keeps their version — the second finds their photo is stale and must redo the edit (retry) against the now-current whiteboard.

### `COPY INTO` rejected-row diagnostics

`ERRORFILE` redirects rows that fail to load instead of aborting the whole statement. In Fabric Data Warehouse `ERRORFILE` applies to **CSV and JSONL only** — **not Parquet**: Parquet data-type conversion errors always fail the whole `COPY INTO`, ignoring `MAXERRORS`.

```sql
COPY INTO dbo.TaxiTrips
FROM 'https://account.blob.core.windows.net/container/data/'
WITH (
    FILE_TYPE = 'CSV',
    ERRORFILE = '/errorsfolder',       -- path relative to the container
    MAXERRORS = 10
)
```

Under the target `ERRORFILE` directory Fabric creates a `_rejectedrows` child folder, then a subfolder named by load timestamp (`YearMonthDay-HourMinuteSecond`), then a subfolder named by **statement ID**. Inside that folder are two files:

| File | Contents |
| :--- | :--- |
| **`error.Json`** | The reject reasons — why each row failed to load |
| **`row.csv`** | The rejected rows themselves, for inspection or reprocessing |

- `MAXERRORS` caps how many rejected rows are tolerated before the whole `COPY INTO` fails. **Default = 0** — any rejected row fails the load unless `MAXERRORS` is raised.
- If `ERRORFILE` points at the full path of a *different* storage account than the source, `ERRORFILE_CREDENTIAL` authenticates to it — **only Shared Access Signature (SAS) is supported** in Fabric. Otherwise the same `CREDENTIAL` used for the source applies.
- When using a **firewall-protected storage account** for the error file, `MAXERRORS` must **also** be specified.

### Query insights for diagnosis

The Warehouse **query insights** views — queryable via T-SQL against `queryinsights.exec_requests_history` and related DMVs — surface historical query text, duration and resource consumption without reproducing the failure live. Useful for intermittent `tempdb` exhaustion or a query that degrades gradually rather than failing outright. Pair query insights with `sys.dm_tran_locks` when a symptom looks like **blocking** rather than an outright error.

### Symptom → cause → fix (§2)

| Symptom | Cause | Resolution |
| :--- | :--- | :--- |
| Notebook fails immediately with HTTP 430 during a busy period | Capacity's Spark VCores fully consumed; **interactive runs don't queue** | Cancel a running job via Monitor hub, wait, or move to Autoscale Billing |
| Executor exit code 137 recurs even after increasing `spark.executor.memory` | The real cause is data skew, not insufficient total memory | Enable `spark.sql.adaptive.skewJoin.enabled`; repartition toward 128–256 MB partitions |
| `AnalysisException: Table or view not found` right after an upstream cell wrote the table | Session/catalog caching lag, or the write hasn't actually completed | Confirm the write succeeded before querying; use a fully-qualified name |
| A script with `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` runs but behaves like snapshot isolation | Fabric Warehouse ignores isolation-level changes and always uses snapshot isolation | Remove the statement; design retry logic around snapshot isolation instead |
| `COPY INTO` fails outright on a Parquet load with a handful of bad rows despite `MAXERRORS = 100` | `MAXERRORS` doesn't apply to Parquet data-type conversion failures | Pre-validate/clean the Parquet source, or switch to CSV/JSONL where `ERRORFILE`/`MAXERRORS` are honored |

---

## 3. Real-Time Errors — Eventhouse and Eventstream
*Source: `03-realtime-errors.md`*

### Eventhouse ingestion failures: `.show ingestion failures`

The KQL management command `.show ingestion failures` returns every recorded ingestion **management-command** failure for a database, with a **14-day retention window**. It does **not** cover every stage of ingestion — for comprehensive coverage across all stages, pair it with ingestion **metrics** and **diagnostic logs**.

| Column | Meaning |
| :--- | :--- |
| `Database` / `Table` | Where the failure occurred |
| `FailedOn` | UTC timestamp of the failure |
| `Details` | The actual root-cause message |
| **`FailureKind`** | **`Permanent`** or **`Transient`** — the single most exam-relevant column |
| `ErrorCode` | The specific ingestion error code |
| `OriginatesFromUpdatePolicy` | Boolean — whether the failure happened while an update policy was executing |

```kusto
.show ingestion failures
| where FailedOn > ago(1d)
| where FailureKind == "Permanent"
```

> 🧠 **Mental model —** A **Permanent** failure is a **returned letter, undeliverable address** — resending changes nothing; fix the address (format, mapping, permission) first. A **Transient** failure is a **delayed flight** — the same request will likely get through once the runway (throttling, timeout, network blip) clears.

### Ingestion error code categories

| Category | Representative errors | `FailureKind` | Typical fix |
| :--- | :--- | :--- | :--- |
| **BadFormat** | `Stream_WrongNumberOfFields`, `Stream_ClosingQuoteMissing`, `BadRequest_InvalidMapping`, `BadRequest_FormatNotSupported` | Permanent | Fix the source file's structure or the ingestion mapping — retrying won't help |
| **BadRequest** | `BadRequest_EmptyBlob`, `BadRequest_InvalidOrEmptyTableName`, `BadRequest_DuplicateMapping`, `Stream_DynamicPropertyBagTooLarge` | Permanent | Fix the request / ingestion-properties definition |
| **DataAccessNotAuthorized** | `Download_Forbidden`, `BadRequest_TableAccessDenied`, `BadRequest_InvalidAuthentication` | Permanent | Grant the missing role/permission on the source storage or target table |
| **DownloadFailed** | `Download_NotTransient` (Permanent); `Download_UnknownError` / `Download_TransientNameResolutionFailure` (Transient) | Mixed | Transient variants: retry. `NotTransient`: investigate storage connectivity directly |
| **EntityNotFound** | `BadRequest_DatabaseNotExist`, `BadRequest_TableNotExist`, `BadRequest_MappingReferenceWasNotFound` | Permanent | Create the missing entity or fix the reference name |
| **FileTooLarge** | `Stream_InputStreamTooLarge`, `BadRequest_FileTooLarge` | Permanent | Split the source file/field below the size limit |
| **InternalServiceError** | `General_InternalServerError`, `Timeout`, `OutOfMemory` (Transient); `Schema_PermanentUpdateFailure` (Permanent) | Mixed | Transient variants: retry; schema failures need manual correction |
| **UpdatePolicyFailure** | `UpdatePolicy_QuerySchemaDoesNotMatchTableSchema` (Permanent); `UpdatePolicy_IngestionError` (Transient) | Mixed | See Update policy failure behaviour below |
| **ThrottledOnEngine** | `General_ThrottledIngestion` | Transient | Retry, or reduce ingestion rate / batch size |
| **RetryAttemptsExceeded** | `General_RetryAttemptsExceeded` | Permanent | The platform already retried repeatedly on your behalf and gave up — investigate the underlying transient cause directly rather than retrying again |

> ⚠️ **Trap —** Assuming *any* ingestion failure is worth an automatic retry. `General_RetryAttemptsExceeded` means Kusto **already retried** past a recurring transient error and gave up. Retrying again from the application layer just repeats a failure the engine has exhausted its patience with. Treat it as effectively permanent and go find the underlying transient cause (storage throttling, a flaky network path).

### Streaming vs queued ingestion error surfaces

- **Queued ingestion** batches data through the Data Management (DM) service before committing — optimised for high throughput, with a default **batching latency** (time *or* size threshold, whichever hits first) before a batch commits. Failures are visible via `.show ingestion failures` and the workspace-monitoring **Ingestion results logs** table family.
- **Streaming ingestion** commits rows with **sub-second latency** directly into the engine, bypassing the DM queue. Failures surface faster but through the same error-code taxonomy — streaming failures still classify as Permanent/Transient the same way.

> ⚠️ **Trap —** Confusing **batching latency** with an ingestion failure. If rows aren't queryable immediately after a queued-ingestion call, that's expected — the batch hasn't committed yet, not a silent failure. Check the configured batching policy (time/size/count threshold) before assuming something broke; only escalate to `.show ingestion failures` if data is missing *after* the batching window has clearly elapsed.

### Update policy failure behaviour

An **update policy** on a source table automatically runs a query against newly ingested data and writes the result into one or more target tables — how Eventhouse implements derived/transformed tables without a separate pipeline.

| Configuration | What happens when the update-policy query fails |
| :--- | :--- |
| **Transactional** (`IsTransactional = true`) | The **entire ingestion into the source table is rolled back** — nothing commits, including the original raw data, if any transactional update policy on it fails |
| **Non-transactional** (**default**) | The **source table's ingestion still commits** even if the update-policy query fails; only the derived target table's write is skipped, and the failure is logged (`UpdatePolicy_IngestionError` or `UpdatePolicy_QuerySchemaDoesNotMatchTableSchema`) |

- `UpdatePolicy_QuerySchemaDoesNotMatchTableSchema` is **Permanent** — the update-policy query's output columns don't match the target table's schema; fix the query or the target schema.
- `UpdatePolicy_IngestionError` and `UpdatePolicy_UnknownError` are **Transient** (retry-worthy), reported against the **source** and **target** table respectively.

> 🧠 **Mental model —** A **non-transactional** update policy is a **photocopier attached to a filing cabinet** — if the copier jams, the original still goes in the cabinet; only the copy is missing. A **transactional** update policy is a **two-part carbon-copy form** — if the carbon copy can't be produced, the whole form (original included) is rejected, because the design assumes both copies must exist together.

### Eventstream source connection errors

| Error / message | Cause | Resolution |
| :--- | :--- | :--- |
| **AADSTS65002** — "Fabric Eventstream isn't preauthorized to access Azure Event Hub through Azure AD" | The Eventstream service principal isn't preauthorized against that specific Event Hubs namespace at the Microsoft Entra **tenant level** | End users **cannot self-resolve** this — it requires tenant-level preauthorization; escalate to a tenant admin or Microsoft support rather than retrying connection settings |
| `EventHubNotFound` (Activator-side, diagnostic of the same root cause) | The eventstream feeding an object was deleted, or the connection to a downstream Fabric Activator item was removed | Recreate the eventstream or reconnect the Activator object to a valid eventstream |
| `EventHubException` | Eventstream received an exception importing data from the source Event Hub | Open the eventstream, inspect the source connection configuration and recent errors |
| `UnauthorizedAccess` | Permissions on the source eventstream item changed after the downstream connection was configured | Restore the required permission on the eventstream item for the consuming identity |
| `IncorrectDataFormat` | Source data isn't in the expected JSON dictionary format | Reshape the source payload to valid JSON before it reaches the eventstream |
| Connection timeout to Azure Event Hubs | Network latency/connectivity between Fabric and the Event Hubs endpoint — firewall, VPN or NSG interference | Verify no firewall/VPN/NSG blocks the path; check Event Hubs-side throttling |

### Throughput, throttling and destination delivery

Eventstream throughput problems rarely throw a hard error — they show up as **lag** (destination data arriving later than expected) or **dropped events**. Common causes: an undersized Event Hubs **throughput-unit** allocation on the source; a destination (Lakehouse, Eventhouse) that can't sustain the incoming rate; or a downstream Eventhouse **paused/scaled down** between capacity changes. Enabling **Eventhouse Always-On** or setting a **minimum consumption unit** prevents pause-triggered ingestion gaps.

### Runtime logs and Data insights

| Panel | Diagnoses | Availability |
| :--- | :--- | :--- |
| **Data insights** | Throughput and status metrics for the eventstream and its sources/destinations; scoping to a specific node shows that node's metrics | Requires an Event Hubs source or Lakehouse destination for the scoped node |
| **Runtime logs** | Detailed engine-level logs at three severities — **warning**, **error**, **information** | Same source/destination requirement as Data insights |

> 🧠 **Mental model —** **Data insights** answers "**is data flowing, and how fast**" — the speedometer and fuel gauge. **Runtime logs** answers "**why did the engine do what it did**" — the diagnostic code reader. "Did an event get dropped or transformed unexpectedly?" → Runtime logs. "Is my eventstream even receiving anything?" → Data insights.

### Common processor errors: schema drift and type mismatch

Eventstream's built-in transformation processors (filter, aggregate, join, manage fields) can fail or silently misbehave when the source schema changes.

| Symptom | Cause | Resolution |
| :--- | :--- | :--- |
| A processor's output is missing fields present in earlier events | **Schema drift** — the source added, removed or renamed a field, and the processor was configured against the old schema | Reconfigure the processor against the current schema; use the **live sample view** to confirm schema correctness before saving |
| A filter/aggregate condition silently never matches | **Type mismatch** — a field's type changed (e.g. string vs numeric) between the processor's configuration and the live data | Inspect the field's live sample value/type; adjust the condition or add an explicit cast/transform upstream of the processor |
| Downstream Activator rule never fires despite data flowing | Field-name typo or type mismatch between the eventstream schema and the rule's object-key mapping | Use Eventstream's live sample view and Activator's rule preview together to confirm the schema binding before troubleshooting rule logic |

### Diagnosing via workspace monitoring KQL

For failures beyond the 14-day window or spanning multiple tables/databases at once, query the monitoring Eventhouse's **Ingestion results logs** table directly instead of the per-database `.show ingestion failures` command:

```kusto
IngestionResultsLogs
| where Timestamp > ago(7d)
| where Result == "Failure"
| summarize FailureCount = count() by Table, FailureKind, ErrorCode
| order by FailureCount desc
```

Aggregating by `Table`, `FailureKind` and `ErrorCode` over a rolling window is the fastest way to spot a table silently accumulating **permanent** failures (a broken upstream mapping) versus one with a transient blip that resolved itself. Pair with the **Command logs** and **Data operation logs** table families when a failure originates from a management command (a manual `.ingest` or schema change) rather than the normal ingestion pipeline.

### Symptom → cause → fix (§3)

| Symptom | Cause | Resolution |
| :--- | :--- | :--- |
| Ingestion keeps failing on retry with the same error | `FailureKind = Permanent` — retrying doesn't change a bad format, missing entity, or authorization problem | Fix the underlying data/config issue named in `Details`/`ErrorCode` before re-ingesting |
| Derived table empty or stale while the source table has current data | Non-transactional update policy silently failing on the query (schema mismatch) | Check `.show ingestion failures` filtered to `OriginatesFromUpdatePolicy == true`; fix the update-policy query or target schema |
| Eventstream shows connected but no data appears in the destination | Data insights shows zero throughput at the source node — upstream Event Hubs config or firewall/NSG rules | Verify Event Hubs throughput-unit allocation and network path; confirm AADSTS65002 isn't blocking the connection |
| A processor condition that used to match stops matching after a source change | Schema drift or a type change in the evaluated field | Use the live sample view to confirm current field name/type; update the processor configuration |
| Data appears in Eventhouse later than expected but isn't missing | Normal queued-ingestion batching latency, not a failure | Check the batching policy threshold before escalating; use streaming ingestion if sub-second latency is required |
| Need ingestion failure trends beyond 14 days / across all tables | `.show ingestion failures` is capped at 14 days and is command-level only | Enable workspace monitoring; query the Ingestion results logs table family in the auto-provisioned monitoring Eventhouse |

---

## 4. OneLake Shortcut Errors
*Source: `04-shortcut-errors.md`*

Shortcut errors are almost always **identity and permission** problems wearing a storage-error costume — a 403 that looks like a broken connection is usually a stale delegated credential, and a query that "just returns different data" between engines is usually a security-mode mismatch, not corruption.

### Shortcut authentication models

| Shortcut type | Model | Notes |
| :--- | :--- | :--- |
| OneLake → OneLake | **Passthrough** (default) or delegated | Passthrough passes the calling user's identity straight to the target — the source system stays in full control of its own access rules |
| External (S3, ADLS, GCS, Blob, Dataverse, OneDrive/SharePoint) | **Delegated only** | Always uses an intermediate credential (another identity, service principal, or account key) — end users never need direct access to the external system |

- **Passthrough:** OneLake uses the calling user's own identity to authorize at the target. If that user lacks permission at the target, the shortcut denies them regardless of what permission they hold at the shortcut path.
- **Delegated:** the shortcut accesses data through a configured connection identity, and the calling user sees the **intersection** of their own security and whatever security applies to the delegated identity. A delegated shortcut can therefore **never grant a user more access than the delegated identity itself has**, even if the user's own permissions would allow more.

> ⚠️ **Trap —** Assuming Direct Lake or T-SQL always passes the calling user's identity through a shortcut. **Power BI semantic models using Direct Lake over SQL**, and **T-SQL engines in Delegated identity mode**, both use the **item owner's identity** instead of the end user's. OneLake security roles still filter what the end user sees, but the actual storage access happens as the owner. This exception is exactly why the same shortcut behaves differently depending on which engine and identity mode is querying it.

### 401 vs 403 vs 404 semantics

| Status | Meaning in a shortcut context | Typical root cause |
| :--- | :--- | :--- |
| **401 Unauthorized** | Authentication itself failed — no valid identity was established | Expired or missing token; malformed Authorization header; expired SAS token |
| **403 Forbidden** | Authentication succeeded, but the identity isn't authorized for this operation | Missing Fabric Read permission at the consumer; missing OneLake security Read at the target; `AuthorizationPermissionMismatch` from a revoked storage role; delegated identity lost access at the producer |
| **404 Not Found** | The target path no longer exists where the shortcut points | Target file/folder/table deleted, renamed, or moved without updating the shortcut |

A shortcut query commonly surfaces 403 with messages like *"This request is not authorized to perform this operation using this permission"* or `AuthorizationPermissionMismatch`. If the message reads *"The SELECT permission or external policy action… was denied on the object"*, that is usually a **user identity mode Object ID mismatch**, not a storage-level permission gap.

> 🧠 **Mental model —** **401** = a locked door with **no ID shown at all**. **403** = you **showed valid ID but you're not on the guest list**. **404** = the door **isn't even there any more**. Three different fixes: get a valid credential, get added to the list, or find where the room went.

### Delegated credential expiry: the classic failure

The most exam-representative shortcut failure: a **delegated** shortcut (or a SQL analytics endpoint in delegated identity mode) keeps working *after* the producer revokes the item owner's access or narrows a OneLake security rule, because the consumer is running against a **cached storage access token** for the item owner — not re-checking permissions on every query.

| Permission layer changed | Propagation delay |
| :--- | :--- |
| SQL `GRANT`/`REVOKE` or a SQL security policy at the consumer | Applies on the **next query** — no cache delay |
| Item owner's OneLake permissions at the producer (shortcut-backed tables) | Cached storage token can remain valid **up to 30–60 minutes** |
| A new OneLake security role/filter created at the producer (affecting a shortcut source) | Only takes effect once the owner's storage token **refreshes** — same 30–60 minute window |
| OneLake security **role membership** changes at the producer | **Not** delayed by the consumer's token cache — takes effect on the normal sync path |

To force a faster refresh when the 30–60 minute wait is unacceptable: **pause and resume the Fabric capacity** hosting the artifact (safest, clears backend caches), or **switch the consumer endpoint to user identity mode and back to delegated mode** (invalidates cached tokens). **Both actions drop existing SQL connections** on any SQL analytics endpoints/warehouses in the affected workspace — a real operational tradeoff, not a free fix.

> ⚠️ **Trap —** Treating "I revoked access but the user can still query the data" as a security bug to escalate immediately. In delegated mode this is **documented, expected caching behaviour**. The fix is understanding the 30–60 minute cache window and, if needed, forcing a refresh via capacity pause/resume or a mode switch — not assuming OneLake security failed.

### S3 / ADLS-specific issues

| Issue | Cause | Resolution |
| :--- | :--- | :--- |
| Shortcut to ADLS Gen2 fails with 403 despite correct-looking credentials | Missing **Storage Blob Data Reader/Contributor** role assignment on the target storage account for the delegated identity/service principal | Assign the correct role in **Azure IAM on the storage account**, not just at the Fabric connection level |
| S3/GCS shortcut works initially, then fails after a source-side key rotation | The cloud connection's stored credential (account key, access key) wasn't updated after rotation | Update the connection's credential; only users with permission on the connection can rebind it |
| Cross-cloud shortcut is slow or times out | Firewall rules on the external storage account/bucket blocking Fabric-side egress IP ranges, or an incorrectly scoped endpoint (`.blob` vs `.dfs` for ADLS Gen2) | Allow Fabric's published IP ranges/service tags; use the **`.blob` endpoint** for ADLS Gen2 when `.dfs`-specific features aren't required — it currently yields the best performance |
| Creating a new S3/ADLS shortcut fails with "no permission on connection" | The creating user lacks permission on the underlying **cloud connection** object, separate from workspace/item permissions | Grant the user permission on the specific connection, or have a connection-permitted user create the shortcut |

### Cache staleness beyond credentials

Shortcut **file caching** (distinct from credential-token caching) reduces cross-cloud egress cost:

- OneLake stores files read through an external shortcut in a **per-workspace cache** with a configurable **1–28 day retention period**, **reset on every access**.
- Files **over 1 GB aren't cached**.
- Caching currently supports **Google Cloud Storage, S3, S3-compatible, and on-premises data gateway** shortcuts.
- If the remote source has a newer version of a cached file, OneLake **detects this and serves the fresh copy**, updating the cache — so caching does **not** cause silently stale *data*. It causes silently stale *tokens* (above) and, separately, briefly stale **query results**: switching between user identity and delegated mode can return cached query results based on the previous security state for **up to 1 hour**.

### Transitive shortcuts and cross-region notes

A **transitive shortcut** is a shortcut whose target is itself another shortcut. Fabric caps chaining: the **maximum number of direct shortcut-to-shortcut links is 5**, and a single OneLake path supports at most **10 shortcuts** pointing at it. Renaming, moving or deleting a shortcut *target* can break every shortcut downstream in the chain — and because shortcuts **don't support cascading deletes**, deleting a shortcut object itself never touches the target data, only the pointer.

Cross-region access through a shortcut works but adds latency proportional to physical distance between the capacity and the target storage. There is **no documented hard limit forcing same-region shortcuts**, but query performance degrades with distance.

> 🧠 **Mental model —** A transitive shortcut is a **relay race baton pass**. Each shortcut hands off to the next, and the platform limits the relay to **5 handoffs** before refusing another leg. Beyond that, chase down the original source and shortcut directly to it.

### OneLake security interplay: delegated mode blocks data-level rules

**The single highest-yield fact in this section: in delegated mode, a shortcut is blocked outright if the source table has Row-Level Security (RLS), Column-Level Security (CLS), or Object-Level Security (OLS) defined in OneLake security.** This is intentional — it prevents an item owner's delegated identity from silently surfacing filtered data to unauthorized end users through a shortcut that bypasses the intended filtering.

| Scenario | Behavior | Resolution |
| :--- | :--- | :--- |
| Source table has OneLake-level RLS/CLS/OLS; consumer shortcut is in delegated mode | Shortcut query **fails** | Switch the consumer endpoint to **user identity mode** so the end user's identity is evaluated against the source's rules directly, or remove the OneLake-level rules and reimplement equivalent filtering in SQL (RLS/CLS/DDM) at the consumer |
| Same query run once through Spark and once through the SQL analytics endpoint in delegated mode | **Different row counts/values** — Spark enforces OneLake security; delegated-mode SQL doesn't | Switch the endpoint to user identity mode to align results, or replicate the filtering in SQL constructs at the SQL layer |
| Multiple OneLake security roles apply to the same user on one table — one restrictive (RLS), one permissive (full access) | The **most permissive role wins** — RLS appears not to apply | Keep restrictive and permissive roles **mutually exclusive** per user/group if RLS enforcement is required |
| Data reached through a shortcut from a Warehouse with SQL-level RLS/CLS | Warehouse SQL security constructs are enforced **only in the Warehouse's own SQL execution context (TDS endpoint)** — they don't translate into OneLake security policies for shortcut access | Apply equivalent OneLake security rules directly at the source lakehouse, or restrict access to the shortcut consumer |

> ⚠️ **Trap —** Debugging "my shortcut query returns nothing in delegated mode" as a broken connection. If the source table has any OneLake-level RLS/CLS/OLS, delegated mode **denies the shortcut entirely by design** — there is no partial or filtered result to expect. Check for data-level rules at the source before investigating connectivity, credentials, or the target path.

### Diagnosing across engines

When the same shortcut behaves differently across surfaces, check in this order:

1. **Access mode** — open the SQL analytics endpoint's **Security tab → View data access mode** to confirm delegated vs user identity mode before investigating further.
2. **OneLake security sync status** — in Object Explorer expand **Security → Roles → Database Roles** and look for `OLS_`-prefixed roles. Sync can lag **up to 5 minutes**, and a broken policy reference (a renamed/dropped column referenced by an RLS/CLS rule) can **stall sync indefinitely** until fixed.
3. **Object ID match** — in user identity mode, the identity referenced in the OneLake security role must be **directly** granted Fabric Read at the consumer; **nested/effective group membership across the producer→consumer boundary is not resolved**.
4. **Semantic model mode** — **Direct Lake over SQL** uses the delegated/owner path; **Direct Lake over OneLake** passes the calling user through directly — the same discrepancy pattern as Spark vs delegated SQL.

### Same root cause, different error per engine

| Underlying change at the producer | Symptom in Spark | Symptom in SQL analytics endpoint (delegated) | Symptom in Direct Lake over SQL |
| :--- | :--- | :--- | :--- |
| RLS/CLS/OLS added to the source table | Query correctly filtered per the caller's identity | **Query fails outright** (shortcut blocked in delegated mode) | Uses the owner's identity — filtering may not match the caller's own access |
| Owner's OneLake permission revoked | Immediate **403** for any caller relying on that path | Continues to succeed until the cached token expires (**30–60 min**) | Same caching lag as the SQL delegated path |
| Target renamed/moved | **404** / path-not-found immediately | 404, or **up to 5 minutes of "single-user mode"** blocking queries while the system validates the new target | Requires a **semantic model refresh** to pick up the new target |

If a symptom is inconsistent across engines, suspect a security **mode** difference (delegated vs user identity, or which identity is doing the reading) before suspecting the shortcut itself is broken.

### Symptom → cause → fix (§4)

| Symptom | Most likely cause | Where to check / fix |
| :--- | :--- | :--- |
| Shortcut worked yesterday, now returns 404 | Target file/folder/table renamed, moved, or deleted at the source | Confirm the target path still exists at the source system |
| Shortcut returns 403 despite the connection looking correctly configured | Missing Storage Blob Data role at the **storage-account** level, separate from the Fabric connection | Azure IAM role assignments on the target storage account |
| Revoked access "still works" for a while | Delegated-mode cached storage token hasn't expired | Wait out the 30–60 minute window, or force a refresh (capacity pause/resume or mode toggle — drops existing SQL connections) |
| Query against a shortcut returns fewer rows in Spark but full rows via SQL | SQL analytics endpoint is in delegated mode and isn't honoring OneLake RLS | Endpoint's Security tab → View data access mode; switch to user identity mode or replicate filtering with SQL RLS/CLS/DDM |
| Shortcut query fails outright in delegated mode with no connectivity problem | Source table has RLS/CLS/OLS defined in OneLake security — delegated mode blocks by design | Switch to user identity mode, or remove OneLake-level rules and reimplement filtering in SQL at the consumer |
| Shortcut creation fails even though the user has Contributor on the workspace | Missing permission on the underlying **cloud connection** object | Connection-level permissions, not workspace role |
| User in a OneLake security role still gets "no permission on the artifact" | Nested/effective group membership isn't resolved across the producer→consumer boundary; the exact Object ID must be granted Fabric Read directly at the consumer | Grant the specific user or group (not an individual group member) Fabric Read directly on the consumer item |

---

## Decision rules — pick the right thing

| Scenario / requirement | Choose | Why |
| :--- | :--- | :--- |
| Activity output shows `failureType: SystemError` | Rerun / retry first | System errors are transient platform issues, not authoring mistakes — the cheapest fix usually works |
| Activity output shows `failureType: UserError` | Edit the pipeline/config | A `UserError` fails identically on every retry attempt |
| A pipeline failed partway and the sink is **non-idempotent** (append-only Copy, no dedup) | **Rerun from failed activity** | A full rerun would re-execute already-succeeded writes and duplicate data |
| An activity calls a flaky external system with occasional 5xx | Configure activity-level **retry** (count + interval) on the General tab | Automatic, fires in-run, no manual intervention |
| Downstream item needs to read a Dataflow Gen2's results reliably | Configure a **Lakehouse/Warehouse data destination** and read from OneLake | Sidesteps the Dataflows connector's internal-API timeout ("key didn't match any rows") |
| Dataflow gateway refresh fails on a referencing query only | Open **TCP 1433** on the gateway, or combine/de-stage the queries | Staged read-back uses TDS, not HTTPS |
| Refresh duration climbing, no error in run history | Investigate **query folding**, not error logs | Folding loss degrades performance without raising an error |
| A `DataFormat.Error` names a bad value | **Keep Rows → Keep Errors** on all columns | Purpose-built isolation of the malformed rows |
| Intermittent `DataSource.Error` | Enable **verbose diagnostics** in dataflow settings | Captures tracing for the specific failure window |
| Need mashup-engine detail for a support case | **Download detailed logs** (needs Admin consent for gateway diagnostics at tenant + gateway) | The standard artifact; retained 28 days |
| Spark job must not fail when capacity is busy | Submit via **pipeline / scheduler / Spark Job Definition** | Those queue FIFO for up to 24 hours; interactive and public-API notebook runs fail immediately |
| Spark capacity contention is chronic | Larger SKU, or **Autoscale Billing** to move Spark off shared capacity | Cancelling jobs and waiting is a stopgap |
| Exit code 137 with a few very long tasks | Fix **skew** (`spark.sql.adaptive.skewJoin.enabled`, salting, repartition) | More executor memory doesn't shrink an oversized partition |
| Exit code 137 right after `.collect()` / `.toPandas()` / unlimited `display()` | Fix the **driver-side** pull (add `.limit(N)`, avoid collecting) | This is driver OOM, not executor OOM |
| Setting `spark.executor.*` / `spark.driver.*` / network / YARN config | `%%configure` as the **first cell** | Those are read once at session launch; `spark.conf.set()` can't change them |
| Setting `spark.sql.*` (AQE, shuffle partitions, broadcast threshold) | `spark.conf.set()` mid-session | Runtime-tunable |
| Two jobs `MERGE` into the same Warehouse table concurrently | Add **retry with exponential backoff** | Snapshot isolation is the only level; 24556/24706 are expected, not a bug |
| Loading CSV/JSONL from a less-trusted external source | `ERRORFILE` + `MAXERRORS` | Turns a hard failure into a partial, diagnosable load |
| Loading **Parquet** with possible bad rows | Pre-clean the source, or convert to CSV/JSONL | `ERRORFILE`/`MAXERRORS` are ignored for Parquet data-type conversion errors |
| Warehouse `SELECT` completes on the backend but the client never gets results | Retry from a different client, or `CTAS` into a table | Isolates a client-delivery problem from a query failure |
| Ingestion failure with `FailureKind = Permanent` | Fix the data/config; do **not** retry | Retrying a bad format, missing entity or auth gap changes nothing |
| Ingestion failure with `FailureKind = Transient` | Retry (or reduce rate/batch size for `ThrottledOnEngine`) | The same request will likely succeed once the condition clears |
| `General_RetryAttemptsExceeded` | Investigate the underlying transient cause directly | The engine already exhausted its own retries |
| Derived table must never exist without its source data (and vice versa) | **Transactional** update policy | Failure rolls back the source ingestion too |
| Derived/summary table where raw ingestion must keep landing | **Non-transactional** update policy (the default, safer for most) | Failure is isolated to the target write |
| Need ingestion failure history beyond 14 days or across all tables | **Workspace monitoring → Ingestion results logs** | `.show ingestion failures` is capped at 14 days, command-level only |
| "Is my eventstream receiving anything / how fast?" | **Data insights** | Throughput and status metrics per node |
| "Why did the engine drop or transform that event?" | **Runtime logs** (warning/error/information) | Engine-level detail |
| Eventhouse must not miss events across capacity pauses/scale-downs | **Eventhouse Always-On** or a minimum consumption unit | Prevents pause-triggered ingestion gaps |
| `AADSTS65002` on an Event Hubs source | Escalate to a **tenant admin** / Microsoft support | Tenant-level preauthorization; not end-user fixable — reconfiguring the connection is wasted effort |
| Shortcut to an external system (S3, ADLS, GCS, Blob, Dataverse, OneDrive/SharePoint) | **Delegated** (the only option) | End users never need direct access to the external system |
| OneLake→OneLake shortcut where the source must keep full control of access | **Passthrough** (the default) | The calling user's identity is evaluated at the target |
| OneLake RLS/CLS/OLS must be enforced consistently across engines | SQL analytics endpoint in **user identity mode** | Delegated mode blocks RLS/CLS/OLS sources outright and doesn't honor OneLake roles |
| Revocation must take effect faster than 30–60 minutes | Capacity **pause/resume**, or toggle identity mode and back | Both invalidate cached tokens — and both drop existing SQL connections |
| Shortcut chain approaching 5 hops | Shortcut **directly to the original source** | 5 direct shortcut-to-shortcut links is a hard cap |
| Cross-cloud shortcut to ADLS Gen2 where `.dfs`-specific features aren't needed | Use the **`.blob` endpoint** | Currently yields the best performance |

## Numbers, limits and defaults to memorise

| Thing | Value | Note |
| :--- | :--- | :--- |
| Databricks access token default validity | **90 days** | Expiry → pipeline error code **3200** ("Error 403… token has expired"); **3208** = interrupted network call to Databricks, usually resolves on retry |
| Throttling HTTP status codes | **429** or **430** | Plus error code **2003** (subscription-level concurrency) — all three are capacity, not auth or config |
| Dataflow Gen2 staging read-back port | **TCP 1433** (TDS) | Writing uses HTTPS **443** — the asymmetry explains "first query succeeds, referencing query fails" |
| Dataflow staging-artifact permission failure trigger | Creator of the workspace's **first** dataflow hasn't signed in for **90+ days** | Or has left the organization → support ticket |
| Pipeline Web activity output size limit | **~4 MB** | Error code 2001/2002 |
| Pipeline combined activity/data/connection payload limit | **896 KB** | Error code 2001/2002 |
| Pipeline error codes — property/type problems | **2103 / 2104 / 2105** | Missing required property, wrong property type, malformed JSON (connector-agnostic); 2105 also = dynamic-content type mismatch |
| Pipeline error code — managed-identity token | **2403** ("Get access token from MSI failed") | Wrong or unreachable resource URL for the MSI token request |
| Pipeline error codes — Azure Batch | **2502** (can't access user storage account) · **2507** (folder path missing or empty) | Bad storage account name/key; no executables at `folderPath` |
| Pipeline error codes — Azure Functions | **3603** (response not a valid JObject) · **3608** (function returned an error status) | Function activities only support JSON responses |
| Pipeline error codes — Azure Machine Learning | **4121** (expired credential) · **4124** (published pipeline endpoint doesn't exist) | — |
| Pipeline error-code families ("zip codes") | **3200s** Databricks · **3600s** Functions · **4100s** Azure ML | Recognising the family narrows the fix to that connector's documented pairs |
| Dataflow Gen2 refreshes per 24-hour rolling window | **300** (CI/CD) / **150** (non-CI/CD) | Burst protection can throttle sooner |
| Dataflow Gen2 burst-throttle window | Many refresh requests within **60 seconds** | Triggers throttling even under the daily cap |
| Dataflow Gen2 single query evaluation cap | **8 hours** | — |
| Dataflow Gen2 total refresh time cap | **24 hours** | — |
| Dataflow Gen2 staged/output-destination queries per dataflow | **50 max** | — |
| Auto-pause of a dataflow refresh schedule | **72 h with 100% failure and ≥6 attempts**, or **168 h with 100% failure and ≥5 attempts** | Emails the dataflow owner; schedule must be manually re-enabled |
| Dataflow Gen2 detailed-log retention | **28 days** | Available a few minutes after refresh completes |
| Capacity units → Spark VCores | **1 CU = 2 Spark VCores** | — |
| Spark bursting | Up to **3×** purchased VCores when capacity is idle | — |
| `TooManyRequestsForCapacity` HTTP status | **430** | — |
| Spark job queue expiration | **24 hours** from submission | FIFO; then dropped and must be resubmitted |
| Spark queueing eligibility | Pipelines, scheduler, Spark Job Definitions **only** | Interactive notebook runs and public-API notebook jobs fail immediately |
| Spark workspace role needed | **Contributor+**, plus item-level `ReadData`/`ReadAll` | Spark specifically needs **ReadAll**, not just `ReadData` |
| `spark.rpc.message.maxSize` | Plain integer, capped at **2047 MB** | Bad value → exit code 13, `Spark_System_YARNApplication_KilledByTrustedServiceUser` |
| Spark exit codes | **137** OOM/SIGKILL · **143** SIGTERM · **134** SIGABRT · **1** user code · **-100** preempted · **13** invalid config kill | Read from Spark UI Executors tab |
| Fabric `spark.executor.memoryOverhead` default | Fixed **384 MB** regardless of node size | Open-source Spark uses `max(384 MB, 10% of executor memory)` |
| Data-skew signature | A handful of tasks with **10–100×** more input than the rest | Confirm in Spark UI Stages tab sorted by input size |
| Target partition size | **128–256 MB** per partition | — |
| Datetime rebase cutover | **Spark 3.0+** switched to the Proleptic Gregorian calendar; historical dates **pre-1582** are the risk zone | `INCONSISTENT_BEHAVIOR_CROSS_VERSION`; set `spark.sql.parquet.datetimeRebaseModeInRead/Write` to `CORRECTED`, but validate on a sample — `CORRECTED` over `LEGACY`-written data can silently shift values |
| Warehouse write-write conflict errors | **24556** and **24706** | Evaluated at the **table** level, at commit time; fix = retry with backoff |
| Warehouse isolation levels available | **Snapshot only** | `SET TRANSACTION ISOLATION LEVEL` silently ignored |
| `COPY INTO` `MAXERRORS` default | **0** | Any rejected row fails the load unless raised |
| `COPY INTO` `ERRORFILE` applicability | **CSV and JSONL only** | Parquet data-type conversion errors always fail the whole statement, ignoring `MAXERRORS` |
| `COPY INTO` rejected-row output | **`error.Json`** (reasons) + **`row.csv`** (rows) | Under `_rejectedrows` / `YearMonthDay-HourMinuteSecond` / statement-ID folders |
| `ERRORFILE_CREDENTIAL` supported auth | **Shared Access Signature (SAS) only** | Required when `ERRORFILE` targets a different storage account; firewall-protected accounts also require `MAXERRORS` |
| `.show ingestion failures` retention | **14 days** | Management-command failures only |
| Eventstream Runtime log severities | **3** — warning, error, information | — |
| Eventstream Event Hubs preauthorization error | **AADSTS65002** | Tenant-level preauthorization gap — not end-user fixable |
| Delegated shortcut cached storage token window | **30–60 minutes** | Explains "revoked but still readable" |
| OneLake security sync lag | Up to **5 minutes** | A broken policy reference can stall sync indefinitely |
| Cached query results after an identity-mode switch | Up to **1 hour** | Based on the previous security state |
| SQL endpoint "single-user mode" after a target rename/move | Up to **5 minutes** of blocked queries | While the system validates the new target |
| Transitive shortcut chain limit | **5** direct shortcut-to-shortcut links | Hard cap |
| Shortcuts per single OneLake path | **10 max** | Hard cap |
| Shortcut file cache retention | Configurable **1–28 days**, reset on every access | Per-workspace cache |
| Shortcut file cache size exclusion | Files over **1 GB aren't cached** | — |
| Shortcut caching supported sources | **GCS, S3, S3-compatible, on-premises data gateway** | — |
| Recommended transitive-shortcut depth | **2–3 hops** in practice | Even though the platform allows 5 |

## Traps and common mistakes

**§1 Pipeline and Dataflow Gen2**
- Reading `errorCode` before `failureType` — `failureType` tells you whether a rerun helps at all, before you spend time investigating.
- Treating a gateway "network-related error" as generic connectivity: writes use HTTPS 443, staged read-back uses **TDS on port 1433**. A first query can succeed while a referencing query fails on the same OneLake instance. Many corporate proxies don't support TDS at all.
- Searching refresh history for a query-folding error — folding loss produces **no error at all**, only slower refreshes.
- Treating "key didn't match any rows" from the Dataflows connector as missing data — it's an internal API timeout.
- Blindly retrying a `UserError` — it fails identically every attempt.
- Full-rerunning a pipeline whose sink is non-idempotent — duplicates the already-written rows.
- Not enabling **Admin consent for gateway diagnostics** until you need the logs during an incident.
- Assuming a paused refresh schedule restarts itself — the 72h/168h auto-pause requires manual re-enablement.

**§2 Notebook and T-SQL**
- Reflexively raising `spark.executor.memory` for every exit code 137 — if the Stages tab shows a few oversized tasks, it's **skew**, and the oversized partition still lands on one executor.
- Confusing driver OOM (`.collect()`, `.toPandas()`, unlimited `display()`) with executor OOM (skew, too few partitions, over-caching, UDFs) — the fixes are not interchangeable.
- Trying to set `spark.executor.*`/`spark.driver.*`/network/YARN config with `spark.conf.set()` mid-session — those are read once at session launch and need `%%configure` as the first cell.
- Assuming interactive notebook runs queue when capacity is full — they fail immediately with HTTP 430; only pipeline/scheduler/SJD submissions queue.
- Expecting logs to exist for a job that never left the queue or whose cluster creation failed — check capacity utilization instead.
- Writing `SET TRANSACTION ISOLATION LEVEL` in Fabric Warehouse — it runs without error and **has no effect**.
- Reporting error 24556/24706 as a bug — table-level snapshot conflicts are expected under concurrent writes; the only fix is retry with backoff.
- Relying on trial and error for unsupported T-SQL — attempting unsupported commands can appear to succeed while corrupting warehouse state.
- Setting a generous `MAXERRORS` on a **Parquet** `COPY INTO` and expecting partial success — Parquet data-type conversion errors ignore it entirely.
- Applying `CORRECTED` datetime rebase mode broadly after a runtime upgrade — on `LEGACY`-written historical (pre-1582) data it can **silently shift values**; validate on a sample first.
- Reading a "path does not exist" literally — it sometimes really means access denied.

**§3 Eventhouse and Eventstream**
- Auto-retrying every ingestion failure — `Permanent` failures never succeed on retry, and `General_RetryAttemptsExceeded` means the engine already gave up.
- Mistaking queued-ingestion **batching latency** for a silent failure — check the batching policy threshold before escalating.
- Expecting a non-transactional update-policy failure to stop source ingestion — it doesn't; the source keeps landing and only the derived table goes stale.
- Repeatedly reconfiguring an Eventstream connection against **AADSTS65002** — it's a tenant-level preauthorization gap that end users cannot self-resolve.
- Saving an Eventstream processor without checking the live sample view — schema drift and type mismatches make conditions **silently stop matching** rather than error.
- Expecting `.show ingestion failures` to cover every stage or more than 14 days — it's command-level and capped; pair with metrics/diagnostic logs or workspace monitoring.

**§4 OneLake shortcuts**
- Assuming Direct Lake and T-SQL always pass the caller's identity through a shortcut — **Direct Lake over SQL** and **T-SQL in delegated identity mode** both use the **item owner's identity**.
- Escalating "I revoked access but they can still read it" as a security breach — that's the documented **30–60 minute** delegated token cache.
- Debugging "delegated shortcut query returns nothing" as a connection problem — a source with OneLake RLS/CLS/OLS **blocks delegated shortcuts entirely, by design**.
- Assuming Warehouse SQL-level RLS/CLS protects shortcut access — those constructs apply only inside the Warehouse's own TDS execution context.
- Granting a user membership in a group and expecting a OneLake security role to resolve it across the producer→consumer boundary — **nested/effective group membership is not resolved**; the exact Object ID needs Fabric Read directly at the consumer.
- Applying both a restrictive (RLS) and a permissive role to the same principal — **most permissive wins**, silently defeating RLS.
- Assuming a Fabric connection-level credential is enough for ADLS Gen2 — the delegated identity also needs **Storage Blob Data Reader/Contributor** in Azure IAM on the storage account.
- Forgetting that capacity pause/resume and identity-mode toggles **drop existing SQL connections**, and that inline metadata objects (TVFs, scalar functions) and SQL roles are **dropped on a mode switch** — script them out first.
- Extending a shortcut chain past **5** hops, or expecting deleting a shortcut to delete target data (there are no cascading deletes).

## Exam tips

**Pipeline / Dataflow Gen2**
- Activity Output JSON fields: **`errorCode`, `message`, `failureType`, `target`** — `failureType` is `UserError` or `SystemError`.
- `LSROBOTokenFailure` = stored refresh token invalidated (Conditional Access, password change, device removal) → fix by **updating and saving the pipeline**.
- Gateway + Dataflow Gen2 staging read-back needs **TCP 1433**; writing uses HTTPS 443.
- Dataflow Gen2 refresh limits: **300/24h** (CI/CD), **150/24h** (non-CI/CD), **8h** per query evaluation, **24h** total refresh, **50** staged/destination queries max.
- Three Power Query error families: **`DataFormat.Error`** (bad data shape), **`DataSource.Error`** (connectivity), **`Expression.Error`** (bad M logic).
- **Rerun from failed activity** skips already-succeeded work; built-in **retry** (count/interval, General tab) is a separate, automatic per-activity setting.
- Numeric error codes are zip-coded by connector: 3200s Databricks, 3600s Functions, 4100s Azure ML; 2003 = concurrency throttling; 2001/2002 = size limits; 2103/2104/2105 = property/type problems; 2403 = MSI token; 2502/2507 = Azure Batch.
- Throttling surfaces as **HTTP 429 or 430**, or error code **2003** — all capacity, never a credential or config fault.

**Notebook / Spark**
- Exit codes: **137** OOM/SIGKILL · **143** SIGTERM (often benign scale-down) · **134** SIGABRT · **1** user code · **-100** preempted.
- `TooManyRequestsForCapacity` = **HTTP 430**; queued jobs expire after **24 hours**; interactive notebook runs don't queue at all.
- 1 CU = 2 Spark VCores; bursting up to 3×; `memoryOverhead` fixed at **384 MB** in Fabric.
- Spark needs **ReadAll**, not just `ReadData`, plus Contributor+ on the workspace.
- `%%configure` (first cell, restarts session) for `spark.executor.*`/`spark.driver.*`/network/YARN; `spark.conf.set()` for `spark.sql.*` only.

**T-SQL Warehouse**
- Snapshot isolation **only** — `SET TRANSACTION ISOLATION LEVEL` is silently ignored.
- Write-write conflict errors **24556** and **24706** — both fixed by retry logic, evaluated at the **table** level.
- `COPY INTO` `ERRORFILE` applies to **CSV and JSONL only**; output = **`error.Json`** + **`row.csv`** under a statement-ID folder; default `MAXERRORS` = **0**.
- Unsupported T-SQL to memorise: triggers, synonyms, materialized views, `SET ROWCOUNT`, `SET TRANSACTION ISOLATION LEVEL`, recursive queries, `BULK LOAD`, `CREATE USER`, `PREDICT`, `SELECT…FOR XML`, `sp_showspaceused`, vector data type/search.
- Locks: `SELECT` = `Sch-S`, DML/`COPY INTO` = `IX`, DDL = `Sch-M`.

**Eventhouse / Eventstream**
- `.show ingestion failures`: **`FailureKind`** is `Permanent` or `Transient`; **14-day** retention; command-level only — pair with metrics/diagnostic logs.
- Error category families: **BadFormat, BadRequest, DataAccessNotAuthorized, DownloadFailed, EntityNotFound, FileTooLarge, InternalServiceError, UpdatePolicyFailure, ThrottledOnEngine** (plus `RetryAttemptsExceeded`).
- `General_RetryAttemptsExceeded` = the platform already retried and gave up; don't keep retrying at the application layer.
- Transactional update policy = source **and** target roll back together; non-transactional (default) = source commits, only the target write is skipped.
- **AADSTS65002** = Eventstream not preauthorized to an Event Hub at the tenant level — not end-user fixable.
- Eventstream monitoring split: **Data insights** = throughput/status; **Runtime logs** = engine-level detail at **warning/error/information**.

**OneLake shortcuts**
- **Passthrough** = calling user's identity to the target (OneLake→OneLake default); **delegated** = configured connection identity, required for all external shortcuts.
- **401** = no valid auth; **403** = authenticated but not authorized; **404** = target moved/renamed/deleted.
- Cache windows: **30–60 min** delegated storage tokens; **up to 5 min** OneLake security sync; **up to 1 hour** cached query results across a mode switch.
- Transitive shortcuts: max **5** direct shortcut-to-shortcut links; max **10** shortcuts per single OneLake path.
- **Delegated mode blocks shortcuts to sources with RLS/CLS/OLS** in OneLake security — by design.
- Direct Lake over SQL / T-SQL delegated mode = **item owner's identity**, not the caller's — the classic Spark-vs-SQL row-count mismatch.

## Key takeaways

- Every surface fails differently, but the diagnostic pattern repeats: find the exact code, classify it (permanent/transient, user/system, auth/data), then apply the documented fix — don't guess from the symptom.
- Retry is a valid fix for exactly one class of error: transient/infrastructure. Permanent failures (bad format, missing permission, schema mismatch) waste capacity on retry.
- A pipeline activity's Output JSON is the fastest first surface; `failureType` triages rerun-worthiness before you dig further.
- Most "network error" gateway Dataflow Gen2 failures trace to one missing port — **TCP 1433** for the TDS staging read-back.
- Throttling and capacity errors (2003, `CapacityLimitExceeded`, HTTP **429/430**, the 300-refreshes/24h cap) are capacity problems solved in the Capacity Metrics app, not pipeline or code bugs.
- Query folding failures degrade performance without producing an error — don't search refresh history for a message that won't be there.
- Spark OOM diagnosis starts with the **exit code**, then branches to driver-side (collect/toPandas) or executor-side (skew, partitioning, caching) fixes — they are not interchangeable.
- Fabric's Spark admission model is core-based: FIFO queueing with 24h expiry for scheduled/pipeline jobs, immediate failure for interactive sessions when capacity is exhausted.
- Fabric Warehouse's single isolation level (snapshot) makes write-write conflicts (24556/24706) an expected, retry-driven outcome, not a defect — conflicts are evaluated at the **table** level. `AnalysisException`, by contrast, fails fast before compute is spent and names the exact table/column/type at fault.
- `COPY INTO`'s `ERRORFILE` + `MAXERRORS` turns a hard load failure into a partial, diagnosable success — but only for CSV/JSONL, never Parquet.
- `FailureKind` is the single most important field in `.show ingestion failures` — it decides whether retry is even worth attempting.
- Update-policy transactionality determines blast radius: transactional can block otherwise-healthy raw ingestion; non-transactional isolates the failure to the derived table.
- Batching latency in queued ingestion is expected behaviour, not a failure state — check the policy threshold first.
- Most shortcut "connectivity" failures are identity/permission problems: check auth mode and cached-token timing (30–60 min tokens, 5 min sync, 1 hour query results) before chasing network causes.
- OneLake security enforcement diverges by engine and mode — Spark always enforces it, delegated-mode SQL never does, user-identity-mode SQL does; and delegated mode blocks RLS/CLS/OLS sources outright.
- Transitive-shortcut (5 hops) and per-path shortcut (10) limits are hard caps worth memorising verbatim.

---

## Scenario Questions

> Attempt all of them before opening any toggle. Answers are hidden until you click.

### Q1. The pipeline that broke itself overnight

Northwind Logistics runs a nightly Data Factory pipeline that has copied shipment data from an on-premises SQL Server into a Lakehouse for eight months without modification. Last Tuesday the security team enforced a new Conditional Access policy and forced a tenant-wide password reset. Since Wednesday every run of the pipeline fails on the first Copy activity. The Output JSON shows `"failureType": "UserError"` and a message containing `LSROBOTokenFailure` and the text "provided grant has expired". Nobody has edited the pipeline.

**What is the correct resolution?**

- **A.** Increase the activity's retry count and interval on the General tab so the token has time to refresh
- **B.** Open the affected pipeline in the Fabric portal, then update and save it to refresh its auth context
- **C.** Scale the capacity up to the next SKU and re-run from the failed activity
- **D.** Open outbound TCP port 1433 to `*.datawarehouse.fabric.microsoft.com` on the on-premises gateway

<details>
<summary>👉 Show answer</summary>

**Answer: B**

**Why it is right:** `LSROBOTokenFailure` means the pipeline's stored refresh token can no longer obtain a new access token — triggered by exactly these events (Conditional Access policy change, password change, device removed from the tenant). The documented fix is to update and save the affected pipeline, which refreshes its auth context; published PowerShell scripts do this in bulk for many pipelines.

**Why the others are wrong:**
- **A** — Retry never fixes a `UserError`; the token is invalid, so every attempt fails identically. Retry is for transient `SystemError` conditions.
- **C** — Nothing here points to capacity. A capacity problem would show `CapacityLimitExceeded`, error 2003, or HTTP 430, not a token grant failure.
- **D** — Port 1433 is the Dataflow Gen2 staging read-back requirement over TDS. This is a pipeline Copy activity with an auth error, not a gateway TDS problem.

**Covered in:** §1 Pipeline and Dataflow Gen2 Errors — Connection, authentication and gateway errors

</details>

### Q2. The referencing query that will not run

Meridian Retail's finance team built a Dataflow Gen2 that uses an on-premises data gateway. Query A reads from an on-prem SQL Server and loads to a Lakehouse — it refreshes successfully every night. Query B references Query A's staged output and applies currency conversion. Query B fails every run with "A network-related or instance-specific error occurred… TCP Provider, error: 0". Both queries target the same OneLake instance, and the gateway machine has outbound HTTPS open.

**What is the most likely root cause, and what are the documented options?**

- **A.** Query B lost query folding, so it timed out against the source; rewrite the currency-conversion step to fold
- **B.** The gateway's stored Entra credential expired between the two queries; re-authenticate the gateway connection
- **C.** The staging Lakehouse exceeded its storage quota mid-refresh; enable a data destination on Query A
- **D.** The gateway cannot reach the staging Lakehouse over TCP 1433 (TDS); open port 1433 to the documented staging endpoints, or combine the referencing queries / disable staging

<details>
<summary>👉 Show answer</summary>

**Answer: D**

**Why it is right:** Dataflow Gen2 **writes** to a Lakehouse over HTTPS (443) but **reads staged data back over TDS on port 1433**. That asymmetry is precisely why a standalone query succeeds while a query that references its staged output fails with a TCP-level error. The documented fixes are opening outbound TCP 1433 to `*.datawarehouse.pbidedicated.windows.net`, `*.datawarehouse.fabric.microsoft.com` and `*.dfs.fabric.microsoft.com`, or combining the referencing queries / disabling staging as a workaround — many corporate proxies pass generic HTTP/TCP but don't support TDS at all.

**Why the others are wrong:**
- **A** — Query folding loss degrades performance without raising an error; it never throws a TCP Provider exception.
- **B** — An expired gateway credential would fail both queries identically, not just the referencing one.
- **C** — A storage/capacity problem surfaces as a capacity error (`CapacityLimitExceeded`), not a TCP Provider network error.

**Covered in:** §1 Pipeline and Dataflow Gen2 Errors — Connection, authentication and gateway errors

</details>

### Q3. Two tasks that never finish

Contoso Manufacturing runs a nightly PySpark notebook joining a 400 GB sensor fact table to a device dimension. All Spark configuration is at defaults. In the Spark UI, the Stages tab shows 1,190 of 1,200 tasks finishing in under 60 seconds, while 2 tasks run for 40+ minutes and then die. The Executors tab reports **exit code 137** for those containers.

**Which action will NOT resolve the failure?**

- **A.** Enable `spark.sql.adaptive.skewJoin.enabled` so AQE splits the oversized partitions
- **B.** Salt the join key so the heavy key's rows spread across many partitions
- **C.** Double `spark.executor.memory` again and rerun the notebook unchanged
- **D.** Repartition the input toward 128–256 MB per partition before the join

<details>
<summary>👉 Show answer</summary>

**Answer: C**

**Why it is right (i.e. why C is the action that fails):** A small subset of very long-running, eventually OOM-killed tasks against many fast tasks is the textbook **data-skew** signature — a handful of partitions carrying **10–100×** more input than the rest, confirmed in the Stages tab sorted by input size. Exit code **137** is SIGKILL/OOM at the container level. Raising `spark.executor.memory` does **not** fix skew: the oversized partition still lands entirely on one executor, so the same task dies at a higher memory ceiling. Note this is **executor** OOM, not driver OOM — driver OOM comes from `.collect()`, `.toPandas()` or an unlimited `display()`, and the two fixes are not interchangeable.

**Why the other options do work:**
- **A** — AQE skew-join handling is the documented first fix; AQE is **on by default in Fabric**, and enabling skew-join splits the oversized partitions across tasks.
- **B** — Manual salting is the documented alternative to AQE skew handling: it redistributes the heavy key so no single partition dominates.
- **D** — Repartitioning toward the documented **128–256 MB per partition** target directly addresses oversized/too-few partitions, which is the other executor-OOM cause alongside skew.

**Covered in:** §2 Notebook and T-SQL Errors — Out-of-memory: driver vs executor

</details>

### Q4. The load that fails anyway

Fabrikam Energy loads a 2 GB **Parquet** file into a Fabric Warehouse table each night with `COPY INTO`. The statement specifies `MAXERRORS = 100` and a configured `ERRORFILE` pointing at a folder in the same storage account as the source. Exactly 12 rows in the file have a genuine data-type mismatch — far below the tolerated error count. The load fails outright and loads zero rows.

**Which explanation is correct?**

- **A.** `MAXERRORS` must be set at the session level with `SET MAXERRORS = 100` before the statement runs
- **B.** `ERRORFILE` requires `ERRORFILE_CREDENTIAL` in every case, and its absence aborts the load
- **C.** `ERRORFILE`/`MAXERRORS` apply only to CSV and JSONL in Fabric Data Warehouse — Parquet data-type conversion errors always fail the whole `COPY INTO`, ignoring `MAXERRORS`
- **D.** Snapshot isolation rolled the `COPY INTO` back because another transaction touched the same table

<details>
<summary>👉 Show answer</summary>

**Answer: C**

**Why it is right:** In Fabric Data Warehouse, `ERRORFILE` (and therefore rejected-row tolerance via `MAXERRORS`) is honored for **CSV and JSONL only**. Parquet data-type conversion errors always fail the entire statement regardless of `MAXERRORS` — which is exactly why a generous `MAXERRORS = 100` did not save this load. The fix is to pre-clean/validate the Parquet source, or land the data as CSV/JSONL where the options are honored.

**Why the others are wrong:**
- **A** — `MAXERRORS` is a statement-level `WITH` option, not a session setting, and it was already set correctly.
- **B** — `ERRORFILE_CREDENTIAL` (SAS only) is needed only when `ERRORFILE` points at a **different** storage account than the source; here they share one account, so the source `CREDENTIAL` applies.
- **D** — A snapshot-isolation write-write conflict raises error **24556** or **24706**, a distinct failure mode from a data-type-driven rejection.

**Covered in:** §2 Notebook and T-SQL Errors — `COPY INTO` rejected-row diagnostics

</details>

### Q5. The summary table that quietly stopped

Helios Telemetry ingests device events into an Eventhouse table `raw_events`. A **non-transactional** update policy runs a KQL projection into a derived table `hourly_summary`. After an upstream firmware release changed the payload, `.show ingestion failures` starts returning rows with `ErrorCode = UpdatePolicy_QuerySchemaDoesNotMatchTableSchema` and `OriginatesFromUpdatePolicy = true`. Analysts report that dashboards over `hourly_summary` have been frozen for six hours, but raw event counts look normal.

**What is happening to data arriving at `raw_events` while this failure persists?**

- **A.** Ingestion into `raw_events` also fails, because the update policy runs inside the same transaction as the source ingest
- **B.** Ingestion pauses automatically until an administrator acknowledges the schema mismatch
- **C.** Both tables keep committing, with `hourly_summary` reusing the last successful projection result
- **D.** Ingestion into `raw_events` continues to succeed; only `hourly_summary` stops receiving new rows, and the failure is logged as Permanent

<details>
<summary>👉 Show answer</summary>

**Answer: D**

**Why it is right:** A **non-transactional** update policy (the default) decouples the source table's ingestion from the update-policy query's success — `raw_events` keeps ingesting normally, only the derived write is skipped, and the failure is logged. `UpdatePolicy_QuerySchemaDoesNotMatchTableSchema` is classified **Permanent**: the projection's output columns no longer match the target schema, so retrying changes nothing — fix the query or the target schema.

**Why the others are wrong:**
- **A** — That describes a **transactional** (`IsTransactional = true`) policy, which rolls back the entire source ingestion.
- **B** — There is no automatic pause-and-acknowledge workflow for update-policy failures.
- **C** — A non-transactional failure simply skips the write; it never substitutes stale cached results.

**Covered in:** §3 Real-Time Errors — Update policy failure behaviour

</details>

### Q6. Reading `.show ingestion failures` (Choose 2)

Kestrel Analytics runs `.show ingestion failures` on an Eventhouse database and gets a mix of rows: some with `ErrorCode = General_ThrottledIngestion`, some with `Stream_WrongNumberOfFields`, and some with `General_RetryAttemptsExceeded`. The team wants to build an automated remediation job and also a 90-day trend dashboard covering every table in the workspace.

**Which two statements are correct? (Choose 2)**

- **A.** `Stream_WrongNumberOfFields` is Transient, so the remediation job should simply re-submit the ingestion
- **B.** `General_ThrottledIngestion` is Transient — retrying, or reducing ingestion rate/batch size, is the documented response
- **C.** `General_RetryAttemptsExceeded` is Transient and should be looped until it succeeds
- **D.** `.show ingestion failures` can supply the 90-day dashboard directly if it is scheduled to run daily
- **E.** The 90-day dashboard needs workspace monitoring and the Ingestion results logs table family, because `.show ingestion failures` retains only 14 days and covers management-command failures only

<details>
<summary>👉 Show answer</summary>

**Answer: B and E**

**Why they are right:** `General_ThrottledIngestion` sits in the **ThrottledOnEngine** category with `FailureKind = Transient` — retry, or reduce ingestion rate/batch size (**B**). `.show ingestion failures` has a hard **14-day retention window** and covers only ingestion management-command failures, so a durable, cross-table, 90-day history requires enabling workspace monitoring and querying the **Ingestion results logs** table family in the auto-provisioned monitoring Eventhouse (**E**).

**Why the others are wrong:**
- **A** — `Stream_WrongNumberOfFields` is a **BadFormat** category error with `FailureKind = Permanent`; the source file's structure or the ingestion mapping must be fixed, and re-submitting achieves nothing.
- **C** — `General_RetryAttemptsExceeded` is **Permanent**: Kusto already retried past a recurring transient error and gave up. Looping from the application layer just repeats a failure the engine has exhausted. Investigate the underlying transient cause instead.
- **D** — Scheduling the command more often does not extend its 14-day retention, and it still misses stages the command never covered.

**Covered in:** §3 Real-Time Errors — Ingestion error code categories; Diagnosing via workspace monitoring KQL

</details>

### Q7. Two engines, two answers

Arcadia Health stores patient encounter data in a producer workspace lakehouse with **row-level security defined in OneLake security**. A consumer workspace has a shortcut to that table. A data engineer runs the identical aggregate query twice: from a Spark notebook it returns 412,000 rows (correctly filtered to her region); through the consumer's SQL analytics endpoint it returns 3.1 million rows. The endpoint's Security tab shows **Delegated identity mode**. Nothing changed at the source between the two runs.

**What explains the discrepancy, and what is the fix?**

- **A.** Delegated identity mode uses the item owner's identity and does not honor OneLake security roles, so RLS isn't applied through SQL; switch the endpoint to user identity mode, or replicate the filtering with SQL RLS/CLS/DDM
- **B.** The SQL analytics endpoint is serving a stale result cache; wait one hour for it to expire
- **C.** Spark and SQL analytics endpoints always return different counts against shortcut-backed tables by design; use only one engine
- **D.** The shortcut target was renamed between the two queries, putting the endpoint into single-user mode for 5 minutes

<details>
<summary>👉 Show answer</summary>

**Answer: A**

**Why it is right:** Spark **always** enforces OneLake security. A SQL analytics endpoint in **delegated identity mode** does not — it reads as the item owner and does not evaluate the caller against OneLake RLS rules, producing a broader, unfiltered result set. Switching the endpoint to **user identity mode** aligns the engines; alternatively, replicate the filtering with SQL-layer RLS/CLS/dynamic data masking at the consumer.

**Why the others are wrong:**
- **B** — The up-to-1-hour cached-result window applies after switching between identity modes and would not systematically return 7× more rows.
- **C** — The divergence is specific to delegated mode, not universal; in user identity mode both engines agree.
- **D** — A renamed/moved target produces a **404**/path-not-found (and up to 5 minutes of blocked queries), not a quietly larger row count.

**Covered in:** §4 OneLake Shortcut Errors — OneLake security interplay; Same root cause, different error per engine

</details>

### Q8. Revocation that has not landed yet

At Vantage Bank, a compliance officer revokes the item owner's OneLake Read permission on a producer lakehouse at 09:00. A consumer workspace reads that lakehouse through a **delegated** shortcut via its SQL analytics endpoint. At 09:20 an analyst in the consumer workspace can still query the data. Compliance demands the access stop within minutes and asks what to do, in what order.

**Which sequence is correct?**

- **A.** Recreate the shortcut → wait 24 hours for cross-workspace propagation → re-test the analyst's query
- **B.** Confirm this is the documented 30–60 minute delegated token cache → script out inline metadata objects (TVFs, scalar functions) and SQL roles, and accept that existing SQL connections will drop → force a refresh by pausing and resuming the capacity, or by toggling the endpoint between user identity and delegated mode → re-test
- **C.** Toggle the endpoint's identity mode immediately → wait for the up-to-1-hour cached-query-result window to expire → then revoke the permission again to make it stick
- **D.** Open a support ticket for a OneLake security breach → disable the shortcut's cloud connection → wait for the 5-minute OneLake security sync → re-test

<details>
<summary>👉 Show answer</summary>

**Answer: B**

**Why it is right:** Continued access after revocation in delegated mode is **documented, expected caching behaviour** — the consumer runs against a cached storage access token for the item owner, valid for **up to 30–60 minutes**. Confirming that first prevents a false security escalation. The two documented ways to force an earlier refresh are pausing and resuming the Fabric capacity (safest, clears backend caches) or toggling the endpoint between user identity and delegated mode. Both **drop existing SQL connections** in the affected workspace, and a mode switch **drops inline metadata objects (TVFs, scalar functions) and SQL roles** — so scripting those out first is part of the correct sequence.

**Why the others are wrong:**
- **A** — There is no 24-hour cross-workspace propagation window, and delegated shortcuts honor the change once the token expires without manual recreation.
- **C** — Sequencing is wrong and self-defeating: the up-to-1-hour cached-query-result window is a *consequence* of switching modes, not something to wait out before acting, and the revocation does not need to be reapplied.
- **D** — This is not a breach, so a ticket is the wrong first step; the 5-minute figure is the OneLake **security sync** lag, not the token cache, and disabling the connection is not the documented remedy.

**Covered in:** §4 OneLake Shortcut Errors — Delegated credential expiry: the classic failure

</details>
