---
title: Monitoring and Alerting — DP-700 Exam-Ready Notes
topic: 09
domain: Domain 3 — Monitor and optimize an analytics solution (30–35%)
source: certification/09-monitoring-alerting/
tags: [dp-700, exam-ready, monitoring, monitor-hub, spark-monitoring, capacity-metrics, semantic-model, direct-lake, framing, sempy, activator, alerts]
---

# 09. Monitoring and Alerting

> **Exam domain:** Domain 3 — Monitor and optimize an analytics solution (30–35%)
> **Source:** `certification/09-monitoring-alerting/` — 3 subtopic files + section index condensed
> **Why the exam cares:** This area covers two blueprint bullets in full — *monitor data ingestion and transformation* and *configure alerts*. Nearly every question is "given this symptom, which surface do you open first, and what do you configure to be told automatically next time." It rewards knowing which surface is scoped to what, and which sources/actions Activator does and does not support.

---

## Orientation — the 60-second version

Microsoft Fabric is a SaaS analytics platform where every workload (data pipelines, Spark notebooks, Power Query dataflows, streaming, warehouses, Power BI) runs on one shared purchased compute pool called a **capacity**. Because the workloads are separate item types, Fabric does not have one single monitoring screen — it has a **layered** monitoring story, and the exam tests whether you can pick the right layer.

Layer 1 is the **Monitor hub**, a tenant-wide "did my jobs run" board covering 17 item types with shallow retention (100 rows, 30 days). Layer 2 is **item-specific monitoring** — pipeline run history with Gantt charts and per-activity JSON, Dataflow Gen2 refresh history with downloadable engine logs, the Spark application detail page with driver/executor logs, Eventstream data insights, Eventhouse ingestion logs. Layer 3 is **workspace monitoring**, an opt-in feature that dumps workspace activity into a KQL-queryable Eventhouse so you can write queries across items and past Monitor hub's 30-day cap. Layer 4 is the **Capacity Metrics app**, an admin-only Power BI app answering "is the whole capacity throttling" rather than "did my job fail."

Sitting alongside those is **semantic model refresh** — its own diagnostic world, because Import models copy data while Direct Lake models only re-point metadata (**framing**), and a "successful" upstream job can still leave a Direct Lake report stale. Finally, **Activator** turns any of these signals into automated email/Teams/Power Automate/run-a-Fabric-item actions. Monitoring shows you; Activator tells you.

## New terms in this topic

| Term | What it actually is |
| :--- | :--- |
| **OneLake** | Fabric's single tenant-wide data lake — one storage account every workspace and item writes into, so items can read each other's data without copying. |
| **Capacity / SKU** | The purchased compute pool (F2, F64, …) that every Fabric item in a workspace runs on. All workloads compete for its Capacity Units (CU); overuse causes throttling. |
| **Monitor hub** | The tenant-wide activity board, opened via **Monitor** in the nav pane. Shows run status/timing/error summary across item types you have permission to see. Solves "did my jobs run and when." |
| **Workspace monitoring** | Opt-in per-workspace feature that provisions a monitoring **Eventhouse**/KQL database and writes workspace activity logs into it. Solves Monitor hub's shallow retention and lack of row-level detail. |
| **Eventhouse / KQL database** | Fabric's real-time analytics store, queried with KQL (Kusto Query Language). Here it doubles as the log sink for workspace monitoring. |
| **KQL queryset** | The saved-query item you write and run KQL in against a KQL database. Its results ribbon carries **Set Alert**, making it a first-class Activator source. |
| **Real-Time dashboard** | A Fabric dashboard built from KQL queries. Individual tiles can carry alerts, provided the tile is KQL-based, non-static, single time range and free of `make-series`. |
| **Eventstream** | Fabric's no-code streaming pipeline item — ingests events from a source, optionally transforms, routes to destinations. Has its own monitoring panes. |
| **Copy job** | A simplified Fabric ingestion item for copying source→destination data (full or incremental) without authoring a full pipeline. |
| **Dataflow Gen2** | Power Query-based low-code transformation item. "Refresh" is its run. Gen1 is the legacy Power BI dataflow — not tracked in Monitor hub. |
| **Spark job definition (SJD)** | A Fabric item that runs a packaged Spark application (JAR/Python file) on a schedule or trigger, as opposed to an interactive notebook. |
| **Spark Advisor** | Built-in real-time recommendation/error-analysis engine surfaced in the notebook and in the Spark application **Diagnostics** panel, pattern-matching against common failure causes. |
| **Livy** | The REST service Fabric uses to submit Spark sessions/jobs; its log stream is one of the three log families on the Spark application Logs tab. |
| **Capacity Metrics app** | An admin-installed Power BI app reporting CU utilization, throttling, storage and autoscale per capacity. Cannot fire alerts. |
| **Semantic model** | The Power BI data model (tables, relationships, measures) that reports query. Its "refresh" behaviour depends on storage mode. |
| **Import mode** | Semantic model storage mode that copies and caches the entire data volume in memory. |
| **Direct Lake** | Storage mode where the model reads Delta/Parquet files in OneLake directly, with no data copy — refresh is metadata-only. |
| **Framing / reframing** | The Direct Lake "refresh": reading the Delta transaction log and re-pointing the model at the newest Parquet files. Solves freshness without copying data. |
| **DirectQuery** | Storage mode that translates each query live against the source and never caches. Direct Lake on SQL endpoints can silently *fall back* to it. |
| **VertiPaq** | The in-memory columnar engine behind both Import and Direct Lake. Direct Lake loads columns on demand — that load is called **transcoding**. |
| **Enhanced refresh** | The asynchronous form of the Power BI Refresh Dataset REST API — table/partition-scoped, pollable, cancellable. |
| **`sempy` / semantic link** | Python package (`sempy.fabric`) in the Fabric Spark runtime that lets a notebook list, refresh and manage semantic models programmatically. |
| **Workspace lineage view** | A visual graph of a semantic model's upstream dependencies (lakehouse/warehouse tables, dataflows). Answers "what does this model actually depend on" before you blame the model for a refresh failure. |
| **Dynamic M parameters** | A Power BI feature that lets a report user's selection feed a value into the underlying M/Power Query. Activator explicitly cannot alert on a report using them. |
| **Activator** | Fabric's no-code event-detection engine: watches events, evaluates rules, fires actions (email, Teams, Power Automate, run a Fabric item). |
| **Real-Time hub** | The catalogue of streams and Fabric/Azure events (job events, workspace item events, OneLake events, capacity overview events) where alerts are set up. |
| **User Data Function (UDF)** | A Fabric item holding custom code you can invoke from other items. Activator can run one as an action — but a UDF-based Activator item breaks deployment pipelines / Git. |
| **Power Automate** | Microsoft's flow-automation service. It is Activator's escape hatch to external/third-party systems, since email and Teams actions are internal-only. |
| **Business event** *(preview)* | A published, named business-level event. Activator can both alert on one as a source and publish one as an action. |

## How the pieces fit

- **Monitor hub** = breadth (17 item types, all workspaces you can see), shallow depth (100 rows / 30 days).
- **Item-specific surfaces** = depth for one item type: pipeline run history, Dataflow Gen2 refresh history, Spark application detail, Eventstream Data insights/Runtime logs, Eventhouse ingestion logs.
- **Workspace monitoring** = the escape hatch past Monitor hub's cap: an auto-provisioned KQL database exposing `ItemJobEventLogs` (pipelines/jobs), `CopyJobActivityRunDetailsLogs` (per-mapping Copy job detail), and Eventhouse metrics/command/data-operation/ingestion/query logs — cross-item, KQL-queryable.
- **Capacity Metrics app** = orthogonal axis: capacity-wide CU and throttling for admins, never job success/failure, and it cannot alert.
- **Semantic model refresh** = its own branch: Import (copy) vs Direct Lake framing (metadata) vs DirectQuery (no refresh); refresh history + Direct Lake tab + workspace lineage + `sempy` are its monitoring surfaces.
- **Activator** = the output stage. Every surface above answers "what happened"; Activator answers "tell me the moment it happens." Alert authoring is decentralised (Set Alert buttons in Eventstream, KQL querysets, Real-Time dashboards, Power BI visuals, Real-Time hub), but every alert lands in an Activator item.

### The Domain 3 triage spine: symptom → surface → diagnosis → lever

| Symptom | Monitoring surface to open first | Likely diagnosis | Lever if slow-not-broken |
| :--- | :--- | :--- | :--- |
| Direct Lake dashboard suddenly slow after a large append | Semantic model refresh / framing | DirectQuery fallback check — confirm reframing actually ran and the table hasn't fallen back | V-Order + Direct Lake guardrails (topic 11) |
| Pipeline Copy activity duration creeping up over weeks, no failures | Pipeline run/activity monitoring | Degraded, not failed — query folding loss check (topic 10) | Partition option, parallel copies, staging (topic 11) |
| Spark notebook runs slow, then exits | Spark application monitoring | Executor OOM (exit code 137) (topic 10) | Executor sizing, AQE skew handling, broadcast joins (topic 11) |
| Eventhouse queries slow only on older data | Eventhouse ingestion monitoring | Not an ingestion failure — rule out via streaming/queued ingestion behaviour (topic 10) | Hot cache vs retention policy (topic 11) |
| Everything on the capacity feels slow, no single item fails | Capacity Metrics app | HTTP 430 queueing, not a code bug (topic 10) | Capacity scale (larger SKU / Autoscale Billing) + workload spread via high concurrency (topic 11) |

> 🧠 **Mental model —** Triage always flows monitor → diagnose → optimize. Every Domain 3 question is secretly asking where you are in that pipe: still figuring out *what's* happening (monitor), know it's broken and need the fix (diagnose), or know it isn't broken and just want it faster/cheaper (optimize). Reaching for a performance lever before confirming the symptom, or treating a slow-but-succeeding job as an error to "fix," is the recurring trap.

---

## 1. Monitoring Surfaces
*Source: `01-monitoring-surfaces.md`*

### 1.1 The Monitor hub — Fabric's central activity view

Selecting **Monitor** in the Fabric navigation pane opens the monitoring hub, tracking job execution health across every workspace you can access. **Any Fabric user can open it, but you only see activities for items you have permission to view.**

Two pages: **Activities** (main view) and **Schedule failures** *(preview)* — a centralised place to configure failure-notification recipients for scheduled items instead of setting them up one item at a time.

| Feature | What it gives you |
| :--- | :--- |
| Current run status | Track active and recently completed jobs from one place |
| Historical runs | Up to **100 activities from the past 30 days** in the main view; **More options → Historical runs** on any activity shows its full 30-day history |
| Activity details and diagnostics | Status, timing, error details via the details pane (point to activity → **View details**) |
| Filtering and search | Filter by **Status**, **Item type**, **Start time**, **Submitted by**, **Location** (workspace); keyword search is scoped to already-loaded rows only |
| Schedule failure notifications | Centralised recipient management for scheduled-item failure emails *(preview)* |

**Monitor hub's Activities page covers 17 item types:** Copy Job, Dataflow Gen2, Dataflow Gen2 CI/CD, Datamart, Data Build Tool (dbt) Job, Digital Twin Builder Flow, Experiment, Graph model, Lakehouse, Map, Notebook, Pipeline, Semantic model, Snowflake database, Spark job definition, and User data function.

> 🔑 **Exam fact —** **Dataflow Gen1 is not supported** and never appears in the Monitor hub table. This exclusion is a common distractor.

> ⚠️ **Trap —** Monitor hub is not a bottomless log. The Activities table caps at **100 rows across the last 30 days**, and even the per-item **Historical runs** drill-down only goes back **30 days**. For anything older, or for raw log-level detail beyond status/timing/error summary, use item-specific history (Dataflow Gen2 refresh history, Spark application logs) or workspace monitoring's KQL-queryable Eventhouse. Monitor hub is not a durable audit log.

### 1.2 Pipeline run and activity monitoring

A pipeline's own **View run history** flyout and **Go to monitor** detail page add pipeline-specific tooling on top of Monitor hub:

- **Gantt view** — each run renders as a bar grouped by pipeline name; bar length = duration. Spots overlaps, delays and anomalies across many runs at a glance.
- **Activity runs table** — filter by activity status, add/remove columns, search by activity name/type/run ID; **Load more** past the first **2,000 activity runs**.
- **Input/Output JSON** — select the **Input** or **Output** link next to an activity run to inspect the exact JSON payload the activity received or produced.
- **Performance details pane** — select an activity for **Duration breakdown** and **Advanced** stats (rows/bytes read and written, for Copy-type activities).
- **Export to CSV** — exports the currently filtered monitoring table.
- **Rerun** — choose **rerun the entire pipeline** or **rerun only from the failed activity**, skipping activities that already succeeded.

> 🧠 **Mental model —** A full pipeline rerun is starting the whole assembly line over; **rerun from failed activity** is resuming the line at the station that jammed — everything upstream that finished stays finished, only the broken station and everything downstream run again.

For log-level detail beyond the run history UI, enable **workspace monitoring**: **Workspace settings → Monitoring → add a monitoring Eventhouse → toggle Log workspace activity**. This writes pipeline-level events into an **`ItemJobEventLogs`** table inside an auto-created KQL database — queryable with KQL for success/failure trend analysis, performance metrics, and error-detail mining across every pipeline in the workspace at once.

### 1.3 Dataflow Gen2 refresh history and diagnostics

Two monitoring entry points: **Recent runs** (per-dataflow, from the workspace item's context menu) and the shared **Monitoring hub** dashboard.

**Refresh history (Recent runs)** shows at list level: start time, status, duration, refresh type. Retention:

- **Up to 50 refresh histories or 6 months of history (whichever limit hits first)** visible in the UI
- **Up to 250 refresh histories or 6 months** stored in OneLake underneath

Drilling into one run (select its **Start time**) surfaces:

- **General run info** — status, refresh type, start/end time, duration, Request ID, Session ID, Dataflow ID
- **Tables section** — every entity with loading enabled, each drillable to its own detail screen
- **Activities section** — every action taken during the refresh (e.g. loading to an output destination), each drillable to activity statistics: endpoints contacted, bytes/rows read and written

**Download detailed logs** (on a run's details screen) produces a zipped file of mashup-engine logs, available a few minutes after the refresh completes and for **up to 28 days** afterwards. Requires **at least workspace Viewer** permission; **gateway-based refreshes** additionally need **Admin consent for gateway diagnostics** enabled at **both tenant and gateway level** first. A lighter-weight **CSV export** of one or more selected runs is available directly from the run list.

At workspace level, the **Status** column on the workspace list view shows the last refresh's outcome **plus** the last save/validate result, with a red exclamation icon (hover for details) on failure of either.

The shared **Monitoring hub dashboard** (distinct from per-dataflow Recent runs) rolls up: dataflow status, refresh start time, refresh duration, submitter, workspace name, capacity used, average refresh duration, refreshes per day, and refresh type — useful for spotting a dataflow whose refresh duration is trending up before it becomes a hard failure.

> ⚠️ **Trap —** Do not confuse a Dataflow Gen2 **save/validate failure** with a **refresh failure**. The workspace Status column's red icon can mean either. A failed validation (something wrong in the Power Query logic itself, fixed by reopening the dataflow editor) is a different failure mode from a failed refresh (typically a data-source or capacity issue, diagnosed via refresh history). Hover the icon to see which occurred — "refresh history" is not the fix for every red icon.

### 1.4 Spark application monitoring

| Entry point | What it's for |
| :--- | :--- |
| **Monitor hub** | Centralised portal for Spark activity across every item in a workspace — in-progress applications from Notebooks, Spark Job Definitions and Pipelines, searchable/filterable |
| **Item Recent Runs** | Per-item (Notebook or SJD) view of current and recent activities: submitter, status, duration |
| **Notebook contextual monitoring** | Author, monitor and debug in one place — job progress, tasks/executors and Spark logs at notebook-cell level, plus the built-in **Spark Advisor** for real-time code/execution advice |
| **Spark job definition inline monitoring** | Real-time submission/run status plus past runs and configurations, on the SJD item |
| **Pipeline Spark activity inline monitoring** | Deep links from a pipeline's Notebook/SJD activity straight to Spark execution details, snapshot and logs — including inline error messages on failure |

Opening a specific application lands on the **Spark application detail page**, with these tabs:

- **Jobs** — every job run for the application: Job ID, description, status, stages, tasks, duration, data processed/read/written, and a code snippet per job; job description links straight into the underlying **Spark UI**
- **Resources** — near-real-time executor allocation/utilization graph
- **Logs** — full logs for **Livy**, **Prelaunch** and **Driver** processes, filterable by keyword/status/notebook, downloadable (logs may be unavailable if the job is still queued or cluster creation failed)
- **Data** — input/output file details: name, format, size, source, path; copy/download/view-properties actions
- **Item snapshots** — the exact notebook code and parameters (or SJD settings) as they were at execution time, plus the triggering pipeline/activity if applicable
- **Diagnostics panel** — real-time recommendations and error analysis from **Spark Advisor**, built on pattern-matching against common failure causes

**Spark monitoring APIs** exist at two levels:

- **Workspace/item-level list APIs** — list Spark applications in a workspace, for a notebook, for a Spark Job Definition, or for a lakehouse
- **Single-application deep-dive APIs** — get a specific notebook run or SJD submission's details; **Spark open-source metrics APIs** (aligned with the Spark History Server API); **Livy Log**, **Driver Log** and **Executor Log** endpoints; **resource usage APIs**

> 🧠 **Mental model —** Monitor hub and Recent Runs are the flight board (status and timing at a glance). The application detail page's Logs/Diagnostics tabs are the black box recorder (what actually happened inside one flight). The monitoring APIs are the maintenance crew's terminal — same data, scriptable for dashboards or alerting pipelines outside the Fabric portal.

### 1.5 Eventstream monitoring

Two monitoring views inside the eventstream editor:

- **Data insights** — metrics on the status and performance of the eventstream, its sources and its destinations; selecting a node on the canvas scopes the panel to that node's metrics
- **Runtime logs** — detailed logs from the eventstream engine itself, at three severity levels: **warning**, **error**, **information**

Availability of each view depends on which source/destination is selected — an **Azure Event Hubs source** or a **Lakehouse destination** specifically must be present to unlock these panels for that node.

### 1.6 Eventhouse ingestion monitoring

Rides on the same **workspace monitoring** mechanism as pipelines and Copy jobs: from the Eventhouse explorer pane (or **Workspace settings → Monitoring**), add a monitoring Eventhouse, which auto-provisions a KQL database capturing workspace activity logs. That monitoring KQL database exposes five queryable table families:

| Table family | Covers |
| :--- | :--- |
| **Metrics** | Numeric performance counters for the eventhouse |
| **Command logs** | Management commands executed against the eventhouse |
| **Data operation logs** | Data-management operations (e.g. merges, extents — Eventhouse's immutable storage shards) |
| **Ingestion results logs** | Per-ingestion outcome — success/failure detail for both queued and streaming ingestion |
| **Query logs** | Queries executed against the eventhouse's KQL databases |

Prebuilt **Real-Time Dashboard** and **Power BI report** monitoring templates (from the `fabric-toolbox` GitHub repo) visualise these tables without hand-writing KQL.

### 1.7 Copy job monitoring

**Copy Job** is one of the 17 item types natively tracked in **Monitor hub** — status, duration and submitter appear alongside every other job type with no extra setup. For **row-level and per-mapping detail**, enabling **workspace monitoring** produces a dedicated **`CopyJobActivityRunDetailsLogs`** table in the monitoring Eventhouse: **one record per source-to-destination table/object mapping** in a Copy job run, queryable with KQL alongside pipeline and eventhouse logs from the same workspace.

### 1.8 Capacity Metrics app — the admin-side view

Scoped to **capacity administrators**, not item owners. It answers "how is my whole capacity doing," not "did my job succeed."

| Page | Shows |
| :--- | :--- |
| **Health** | High-level overview across every capacity you administer; flags the ones consuming the most compute or hitting throttling/query rejections |
| **Compute** | **14-day** compute-performance view — ribbon charts, utilization trends, a matrix of operations by item |
| **Storage** | **30-day** storage usage by workspace, including soft-deleted data |
| **Timepoint** | Drill into a specific **30-second** window to see which operations consumed the most compute |
| **Timepoint summary** | Same window, summarized by operation type rather than individual operation |
| **Timepoint item detail** | Granular per-operation detail within an item at a timepoint — filterable by operation ID, user, CU threshold |
| **Autoscale compute for Spark** / **detail** | Autoscaling behaviour for Spark workloads specifically |

Installing the app requires **capacity admin** rights; after install, an admin can **share** the report with other users or **B2B guests** without granting them admin rights. Data has processing latency: **usage typically appears within 10–15 minutes**, while dimensions (capacities/workspaces/items) refresh **once at midnight local time** via the app's own scheduled semantic model refresh — so a brand-new capacity or workspace won't show up until the next refresh (or a manual one).

Install and share the app **proactively**, before a throttling incident forces a scramble to get admin access.

> 🔑 **Exam fact —** **The Capacity Metrics app does not support alerts or notifications on its own.** For real-time alerting on capacity conditions, use **Activator** against **Fabric capacity overview events** in Real-Time hub.

> ⚠️ **Trap —** Treating the Capacity Metrics app as a per-job failure/success dashboard. It is a **capacity resource-consumption** tool — CU%, throttling, memory, storage — not a job-status tool. "Why did my pipeline fail" → Monitor hub or pipeline run history. "Why is everything on this capacity slow" / "are we about to be throttled" → Capacity Metrics app.

### 1.9 THE monitoring-surfaces table

| Surface | What to monitor | Historical depth | Access path |
| :--- | :--- | :--- | :--- |
| **Monitor hub** | Cross-item status/timing/error summary (17 item types) | 100 rows / 30 days (main); 30 days (per-item historical) | **Monitor** in nav pane |
| **Pipeline run/activity monitoring** | Run + activity-level detail, Gantt, input/output JSON, rerun-from-failure | 2,000+ activity runs (Load more) | Pipeline **···** → View run history → Go to monitor |
| **Dataflow Gen2 refresh history** | Refresh status, per-table/per-activity detail, downloadable logs | 50 runs UI / 250 runs or 6 months in OneLake; logs 28 days | Dataflow **···** → Recent runs |
| **Spark application detail** | Jobs, executor resources, Livy/Driver/Prelaunch logs, input/output data, code snapshots | Per-application, while retained | Monitor hub → Spark application, or item Recent Runs |
| **Spark monitoring APIs** | Same data as above, programmatically | Same as UI | REST API (workspace/item list + single-application) |
| **Eventstream Data insights / Runtime logs** | Source/destination throughput, engine warnings/errors/info | Live/session-scoped | Eventstream editor, lower pane |
| **Eventhouse ingestion monitoring** | Metrics, command logs, data ops, ingestion results, query logs | Per workspace-monitoring retention | Workspace settings → Monitoring → Eventhouse |
| **Copy job monitoring** | Job status (Monitor hub) + per-mapping detail (workspace monitoring) | 30 days (Monitor hub) / workspace-monitoring retention | Monitor hub, or `CopyJobActivityRunDetailsLogs` table |
| **Capacity Metrics app** | CU utilization, throttling, storage, autoscale, by capacity | 14 days (Compute); 30 days (Storage) | Installed app (capacity admin only) |

### 1.10 Decision guidance: symptom → which surface to open first

| Symptom | Open first |
| :--- | :--- |
| "Did my pipeline/notebook/dataflow run succeed, and when?" | Monitor hub |
| "My pipeline failed partway through — how do I fix and resume without redoing everything?" | Pipeline run history → **Rerun from failed activity** |
| "My dataflow refresh failed — what exactly went wrong in which table?" | Dataflow Gen2 refresh history → detailed logs |
| "My Spark job is slow or failing — what's the driver actually logging?" | Spark application detail page → **Logs** tab |
| "I need Spark run data in an automated dashboard, not the portal" | Spark monitoring APIs |
| "Is my eventstream actually receiving/forwarding events?" | Eventstream **Data insights** |
| "Why did an event get dropped or transformed unexpectedly?" | Eventstream **Runtime logs** |
| "Is data landing in my Eventhouse tables?" | Eventhouse ingestion monitoring (Ingestion results logs) |
| "Which rows failed in my Copy job's table mapping?" | Workspace monitoring → `CopyJobActivityRunDetailsLogs` |
| "Everything on this capacity feels slow, or we're getting throttled" | Capacity Metrics app |
| "I need to be paged the moment any of the above happens" | Activator (§3) |

### 1.11 Common issues and errors

| Issue | Cause | Resolution |
| :--- | :--- | :--- |
| Monitor hub shows no history for a job older than 30 days | Monitor hub retention (main view and Historical runs) capped at 30 days | Use item-specific history (Dataflow refresh history up to 6 months) or a workspace-monitoring KQL query |
| Dataflow Gen2 "Download detailed logs" button missing or failing for a gateway-based refresh | **Admin consent for gateway diagnostics** not enabled at tenant and/or gateway level | Enable it at **both** levels, then retry after the next refresh |
| Spark application Logs tab shows no content | Job still queued, or cluster creation failed before logs could be generated | Wait for execution to start, or investigate the cluster-creation failure separately (Diagnostics panel / Spark Advisor) |
| Newly created workspace or item doesn't appear in the Capacity Metrics app | Dimensions (capacities/workspaces/items) refresh once daily via the app's own scheduled semantic model refresh | Wait for the next scheduled refresh, or manually refresh the app's semantic model |
| Eventstream Data insights / Runtime logs panel empty or unavailable for a node | Selected source/destination type doesn't support these panels (only Event Hubs sources / Lakehouse destinations expose them) | Select a supported node, or use the destination's own monitoring surface (e.g. Eventhouse ingestion monitoring) |
| Can't find why a specific row failed to copy in a Copy job | Monitor hub only shows job-level status, not row/mapping-level detail | Enable workspace monitoring and query `CopyJobActivityRunDetailsLogs` |

---

## 2. Semantic Model Refresh
*Source: `02-semantic-model-refresh.md`*

### 2.1 Refresh types: Import vs Direct Lake framing

| Aspect | Import refresh | Direct Lake refresh (framing) |
| :--- | :--- | :--- |
| What moves | Entire data volume, replicated into an in-memory cache | Metadata only — references to the latest Parquet files |
| Typical duration | Minutes to hours, depending on volume | Seconds |
| Resource cost | High — data-source and capacity CPU/memory | Low — metadata analysis of the Delta log |
| Data prep location | Power Query, defined inside the semantic model | Upstream in OneLake (Spark, T-SQL, dataflows, pipelines) |
| Engine | VertiPaq (in-memory cache) | VertiPaq (on-demand column loading, called **transcoding**) |

Both Import and Direct Lake use the same **VertiPaq** engine — the difference is entirely in how (and how much) data gets into memory. **DirectQuery** translates every query live against the source and never populates a VertiPaq cache at all.

> 🧠 **Mental model —** Import refresh = photocopying the entire book every time something changes. Direct Lake framing = updating the book's table of contents to point at the newest chapters someone else already wrote in OneLake. DirectQuery = never keeping a copy at all; you call the library every time someone asks a question.

### 2.2 What framing means, and when reframing happens

**Framing** gives semantic model owners point-in-time control over what Delta table state a Direct Lake model reflects. During framing the model analyses each Delta table's transaction log, **evicts only the column segments and join indexes whose underlying data actually changed**, and updates references to the newest Parquet files. **Dictionaries are usually retained** (not dropped) — new values are appended — which is why incremental framing is cheap even on tables that changed.

Critically: **from the moment framing completes, all queries see the Delta tables exactly as they stood at that framing operation** — not whatever the Delta tables contain right now. New Parquet files written after framing stay invisible until the *next* framing operation.

**Reframing** is triggered by any of:

- **Automatic updates** *(enabled by default)* — the model detects OneLake data changes and reframes with no user action
- **Manual refresh** — a user selects Refresh in the portal
- **Scheduled refresh** — same schedule mechanism as Import mode
- **Programmatic refresh** — enhanced refresh API, XMLA/TOM/TMSL, or `sempy`

Disable automatic updates when you want framing only on your own schedule — useful when upstream ETL is long-running and report users must not see a half-written intermediate state mid-load.

> ⚠️ **Trap —** Assuming a Direct Lake model always shows "live" data because there is no visible refresh schedule to configure. A Direct Lake model only reflects data as of its **last successful framing operation**. If automatic updates are disabled (or delayed) and no manual/scheduled/programmatic refresh has run since new data landed, users see stale data despite Direct Lake's near-real-time reputation.

### 2.3 DirectQuery fallback

**Direct Lake on SQL endpoints** can silently switch a table to DirectQuery mode when Direct Lake conditions aren't met. **Direct Lake on OneLake never falls back** — it runs exclusively in `DirectLakeOnly` mode.

Direct Lake mode holds only when **all** of these are true:

- No table references SQL **row-level security (RLS)**, **dynamic data masking (DDM)** or **object-level security (OLS)** defined at the SQL analytics endpoint
- No table is based on an **unmaterialized SQL view**
- No table exceeds **Fabric capacity guardrails** (Parquet files, row groups, rows per table)
- The model was **refreshed (framed) after** the underlying Delta tables were created or modified

The **`DirectLakeBehavior`** model property (set via TOM/TMSL, or the Model view's Properties pane) controls what happens when conditions aren't met:

| Value | Behavior |
| :--- | :--- |
| **Automatic** *(default)* | Silently falls back to DirectQuery; slower but functional — use in production |
| **DirectLakeOnly** | Query fails with an error instead of falling back — use during development to surface problems early |
| **DirectQueryOnly** | Always uses DirectQuery — use to benchmark fallback performance |

Diagnose which tables are falling back and why with a DAX query:

```dax
EVALUATE TABLETRAITS()
```

The **`DirectLakeFallbackInfo`** column reports a fallback reason per table, or `None` if the table is running in Direct Lake mode.

| Fallback cause | Fix |
| :--- | :--- |
| Table isn't framed | Refresh the semantic model to frame it |
| Table is based on a SQL view | Materialize the view as a Delta table, or accept DirectQuery for that table |
| OLS at the SQL endpoint | Move object-level security to the semantic model, or accept fallback |
| RLS or DDM at the SQL endpoint | Move row-level security to the semantic model, or accept fallback |
| Delta table exceeds guardrails | Run `OPTIMIZE` / `VACUUM`, or move to a higher Fabric SKU |
| Capacity under memory pressure | Reduce concurrent workloads, or upgrade the capacity SKU |

### 2.4 Refresh history and failure diagnosis

View refresh history for any semantic model: select the model → **Refresh → Refresh history** — status, duration and error messages per attempt. For Direct Lake models the Fabric portal's refresh history includes a **Direct Lake tab** listing Direct Lake-related refresh (framing) failures; **successful framing operations aren't always listed individually** unless the status actually changed (e.g. from failure to success).

| Cause | Typical symptom | Fix direction |
| :--- | :--- | :--- |
| **Credentials** | Refresh fails immediately with an authentication/authorization error; after **4 consecutive failures** Power BI automatically deactivates the refresh schedule | Update the data source credentials/connection, then re-enable the schedule |
| **Timeout** | Refresh runs the full duration then fails with a timeout error | Adjust the `timeout` parameter (enhanced refresh), reduce data volume per refresh, or split into multiple table/partition refreshes |
| **Memory / capacity limits** | Resource-governance error — the refresh exceeds the per-model or per-query memory limit the capacity enforces, or too many models are refreshing concurrently | Reduce concurrent refreshes, optimize the model, or scale up the capacity SKU |

> ⚠️ **Trap —** Treating every refresh failure as a data-source problem. A refresh that fails after running for hours and hits a **resource-governance** error is a **capacity sizing** issue, not source connectivity — check the Capacity Metrics app's Compute page for concurrent refresh load before assuming the data source is broken.

### 2.5 The enhanced refresh API

The classic Refresh Dataset REST API triggers a refresh but ties up a long-running HTTP connection. **Enhanced refresh** — the same API family used asynchronously — removes that requirement and adds:

- **Batched commits** (`commitMode`: `transactional` or `partialBatch`)
- **Table- and partition-level refresh** (`objects` array — refresh only what changed)
- **Incremental refresh policy application** (`applyRefreshPolicy`)
- **`GET` refresh details** — poll `requestId` for status without blocking
- **Refresh cancellation** — `DELETE` an in-progress enhanced refresh by `requestId`
- **Timeout configuration** (`timeout`, **default 5 hours per attempt**; total duration including retries **capped at 24 hours**)

| Verb | Purpose |
| :--- | :--- |
| `POST /refreshes` | Start a refresh; returns a `requestId` in the `Location` header |
| `GET /refreshes` | List historical/current/pending refreshes (supports `$top`) |
| `GET /refreshes/{requestId}` | Poll one refresh's status (`Unknown`, `InProgress`, `Completed`, `Failed`, `Disabled`, `Cancelled`) |
| `DELETE /refreshes/{requestId}` | Cancel an in-progress **enhanced** refresh only — standard refreshes triggered via the portal button can't be cancelled this way |

```json
POST /v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes
{
  "commitMode": "partialBatch",
  "objects": [ { "table": "FactSales", "partition": "2026Q3" } ],
  "applyRefreshPolicy": true,
  "timeout": "05:00:00"
}
```

> 🧠 **Mental model —** A standard scheduled refresh is calling and waiting on hold — the connection must stay open. Enhanced refresh is leaving a voicemail and checking back later with a claim ticket (`requestId`) — hang up immediately and poll whenever convenient, which matters for large models whose refresh runs for hours.

### 2.6 Scheduled refresh configuration

| License | Max scheduled refreshes/day |
| :--- | :--- |
| Power BI Pro | 8 |
| Power BI Premium Per User (PPU) | 48 |
| Premium / Fabric capacity | 48 |

A schedule is **automatically deactivated after 4 consecutive failures**, or **immediately** if Power BI detects an unrecoverable configuration error (e.g. invalid/expired credentials). In both cases the fix is correcting the underlying issue then **manually re-enabling** the schedule — **it does not reactivate itself** once the root cause is fixed.

### 2.7 Monitoring via workspace lineage and Monitor hub

Semantic model refreshes are one of the item types Monitor hub tracks natively (§1) — status, duration and error details appear alongside pipelines, dataflows and every other tracked type. **Workspace lineage view** additionally shows a semantic model's upstream dependencies (lakehouse/warehouse tables, dataflows) as a visual graph — the fastest way to confirm *what a semantic model actually depends on* before diagnosing a refresh failure. A credential or availability problem in an upstream source surfaces as a refresh failure on the model even though the root cause lives upstream.

### 2.8 Semantic link (`sempy`) for programmatic checks

**Semantic link**, via the `sempy.fabric` Python package (**available by default in the Fabric Spark runtime, Spark 3.4+**), lets a notebook read from and orchestrate semantic models without leaving the notebook environment:

- The **`sempy.fabric.semantic_model`** module provides functions to **list, deploy, refresh and manage** semantic models — including **Direct Lake connection management** and refresh-request orchestration
- **`RefreshExecutionDetails`** represents the status of a refresh request, mirroring what the enhanced refresh API's `GET /refreshes/{requestId}` returns — but callable directly from Python inside a notebook, useful for building a refresh-orchestration or validation step into a larger pipeline without a separate REST call

```python
import sempy.fabric as fabric
# trigger + poll a semantic model refresh from the same notebook that wrote the Delta tables
# RefreshExecutionDetails mirrors GET /refreshes/{requestId}
```

> 🧠 **Mental model —** `sempy` is the enhanced refresh API's native Python front door — anything a raw REST call can do, you can do inline in a notebook cell, which matters when refresh orchestration must sit in the same Spark pipeline that just wrote the Delta tables.

### 2.9 Common issues and errors

| Issue | Cause | Resolution |
| :--- | :--- | :--- |
| Direct Lake report shows data missing recently loaded rows | Model hasn't been reframed since new data landed (automatic updates disabled or delayed) | Trigger a manual/programmatic refresh (framing), or verify automatic updates is enabled |
| One table's queries slower than the rest of a Direct Lake on SQL endpoints model | That table is falling back to DirectQuery | Run `EVALUATE TABLETRAITS()`, identify the fallback cause, apply the matching fix |
| Refresh fails with a resource-governance / memory error | Refresh exceeds per-model/per-query memory limits, or too many concurrent refreshes on the capacity | Reduce concurrency, optimize the model, or scale the capacity SKU |
| Refresh schedule stopped running with no manual change | 4 consecutive failures auto-deactivated it, or an unrecoverable credential error occurred | Check refresh history, fix the root cause, manually re-enable the schedule |
| Enhanced refresh `POST` returns **`400 Bad Request`** | A refresh is already running on that model — **only one refresh operation is accepted at a time per model** | Wait for the in-progress refresh, or poll `GET /refreshes` before submitting a new one |
| `DELETE /refreshes/{requestId}` fails to cancel a refresh | The refresh was triggered by a scheduled/on-demand portal action, not the enhanced refresh API — those can't be cancelled via `DELETE` | Let it complete, or trigger future refreshes via the enhanced refresh API if cancellation is needed |

---

## 3. Activator and Alerts
*Source: `03-activator-alerts.md`*

**Fabric Activator** is Fabric's no-code event-detection engine — it watches data streams (and Fabric/Azure/business events) for conditions you define, then automatically fires an action: an email, a Teams message, a Power Automate flow, or running another Fabric item. It **reached General Availability in November 2024** and is officially named **Fabric Activator** (Fabric product names shift — verify current naming against the live docs before the exam).

### 3.1 Core concepts: events, objects, conditions, rules

| Concept | Definition |
| :--- | :--- |
| **Event** | A single observation about the state of something — a telemetry reading, a file drop, a row change — carrying a timestamp, a payload, and one or more identifying attributes |
| **Object** | Events grouped by a shared identifier (e.g. every event with the same `device_id`) so rules evaluate per instance — a freezer, a vehicle, a user session — independently of every other instance |
| **Rule** | The condition to detect on an object (or on raw events) plus the action to take when it fires; each Activator item can hold one or more rules, evaluated continuously |
| **Condition** | The logic a rule tests — a **stateless** comparison (`value < 50`) evaluated on each event in isolation, or a **stateful** expression (`BECOMES`, `DECREASES`, `INCREASES`, `EXIT RANGE`, or absence-of-data / **heartbeat**) that tracks state across events per object |

Rules come in **three flavors**:

- Rules **on events** — fire every time a matching event arrives
- Rules **on objects** — fire on the arrival of an object, or an object meeting a condition
- Rules **on properties** — monitor a specific attribute of an object over time (e.g. a package's temperature staying within range)

Stateful evaluation relies on **delta detection** (tracking changes between prior and current values), **temporal sequencing** (time-based conditions like heartbeat absence), and **state transitions** (a rule fires only on *entry* into a new state, not on every event while that state persists) — the last point is what stops a threshold breach spamming one alert per incoming event.

Each rule condition compiles into an execution graph evaluated continuously, in memory, targeting **subsecond latency for stateless rules on streaming data**; rules with aggregations or lookback windows take longer, bounded by the lookback period and late-arrival tolerance.

> 🧠 **Mental model —** Stateless rules are a motion-triggered light: every qualifying event flips it on, no memory needed. Stateful rules are a smoke detector with a memory: it alarms once on the transition into "smoke detected," then waits for a transition back to normal before it can alarm again. That transition-only behaviour is exactly why `BECOMES`/`DECREASES` beat raw thresholds for anything that could otherwise spam recipients.

### 3.2 Alert sources

| Source | Notes |
| :--- | :--- |
| **Eventstreams** | The primary streaming source — Activator subscribes to one or more eventstreams; alert authoring is embedded directly in the Eventstream editor |
| **KQL querysets / Real-Time dashboards** | Set an alert on a scheduled KQL query's results (**default check frequency: 5 minutes**), or on a Real-Time dashboard tile meeting a condition — authored via **Set Alert** on the query results ribbon, or directly on a dashboard tile |
| **Power BI report visuals** | Table-visual row detection on a published report; Activator supports only a defined list of visual types: **stacked/clustered column and bar, line, area, pie, donut, gauge, card, KPI, and specific map types using a *Location* field — not Latitude/Longitude** |
| **Real-Time hub events** | **Fabric job events** (e.g. pipeline succeeded/failed), **Fabric workspace item events**, **Fabric OneLake events**, **Fabric capacity overview events** — all set up as alerts directly from their respective Real-Time hub pages |
| **Fabric Data Warehouse SQL query results** *(preview)* | Evaluate a SQL query on a configurable schedule and trigger on the result set — alerting on warehouse data without a streaming source |
| **Business events** *(preview)* / **Fabric Ontology business entities** *(preview)* | Set alerts directly on published business events, or on modeled ontology entities for operational decisioning |

> 🔑 **Exam fact —** **Creating alerts from the Fabric or Power BI Capacity Metrics app, or from a SQL analytics endpoint directly, is not supported.** A scenario naming either as the alert source is a distractor. Route capacity-throttling alerting through **Fabric capacity overview events** in Real-Time hub, and warehouse alerting through the **Warehouse SQL query (preview)** mechanism — not the SQL analytics endpoint.

### 3.3 Actions

When a rule's condition is met, Activator can trigger:

- **Email** — **internal recipients only** (see limits)
- **Teams** — message to an individual, group chat, or channel (**shared channels only, not private**)
- **Power Automate** — a custom flow, for integration with external/third-party systems
- **Run a Fabric item** — **pipeline, notebook, Spark job definition, dataflow, User Data Function, or copy job *(preview)***
- **Publish a business event** *(preview)* — for triggering downstream processes that consume business events

When a rule fires, Activator sends the action and **continues monitoring immediately** — it does not wait for the action to complete, which is what lets it process many objects' state transitions concurrently without one slow downstream action blocking detection on the rest.

> ⚠️ **Trap —** Assuming Activator can trigger literally any Fabric item type, or that one alert scenario always maps to exactly one action. It supports a specific list (pipeline, notebook, Spark job definition, dataflow, UDF, copy job, business-event publish, Power Automate, Teams, email) — and **a single rule can be configured to take more than one action** (e.g. email the on-call engineer *and* rerun the failed pipeline).

### 3.4 Setting alerts from Monitor hub and RTI surfaces

Alert authoring is deliberately **decentralized** — rather than forcing every alert through a single Activator item editor, Fabric embeds "Set Alert" into the surface where the condition naturally lives:

| Where you are | How you set the alert |
| :--- | :--- |
| **Eventstream editor** | Attach an operator chain, then configure an Activator destination directly in the event-processing canvas |
| **KQL queryset results** | Run the query, select **Set Alert** in the ribbon, configure frequency and condition in the **Add Rule** pane |
| **Real-Time dashboard tile** | Select the tile's alert option directly (tile must be **KQL-based, non-static, single time range, no `make-series`**) |
| **Power BI report visual** | Set alert directly on a supported visual type in a published report |
| **Real-Time hub → Fabric events** | Select **Job events**, **Workspace item events**, **OneLake events** or **Capacity overview events**, pick the specific item/event, then target an existing Activator item or create a new one |

Regardless of entry point, **every alert ultimately lives inside an Activator item in a workspace** — the in-context "Set Alert" flows are shortcuts that create or add to that item, not a separate alerting mechanism.

### 3.5 Alert-on-pipeline-failure pattern

The canonical pattern uses **Fabric job events** as the source, since pipeline runs triggered by schedules or manual triggers surface as job events:

1. From **Real-Time hub**, select **Fabric events → Job events** (or start from an Activator item's **Get Data** tile and pick **Job events**)
2. Select the specific pipeline to monitor and the event of interest (e.g. run failed)
3. Choose the workspace/Activator item to save the rule into — create a new Activator item or add to an existing one
4. Configure the action — typically email or Teams to the on-call owner, and optionally a Fabric-item action to trigger a remediation pipeline

For **workspace-wide** failure alerting (many pipelines, one rule), the alternative is enabling **workspace monitoring** and building a single Activator rule on a **KQL queryset** that queries the monitoring Eventhouse's **`ItemJobEventLogs`** table for recent failures — one rule covers every pipeline in the workspace, including ones added later, instead of a rule per pipeline. This is a **distinct data-source path (KQL queryset)** from the direct job-events path, and the exam may present both as valid alternatives with different maintenance tradeoffs.

> 🧠 **Mental model —** A job-events rule per pipeline is a smoke detector in every room: precise, but you install and maintain one per room. A workspace-monitoring KQL rule is one central fire-alarm panel wired to every room's sensor: less setup per new pipeline, but you depend on the wiring (workspace monitoring) being enabled and the query catching every case.

### 3.6 Limits and licensing notes

Activator instances are scoped to a **Fabric capacity** and billed **pay-as-you-go** — cost accrues only while rules are actively running, which favours intermittent detection scenarios over always-on polling.

**Documented general limitations:**

- Alerts from **Dynamic M parameters** in a report aren't supported
- Alerts from the **Fabric or Power BI Capacity Metrics app** aren't supported
- Alerts from a **SQL analytics endpoint** directly aren't currently supported (use the Warehouse SQL query preview mechanism instead)
- Email recipients must be **internal addresses on the creator's verified Entra tenant domains** — no external or guest email addresses
- **Teams group chats must be recently active** to appear as a target; only **shared channels** are supported, **not private channels**
- **Lifecycle management** (deployment pipelines, Git integration) doesn't support Activator items that use **Azure Blob Storage events, Power BI, or User Data Functions** as a source/action — including one in a deployment pipeline or Git-integrated workspace produces a **deploy/commit error**

**Documented numeric limits:**

| Limit | Value |
| :--- | :--- |
| Max incoming events/second/rule | **10,000** (Activator stops the rule if exceeded) |
| Email messages/Activator item/hour | 500 |
| Email messages/rule/recipient/hour | 30 |
| Teams messages/Activator item/hour | 500 |
| Teams messages/rule/recipient/hour | 30 |
| Teams messages/recipient/hour | 100 |
| Teams messages/Teams tenant/second | 50 |
| Power Automate flow executions/rule/hour | 10,000 |
| Fabric item activations/user/minute | 50 |

Before activating a rule, **preview its estimated firing rate against historical data** to catch alert-spam risk before it reaches recipients.

Activator reached **GA in November 2024**; all **preview-era items and rules were phased to read-only in January 2025 and deleted in March 2025** — a reminder that Activator's exact feature list (particularly preview vs GA) is worth re-verifying against current docs close to exam day.

### 3.7 Common issues and errors

| Issue | Cause | Resolution |
| :--- | :--- | :--- |
| Alert never fires despite the condition clearly being met | Rule may be evaluating a stateless condition that already "settled" into the triggering state before the rule was activated, or state-transition logic requires exiting and re-entering the state | Review whether the condition needs `BECOMES`/transition logic vs a raw stateless comparison |
| Emails stop arriving mid-incident during a high-volume event | Action rate limit exceeded (e.g. 500 emails/Activator item/hour) | Reduce alert granularity (aggregate/dedupe upstream), or split rules across more Activator items if genuinely independent |
| Can't add an Activator item to a deployment pipeline | The item uses Azure Blob Storage events, Power BI, or User Data Functions as a source/action — unsupported in lifecycle management | Recreate the dependency using a supported source/action, or manage that Activator item outside the deployment pipeline |
| Email alert recipient never receives the notification | Recipient's domain isn't a verified domain on the creator's Entra tenant, or the recipient is external/guest | Verify the domain in **Microsoft Entra ID → Custom domain names**; use an internal recipient |
| Power BI visual alert can't be created on a chosen visual | The visual type isn't in Activator's supported list, or the visual uses Dynamic M parameters | Switch to a supported visual type, or remove the Dynamic M parameter dependency |
| Attempt to alert directly from the Capacity Metrics app fails | Not a supported alert source | Use **Fabric capacity overview events** in Real-Time hub instead |

---

## Decision rules — pick the right thing

| Scenario / requirement | Choose | Why |
| :--- | :--- | :--- |
| "Did my pipeline/notebook/dataflow run succeed, and when?" | Monitor hub | Cross-item status board over 17 item types |
| Pipeline failed partway; don't redo successful activities | Pipeline run history → **Rerun from failed activity** | Resumes at the failed activity; a full rerun wastes compute and can duplicate output on non-idempotent sinks |
| Need per-table/per-activity detail on a failed dataflow refresh | Dataflow Gen2 refresh history → **Download detailed logs** | Mashup-engine logs, 28-day window |
| Need what the Spark driver actually logged | Spark application detail page → **Logs** tab (Livy / Prelaunch / Driver) | Only surface with raw process logs |
| Spark run data must feed an automated dashboard outside the portal | Spark monitoring APIs (list + single-application, Livy/Driver/Executor Log, resource usage, Spark open-source metrics) | Scriptable equivalent of the UI |
| Check first before manually digging through raw Spark logs | **Diagnostics** panel / Spark Advisor | Pattern-matches common failure causes automatically |
| Is the eventstream receiving/forwarding events? | Eventstream **Data insights** | Source/destination throughput metrics |
| Why was an event dropped or transformed unexpectedly? | Eventstream **Runtime logs** | Engine-level warning/error/information |
| Is data landing in Eventhouse tables? | Eventhouse **Ingestion results logs** | Per-ingestion success/failure for queued and streaming |
| Which rows/mappings failed in a Copy job? | Workspace monitoring → `CopyJobActivityRunDetailsLogs` | Monitor hub only shows job-level status |
| Need history older than 30 days, or cross-item log queries | **Workspace monitoring** KQL Eventhouse | Monitor hub caps at 100 rows / 30 days |
| Everything on the capacity is slow, no single item fails | Capacity Metrics app → **Compute** or **Timepoint** | Capacity-wide CU contention, not item failure |
| Need alerting on capacity conditions | Activator on **Fabric capacity overview events** (Real-Time hub) | Capacity Metrics app cannot alert |
| Freshness matters and data volume is large / prepped upstream in OneLake | **Direct Lake** (framing = seconds, metadata only) | No data copy; VertiPaq loads columns on demand |
| DirectQuery fallback must never silently degrade performance | **Direct Lake on OneLake** | It never falls back; SQL-endpoint Direct Lake can |
| Want fallback surfaced as a hard error during development | `DirectLakeBehavior = DirectLakeOnly` | Query fails instead of silently degrading |
| Want to benchmark what fallback costs | `DirectLakeBehavior = DirectQueryOnly` | Forces DirectQuery for comparison |
| Production Direct Lake on SQL endpoints, availability over speed | `DirectLakeBehavior = Automatic` *(default)* | Slower but functional |
| Report users must not see half-written intermediate ETL state | Disable **automatic updates**; frame on your own schedule | Framing is point-in-time |
| Large/complex model; refresh only what changed; need cancellation | **Enhanced refresh API** (`objects`, `timeout`, `commitMode`, `DELETE`) | Async, table/partition scoped, cancellable |
| Refresh must run right after a Spark write, in the same notebook | **`sempy.fabric.semantic_model`** | Notebook-native trigger + `RefreshExecutionDetails` polling |
| Confirm what a semantic model depends on before blaming it | **Workspace lineage view** | Many refresh failures originate upstream |
| Alert should fire once on entering a bad state, not per event | **Stateful** condition (`BECOMES`, `DECREASES`, `INCREASES`, `EXIT RANGE`, heartbeat) | Fires only on state transition |
| Alert on every qualifying raw event | **Stateless** comparison | No memory; evaluates each event in isolation |
| Alert on one specific pipeline failing | Real-Time hub → **Fabric job events** | Pipeline runs surface as job events |
| Alert on failures across many/growing pipelines with one rule | Workspace monitoring + one Activator rule on a **KQL queryset** over `ItemJobEventLogs` | One rule covers current and future pipelines |
| Alert on warehouse data with no streaming source | **Fabric Data Warehouse SQL query results** *(preview)* | Scheduled SQL evaluation; SQL analytics endpoint is unsupported |
| Notify *and* remediate the same condition | One rule, multiple actions | A single rule supports more than one action |
| Integrate an alert with an external/third-party system | **Power Automate** action | Email/Teams are internal-only |

## Numbers, limits and defaults to memorise

| Thing | Value | Note |
| :--- | :--- | :--- |
| Monitor hub item types covered | **17** | Dataflow Gen1 **not** included |
| Monitor hub main Activities view | **100 activities / past 30 days** | Hard cap |
| Monitor hub per-item Historical runs | **30 days** | Same retention ceiling |
| Pipeline activity runs before "Load more" | **2,000** | Activity runs table |
| Dataflow Gen2 refresh history in UI | **50 refresh histories or 6 months**, whichever hits first | List view |
| Dataflow Gen2 refresh history in OneLake | **250 refresh histories or 6 months** | Underlying store |
| Dataflow Gen2 detailed log download window | **28 days** after the refresh | Available a few minutes after completion |
| Dataflow Gen2 detailed logs minimum permission | Workspace **Viewer** | Gateway refreshes also need Admin consent for gateway diagnostics at tenant **and** gateway level |
| Spark application detail page tabs | **6** — Jobs, Resources, Logs, Data, Item snapshots, Diagnostics | Diagnostics = Spark Advisor |
| Spark **Logs** tab process log families | **3** — Livy, Prelaunch, Driver | Empty usually means still queued or cluster creation failed |
| Eventstream Runtime log severities | **3** — warning, error, information | Requires Event Hubs source or Lakehouse destination node |
| Eventstream message size cap | **1 MB** per message | A *size* cap — not an alert-volume throttle; classic distractor |
| Eventhouse monitoring table families | **5** — Metrics, Command logs, Data operation logs, Ingestion results logs, Query logs | Via workspace monitoring |
| `CopyJobActivityRunDetailsLogs` granularity | **1 record per source-to-destination table/object mapping** | Per Copy job run |
| Capacity Metrics — Compute page window | **14 days** | Compute performance |
| Capacity Metrics — Storage page window | **30 days** | Includes soft-deleted data |
| Capacity Metrics — Timepoint window | **30 seconds** | Drill into peak consumption |
| Capacity Metrics usage data latency | **10–15 minutes** | Typical |
| Capacity Metrics dimension refresh | **Once at midnight local time** | Capacities/workspaces/items; new ones invisible until then |
| Import refresh duration | **Minutes to hours** | Full data copy |
| Direct Lake framing duration | **Seconds** | Metadata only |
| Direct Lake automatic updates | **Enabled by default** | Reframes on detected OneLake changes |
| `DirectLakeBehavior` default | **Automatic** | Silent DirectQuery fallback |
| Refresh schedule auto-deactivation | After **4 consecutive failures** | Or immediately on unrecoverable config error; manual re-enable required |
| Enhanced refresh `timeout` default | **5 hours per attempt** | Configurable |
| Enhanced refresh total duration cap | **24 hours** including retries | Hard ceiling |
| Concurrent refreshes per model | **1** | A second `POST /refreshes` returns `400 Bad Request` |
| Scheduled refreshes/day — Power BI Pro | **8** | License-gated |
| Scheduled refreshes/day — PPU | **48** | License-gated |
| Scheduled refreshes/day — Premium / Fabric capacity | **48** | License-gated |
| `sempy` Spark runtime requirement | **Spark 3.4+** | Package available by default in Fabric Spark runtime |
| Activator core concepts | **4** — events, objects, conditions, rules | Objects = per-instance evaluation |
| Activator rule flavors | **3** — on events, on objects, on properties | Properties = one attribute of an object over time |
| KQL queryset alert default check frequency | **5 minutes** | Configurable |
| Activator stateless-rule latency target | **Subsecond** on streaming data | Aggregation/lookback rules take longer |
| Activator max incoming events/second/rule | **10,000** | Activator **stops the rule** if exceeded |
| Email messages / Activator item / hour | **500** | Throttled beyond |
| Email messages / rule / recipient / hour | **30** | Throttled beyond |
| Teams messages / Activator item / hour | **500** | |
| Teams messages / rule / recipient / hour | **30** | |
| Teams messages / recipient / hour | **100** | |
| Teams messages / Teams tenant / second | **50** | |
| Power Automate flow executions / rule / hour | **10,000** | |
| Fabric item activations / user / minute | **50** | |
| Activator GA date | **November 2024** | Preview items read-only Jan 2025, deleted Mar 2025 |

## Traps and common mistakes

**§1 Monitoring surfaces**

- Monitor hub is not a bottomless log — 100 rows / 30 days in the main view, 30 days in Historical runs. Older or log-level detail needs item-specific history or workspace monitoring's KQL Eventhouse.
- Dataflow **Gen1** is not tracked in Monitor hub; Gen2 and Gen2 CI/CD are.
- Monitor hub keyword search only searches **already-loaded rows**, not the whole history.
- A red icon in the workspace **Status** column can mean a **save/validate** failure *or* a **refresh** failure — hover to tell which; refresh history won't fix a validation error.
- Dataflow Gen2 detailed logs silently unavailable for gateway refreshes unless **Admin consent for gateway diagnostics** is on at **both** tenant and gateway level.
- Spark **Logs** tab empty does not mean the job is fine — it usually means the job is still queued or cluster creation failed.
- Eventstream Data insights / Runtime logs panels are **not universal** — only Event Hubs sources and Lakehouse destinations expose them.
- Copy job failures at row/mapping level are invisible in Monitor hub — you need `CopyJobActivityRunDetailsLogs`.
- The Capacity Metrics app is a **resource-consumption** tool, not a job-status tool — and it **cannot fire alerts**.
- A brand-new workspace/capacity is absent from the Capacity Metrics app until the midnight dimension refresh.

**§2 Semantic model refresh**

- Direct Lake is **not** automatically live — it reflects data as of the **last successful framing**; new Parquet files written after framing are invisible until the next framing.
- A "successful" notebook/pipeline run says nothing about whether the downstream Direct Lake model has reframed.
- Successful framing operations are not always individually listed in the Direct Lake refresh-history tab — only status changes reliably appear.
- With `DirectLakeBehavior = Automatic`, fallback is **silent** — one table gets slower but still returns correct results, so it looks like a performance problem, not a config problem.
- Not every refresh failure is a data-source problem — a resource-governance error after hours of running is a **capacity sizing** issue.
- A dead refresh schedule after 4 consecutive failures **does not reactivate itself** once the root cause is fixed.
- `DELETE /refreshes/{requestId}` only cancels refreshes that were **started by the enhanced refresh API** — portal-triggered refreshes cannot be cancelled this way.
- A second `POST /refreshes` while one is running returns **`400 Bad Request`** — one refresh at a time per model.
- Disabling **automatic updates** is a Direct Lake concept — it does nothing for an Import-mode model.

**§3 Activator and alerts**

- The **Capacity Metrics app** and the **SQL analytics endpoint** are explicitly **unsupported alert sources** — classic distractors.
- Alerts from **Dynamic M parameters** in a report aren't supported.
- Email alerts to **external/guest** addresses silently never arrive — recipients must be internal on verified Entra tenant domains.
- Teams **private** channels are not supported (shared channels only), and group chats must be **recently active** to appear as a target.
- Activator items using **Azure Blob Storage events, Power BI, or User Data Functions** break **deployment pipelines / Git integration** with a deploy/commit error.
- "Alerts stopped mid-incident" means **rate-limit throttling**, not a crash — Activator throttles or cancels excess actions rather than failing. Do not confuse this with **Eventstream's 1 MB per-message size cap**, which is unrelated to alert-volume throttling.
- A rule can fail to fire because the condition **already settled** into the triggering state before the rule was activated — stateful transitions need an exit and re-entry.
- Power BI visual alerts only work on the **supported visual list**; map visuals must use a **Location** field, not Latitude/Longitude.
- Real-Time dashboard tile alerts require the tile to be **KQL-based, non-static, single time range, and free of `make-series`**.
- Activator can only run a **specific list** of Fabric items as actions — but a single rule **can** take multiple actions.

## Exam tips

- **Monitor hub covers 17 item types but not Dataflow Gen1** — memorize the exclusion.
- **"Rerun from failed activity"** is pipeline-specific vocabulary; don't confuse it with a generic retry.
- Dataflow Gen2: **detailed logs = 28-day** download window; **refresh history = 50 UI rows / 250 or 6 months in OneLake**.
- Spark monitoring API names to recognise: **Livy Log, Driver Log, Executor Log**, plus the **Spark open-source metrics APIs**.
- Eventstream's two monitoring tabs are **Data insights** and **Runtime logs** (three severities: warning/error/information).
- Eventhouse's five monitoring table families: **Metrics, Command logs, Data operation logs, Ingestion results logs, Query logs**.
- The Capacity Metrics app is **admin-installed**, CU/throttling-focused, and explicitly **does not support alerts**.
- **Framing = metadata-only refresh; Import refresh = full data copy** — know this cold.
- **Direct Lake on OneLake never falls back** to DirectQuery; **Direct Lake on SQL endpoints can**, governed by `DirectLakeBehavior` (`Automatic` / `DirectLakeOnly` / `DirectQueryOnly`).
- **`EVALUATE TABLETRAITS()` + `DirectLakeFallbackInfo`** is the per-table fallback diagnostic.
- Enhanced refresh API verbs: **`POST`** (start), **`GET`** (status/list), **`DELETE`** (cancel — enhanced-triggered refreshes only).
- Scheduled refresh limits: **Pro = 8/day, PPU and Premium/Fabric capacity = 48/day**; auto-deactivates after **4 consecutive failures**.
- **`sempy.fabric.semantic_model`** gives notebook-native refresh orchestration and **`RefreshExecutionDetails`** status polling.
- Know Activator's four core concepts cold — **events, objects, conditions, rules** — and that **objects** are how per-instance (per-device, per-package) evaluation works.
- Stateful vocabulary: **`BECOMES`, `DECREASES`, `INCREASES`, `EXIT RANGE`, absence-of-data/heartbeat**; stateless is just a raw comparison.
- Alert sources: **Eventstreams, KQL querysets/Real-Time dashboards, Power BI visuals, Real-Time hub events (job / workspace item / OneLake / capacity overview), Warehouse SQL query (preview)**.
- Unsupported alert sources are a common distractor: **Capacity Metrics app** and **SQL analytics endpoint** directly.
- Actions: **email, Teams, Power Automate**, and running a **pipeline / notebook / Spark job definition / dataflow / UDF / copy job (preview)**, or **publishing a business event (preview)**.
- Pipeline-failure alerting uses **Fabric job events** — per-pipeline (Real-Time hub → Job events) or workspace-wide (KQL queryset against `ItemJobEventLogs`).
- Know the **rate limits exist** even if not memorized exactly — "alerts stopped during a huge spike" points to **throttling**, not a system failure.

## Key takeaways

- Monitor hub is the tenant-wide first stop across 17 item types, but its 100-row/30-day cap means it is not a durable log — workspace monitoring's KQL Eventhouse fills that gap.
- Pipeline monitoring's standout capability is **resuming from a failed activity** rather than a full rerun.
- Spark monitoring is layered: Monitor hub / Recent Runs for status → application detail tabs (Jobs, Resources, Logs, Data, Item snapshots, Diagnostics) for deep diagnosis → REST APIs for programmatic access.
- Eventstream, Eventhouse and Copy job each have a purpose-built monitoring view, and workspace monitoring ties several of them into one KQL-queryable Eventhouse.
- The Capacity Metrics app is the only admin-scoped, capacity-wide surface — CU utilization and throttling, not job success/failure — and it cannot alert on its own.
- Import refresh copies data; Direct Lake framing copies only metadata references — that's why framing takes seconds.
- Between framings, Direct Lake queries see a **fixed point-in-time snapshot**; reframing triggers on automatic updates, manual action, schedule, or programmatic call.
- DirectQuery fallback is a **Direct Lake on SQL endpoints-only** behaviour, governed by `DirectLakeBehavior` and diagnosable via `TABLETRAITS()`.
- The enhanced refresh REST API adds async operation, table/partition scoping, batched commits and cancellation — the right tool for large or complex models.
- Refresh schedules die silently after 4 consecutive failures and stay dead until manually re-enabled.
- `sempy` brings refresh orchestration and status polling natively into notebooks, alongside the portal, Monitor hub and workspace lineage.
- Activator is Fabric's no-code event-detection engine: events feed objects, rules evaluate stateless or stateful conditions per object, actions fire on match.
- Alert authoring is decentralized — Eventstream, KQL querysets, Real-Time dashboards, Power BI visuals and Real-Time hub all offer in-context "Set Alert," all backed by an Activator item.
- Pipeline-failure alerting has two valid patterns — per-pipeline job events, or workspace-wide via workspace monitoring's KQL logs — different maintenance tradeoffs, not right/wrong.
- The Capacity Metrics app and SQL analytics endpoints are explicitly unsupported alert sources; capacity alerting routes through Real-Time hub's capacity overview events.
- Documented rate limits (email, Teams, Power Automate, Fabric item activations) are the real answer to "why did alerts stop" during high event volume.

---

## Scenario Questions

> Attempt all of them before opening any toggle. Answers are hidden until you click.

### Q1. The vanishing run history

Northwind Logistics runs a nightly ingestion pipeline that has been in production for 8 months. A compliance auditor asks the data engineering team to produce the run status and duration of that pipeline for every night in the last quarter. The team opens Monitor hub, filters by Item type = Pipeline, and can only produce results going back a few weeks. Workspace monitoring has never been enabled on this workspace.

**What is the correct explanation and the correct remediation for future audits?**

- **A.** Monitor hub only stores runs for items you personally submitted; the auditor needs the pipeline owner to export the history instead.
- **B.** Monitor hub's main Activities view caps at 100 activities across the last 30 days and per-item Historical runs also only go back 30 days; enable workspace monitoring so pipeline events land in the `ItemJobEventLogs` table in a KQL database for long-term querying.
- **C.** Monitor hub retains 6 months of history but the keyword search only searches loaded rows; scroll and reload to reach older runs.
- **D.** Pipeline history is stored in the Capacity Metrics app's Compute page, which retains 14 days; install the app and increase its retention setting.

<details>
<summary>👉 Show answer</summary>

**Answer: B**

**Why it is right:** Monitor hub's Activities table caps at **100 activities from the past 30 days**, and even the per-activity **Historical runs** drill-down is limited to **30 days**. It is explicitly not a durable audit log. The documented way past that cap is **workspace monitoring** (Workspace settings → Monitoring → add a monitoring Eventhouse → Log workspace activity), which writes pipeline-level events into the **`ItemJobEventLogs`** table in an auto-created KQL database, queryable with KQL.

**Why the others are wrong:**
- **A** — Monitor hub shows activities for every item you have permission to view, not only ones you submitted; ownership is not the constraint, retention is.
- **C** — 6 months is Dataflow Gen2 refresh-history retention, not Monitor hub's. The keyword-search-scoped-to-loaded-rows detail is real but does not extend retention.
- **D** — The Capacity Metrics app reports CU utilization and throttling, not per-pipeline run status, its Compute window is fixed at 14 days, and it is admin-scoped.

**Covered in:** §1.1 The Monitor hub, §1.2 Pipeline run and activity monitoring

</details>

### Q2. The dataflow that fails in a specific table

Fabrikam's finance team owns a Dataflow Gen2 that loads 11 entities into a lakehouse each morning. Today the workspace list shows a red exclamation icon next to the dataflow. The team hovers the icon and confirms it is a refresh failure, not a validation failure, but the run summary only says the refresh failed. They need to know which entity failed, how many rows were read before it stopped, and they want to hand the raw engine logs to Microsoft support. The dataflow refreshes through an on-premises data gateway.

**Which sequence gets them there, and what prerequisite must already be satisfied?**

- **A.** Open Monitor hub → **View details** on the dataflow activity; the details pane carries per-table row counts and downloadable mashup-engine logs by default, with no prerequisite.
- **B.** Open the Capacity Metrics app's **Timepoint item detail** page and filter by operation ID to see which table consumed compute before failing; the prerequisite is capacity admin rights.
- **C.** Open **Recent runs** → the failed run → the **Tables** section only; detailed logs are never available for gateway-based refreshes under any configuration.
- **D.** Open **Recent runs** → select the run's **Start time** → inspect the **Tables** and **Activities** sections, then select **Download detailed logs**; the prerequisite is **Admin consent for gateway diagnostics** at both tenant and gateway level.

<details>
<summary>👉 Show answer</summary>

**Answer: D**

**Why it is right:** Drilling into a Dataflow Gen2 run by selecting its **Start time** exposes the **Tables** section (every entity with loading enabled, each drillable) and the **Activities** section (endpoints contacted, bytes/rows read and written). **Download detailed logs** yields the zipped mashup-engine logs, available a few minutes after the refresh and for **28 days**; it requires at least workspace **Viewer**, and for gateway-based refreshes **Admin consent for gateway diagnostics** must be enabled at **both** tenant and gateway level.

**Why the others are wrong:**
- **A** — Monitor hub's details pane gives status, timing and an error summary only, not per-table row counts or mashup logs.
- **B** — Capacity Metrics is capacity resource consumption (CU, throttling, storage), not dataflow entity-level diagnostics.
- **C** — Detailed logs *are* available for gateway-based refreshes; they just require the Admin consent for gateway diagnostics setting first.

**Covered in:** §1.3 Dataflow Gen2 refresh history and diagnostics

</details>

### Q3. Slow everywhere, failing nowhere

Contoso Retail runs 6 workspaces on a single F64 capacity. Between 09:00 and 11:00 each weekday, analysts report notebooks taking longer to start, dashboards feeling sluggish, and dataflow refreshes stretching out. Monitor hub shows no failed runs anywhere in the tenant. The capacity administrator wants to identify precisely which operations are consuming compute during the worst minute of the morning peak.

**Which surface and page should the administrator open?**

- **A.** The Capacity Metrics app — the **Compute** page for the 14-day utilization trend, then the **Timepoint** page to drill into a specific 30-second window and see which operations consumed the most compute.
- **B.** Monitor hub, filtered by Start time, sorted by duration, since it covers 17 item types and will show the slowest jobs.
- **C.** Workspace monitoring's `ItemJobEventLogs` table, since it is the only KQL-queryable source of capacity consumption data.
- **D.** The Spark application detail page's **Resources** tab for each notebook, since the executor utilization graph is the authoritative capacity-consumption view.

<details>
<summary>👉 Show answer</summary>

**Answer: A**

**Why it is right:** Slowness across many items with **no individual failures** is the classic capacity-throttling signature. The Capacity Metrics app is the only capacity-wide, admin-scoped surface: the **Compute** page gives 14 days of utilization trends and a matrix of operations by item, and the **Timepoint** page drills into a specific **30-second window** to show which operations consumed the most compute (with Timepoint summary and Timepoint item detail for further breakdown).

**Why the others are wrong:**
- **B** — Monitor hub is item-status-centric; it will not surface capacity-wide CU consumption or throttling, and nothing has failed.
- **C** — `ItemJobEventLogs` carries job-level events from workspace monitoring, not capacity CU/throttling metrics.
- **D** — The Resources tab shows one Spark application's executor allocation, not the whole capacity's contention across dataflows, dashboards and notebooks.

**Covered in:** §1.8 Capacity Metrics app, §1.10 Decision guidance

</details>

### Q4. The report that will not move

Adventure Works loads 40 million new rows into a lakehouse Delta table each night via a Spark notebook inside a pipeline. A Direct Lake semantic model on a SQL analytics endpoint sits on top, feeding an executive report. For three consecutive mornings, Monitor hub has shown the notebook activity **Succeeded**, no errors appear in the pipeline run or the semantic model's refresh history, yet the report shows the same three-day-old figures. Automatic updates were switched off two weeks ago during an ETL redesign and never switched back on.

**Which explanation and remediation is correct?**

- **A.** The Delta transaction log is corrupted; run `RESTORE` on the table to a previous version, then reload.
- **B.** Direct Lake has been disabled tenant-wide; convert the semantic model to Import mode so the report refreshes on a schedule.
- **C.** A Direct Lake model reflects data only as of its **last successful framing operation** — with automatic updates disabled and no manual/scheduled/programmatic refresh since, the newly written Parquet files remain invisible. Confirm the write landed via the Spark application's **Data** tab, confirm framing has not run via the refresh history's **Direct Lake** tab, then trigger a refresh (or re-enable automatic updates) and add an Activator alert for recurrence.
- **D.** The notebook's success status guarantees model freshness, so the fault must be capacity throttling; check the Capacity Metrics app's Compute page.

<details>
<summary>👉 Show answer</summary>

**Answer: C**

**Why it is right:** Framing is what makes new Delta data visible to a Direct Lake model. From the moment framing completes, all queries see the Delta tables exactly as they stood at that operation; **new Parquet files written after framing are invisible until the next framing**. With **automatic updates** (normally on by default) disabled and no manual, scheduled or programmatic refresh, the model stays frozen. The Spark application **Data** tab confirms the write actually landed; the refresh history's **Direct Lake tab** lists Direct Lake framing failures and shows whether framing has run; Activator turns "reframing hasn't run" into a proactive alert.

**Why the others are wrong:**
- **A** — `RESTORE` targets data corruption, which would surface errors somewhere; here every surface reports success.
- **B** — Notebook success does not imply model freshness, and switching to Import is a large architectural change that does not address the actual cause.
- **D** — Capacity throttling produces broad slowness across many items, not one report frozen at a specific point in time with no other complaints.

**Covered in:** §2.2 What framing means and when reframing happens, §1.4 Spark application monitoring, §2.4 Refresh history and failure diagnosis

</details>

### Q5. The schedule that stopped itself

Tailspin Traders' Import-mode semantic model refreshes nightly on a Fabric capacity. On Monday the source database's service principal secret expired. On Friday an analyst notices the model has not refreshed since Monday and that no one disabled the schedule. Separately, the team wants to shorten the nightly window by refreshing only the two fact partitions that actually change, and to be able to cancel a runaway refresh without waiting for it.

**Which combination correctly explains the stoppage and addresses both requirements?**

- **A.** Fabric capacity licensing allows only 8 scheduled refreshes/day, so the schedule exhausted its quota; upgrade to PPU for 48/day and use `applyRefreshPolicy` to reduce runtime.
- **B.** Four consecutive refresh failures automatically deactivated the schedule (and an unrecoverable credential error can deactivate it immediately); fix the credential and **manually re-enable** the schedule, then use the **enhanced refresh API** with the `objects` array for partition-scoped refresh and `DELETE /refreshes/{requestId}` to cancel in-progress enhanced refreshes.
- **C.** Disabling automatic updates paused the schedule; re-enable automatic updates, then use `commitMode: transactional` to allow cancellation.
- **D.** The model fell back to DirectQuery, which does not support scheduled refresh; run `EVALUATE TABLETRAITS()` and materialize the offending view, then use the classic Refresh Dataset API.

<details>
<summary>👉 Show answer</summary>

**Answer: B**

**Why it is right:** Power BI/Fabric **automatically deactivates a refresh schedule after 4 consecutive failures**, or immediately on an unrecoverable configuration error such as invalid/expired credentials — and it **does not reactivate itself** once the root cause is fixed; someone must manually re-enable it. The **enhanced refresh API's `objects` array** performs table- and partition-level refresh, and **`DELETE /refreshes/{requestId}`** cancels an in-progress refresh — but only one that was started via enhanced refresh, not a portal-triggered one.

**Why the others are wrong:**
- **A** — 8/day is the **Power BI Pro** limit; PPU and Premium/Fabric capacity both allow 48/day, and this model is already on capacity. A quota is not what stopped it.
- **C** — **Automatic updates** is a **Direct Lake** framing concept and has no effect on an Import-mode schedule; `commitMode` controls batched commits (`transactional` / `partialBatch`), not cancellation.
- **D** — DirectQuery fallback applies to **Direct Lake on SQL endpoints**, not Import mode, and the classic Refresh Dataset API ties up a long-running connection with no partition scoping or cancellation.

**Covered in:** §2.6 Scheduled refresh configuration, §2.4 Refresh history and failure diagnosis, §2.5 The enhanced refresh API

</details>

### Q6. Choosing supported alert sources (Choose 2)

Woodgrove Bank wants two new alerts. First, the platform team must be notified when the shared F128 capacity approaches throttling. Second, the risk team must be notified when a scheduled KQL query over an Eventhouse returns any rows breaching an exposure threshold. A junior engineer has proposed several configurations and asks which are actually supported by Activator.

**Which two configurations are supported? (Choose 2)**

- **A.** Set an alert directly inside the Microsoft Fabric Capacity Metrics app on the Compute page's utilization visual.
- **B.** Set an alert on **Fabric capacity overview events** from Real-Time hub, targeting a new or existing Activator item.
- **C.** Set an alert directly on the lakehouse's SQL analytics endpoint so any query result can trigger a rule.
- **D.** Set an alert on a Power BI report visual that uses Dynamic M parameters to scope the exposure threshold.
- **E.** Run the KQL queryset, select **Set Alert** in the results ribbon, and configure the condition and check frequency in the **Add Rule** pane.

<details>
<summary>👉 Show answer</summary>

**Answer: B and E**

**Why it is right:** **Fabric capacity overview events** in Real-Time hub are the documented route for capacity alerting, precisely because the Capacity Metrics app cannot alert. **KQL querysets** are a first-class Activator source — run the query, choose **Set Alert** on the results ribbon, and configure frequency (default check frequency 5 minutes) and condition in the **Add Rule** pane.

**Why the others are wrong:**
- **A** — Alerts from the **Fabric or Power BI Capacity Metrics app** are explicitly **not supported**.
- **C** — Alerts from a **SQL analytics endpoint** directly are **not currently supported**; warehouse alerting goes through the Fabric Data Warehouse SQL query results mechanism (preview).
- **D** — Alerts from **Dynamic M parameters** in a report are explicitly not supported, and the visual must also be on Activator's supported visual list.

**Covered in:** §3.2 Alert sources, §3.6 Limits and licensing notes, §1.8 Capacity Metrics app

</details>

### Q7. Standing up pipeline-failure alerting (ordering)

Litware Health wants to be paged in Teams the moment its nightly `dw-load-claims` pipeline reports a failed run, and it also wants a remediation pipeline kicked off automatically on the same condition. The lead engineer has never configured a Fabric alert before and has written the four steps of the canonical pattern on sticky notes, out of order:

1. Configure the actions — Teams message to the on-call owner, plus run the remediation pipeline
2. Select the `dw-load-claims` pipeline and the event of interest (run failed)
3. Choose the workspace and Activator item to save the rule into — create a new one or add to an existing one
4. From **Real-Time hub**, select **Fabric events → Job events**

**Put the four steps into the correct order.**

- **A.** 2 → 4 → 1 → 3
- **B.** 4 → 3 → 2 → 1
- **C.** 3 → 4 → 2 → 1
- **D.** 4 → 2 → 3 → 1

<details>
<summary>👉 Show answer</summary>

**Answer: D** (4 → 2 → 3 → 1)

**Why it is right:** The canonical alert-on-pipeline-failure pattern runs source → subject → home → action. **Step 4** first: from Real-Time hub select **Fabric events → Job events** (or start from an Activator item's **Get Data** tile and pick Job events) — pipeline runs triggered by schedules or manual triggers surface as **job events**. **Step 2** next: pick the specific pipeline and the event of interest. **Step 3**: choose the workspace/Activator item the rule is saved into, creating a new item or adding to an existing one. **Step 1** last: configure the action — typically email or Teams to the on-call owner, and optionally a Fabric-item action to trigger a remediation pipeline. A single rule can carry more than one action, which is what satisfies both requirements here.

**Why the others are wrong:**
- **A** — You cannot select a specific pipeline and event (2) before opening the Job events source (4); the Job events catalogue is what exposes the list of pipelines to pick from. It also configures the action (1) before the rule has an Activator item to live in (3).
- **B** — Inverts 3 and 2: the Activator item that stores the rule is chosen *after* the pipeline and event have been selected, not immediately after opening Job events.
- **C** — Starts by choosing the Activator item (3) with no source selected yet. Even the alternative entry point — starting inside an existing Activator item — still requires picking **Job events** on the **Get Data** tile before any pipeline can be selected.

**Covered in:** §3.5 Alert-on-pipeline-failure pattern, §3.4 Setting alerts from Monitor hub and RTI surfaces

</details>

### Q8. Which one will fail?

Proseware is preparing to promote a workspace from Dev to Test using a Fabric deployment pipeline. The workspace contains an Activator item with several rules, a Direct Lake on OneLake semantic model, a Copy job, and a Dataflow Gen2. The Activator item currently holds four rules: one triggered by Azure Blob Storage events that emails the data team; one on an eventstream that runs a Fabric notebook; one on Fabric job events that posts to a shared Teams channel; and one on a KQL queryset that emails an external auditor at a partner company.

**Which of the following will FAIL or silently not work?**

- **A.** Both the Azure Blob Storage-events rule and the email to the external auditor at the partner company.
- **B.** The eventstream rule that runs a Fabric notebook, because notebooks are not on Activator's supported action list.
- **C.** The Fabric job events rule posting to a shared Teams channel, because Activator supports private channels only.
- **D.** The Direct Lake on OneLake semantic model, because it will fall back to DirectQuery during deployment.

<details>
<summary>👉 Show answer</summary>

**Answer: A**

**Why it is right:** Two documented limitations bite here. First, **lifecycle management (deployment pipelines, Git integration) does not support Activator items that use Azure Blob Storage events, Power BI, or User Data Functions as a source/action** — including such an item in a deployment pipeline or Git-integrated workspace produces a deploy/commit error. Second, **email recipients must be internal addresses on the creator's verified Entra tenant domains** — external and guest addresses are not supported, so the auditor never receives the alert.

**Why the others are wrong:**
- **B** — Running a **notebook** is explicitly a supported Activator action, alongside pipeline, Spark job definition, dataflow, User Data Function and copy job (preview).
- **C** — It is the reverse: Activator supports **shared channels only, not private channels**, so posting to a shared channel is fine (group chats must additionally be recently active to appear as a target).
- **D** — **Direct Lake on OneLake never falls back** to DirectQuery; it runs exclusively in `DirectLakeOnly` mode. Fallback is a Direct Lake on SQL endpoints behaviour.

**Covered in:** §3.6 Limits and licensing notes, §3.3 Actions, §2.3 DirectQuery fallback

</details>

