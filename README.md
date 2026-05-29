# Kinesis + Datadog Data Streams Monitoring (DSM) Demo

A minimal Python producer and consumer for Amazon Kinesis Data Streams,
instrumented with Datadog Data Streams Monitoring to show real-time pipeline
topology, consumer lag, and end-to-end latency in the Datadog DSM view.

This is the first of two related projects. Once the Kinesis stream and DSM
pipeline are confirmed working here, continue to
[datadog-postgres-dbm-demo](https://github.com/databased/datadog-postgres-dbm-demo)
to add PostgreSQL Database Monitoring on top of the same stream.

## Cost and Time to First Signal

**AWS:** Kinesis Data Streams is included in the
[AWS Free Tier](https://aws.amazon.com/kinesis/data-streams/pricing/).
The first 1 million `PutRecord` calls per month are free. This demo
publishes roughly 1 record per second -- a typical session stays well
within the free tier at no cost.

**Datadog:** A 14-day free trial is available at
[datadoghq.com](https://www.datadoghq.com/). No credit card required to start.

**Time to first DSM signal: approximately 30 minutes** from a fresh clone
following this README.

## What This Shows in Datadog

**Data Streams Monitoring (APM -> Data Streams):**
- Pipeline topology map: `kinesis-dsm-producer` -> Kinesis stream -> `kinesis-dsm-consumer`
- Consumer lag metric: builds in real time when `CONSUMER_LAG_SECONDS` is set
- Service map: producer and consumer as named, attributed services
- End-to-end latency per hop

**APM Traces (APM -> Services -- zero additional setup):**

Because both services start with `ddtrace-run`, Datadog APM captures spans
for every Kinesis API call automatically. Navigate to APM -> Services ->
`kinesis-dsm-consumer` -> Traces -> open any trace to see the flame graph:

```
[consume]  ~260ms total
  [poll_kinesis_shard]  ~30ms
  [write_to_db]  ~180ms   (in datadog-postgres-dbm-demo only)
```

No additional instrumentation code. `ddtrace-run` patches the Kinesis client
at interpreter startup before any user imports run.

**Integration breadth -- four signals from one instrumented Python app:**

| Integration | Signal | Setup required |
|---|---|---|
| AWS Kinesis | DSM topology, lag | 3 env vars |
| Python app | APM traces, flame graph | `ddtrace-run` (already in Makefile) |
| Datadog Agent | Host metrics | Agent install (see Prerequisites) |
| Python logs | Log-to-trace correlation | `DD_LOGS_INJECTION=true` (see below) |

## Prerequisites

- AWS account with credentials configured (`aws configure`)
- Python 3.9+

### Prerequisite 1: Datadog API Key

1. Log in at https://app.datadoghq.com
2. Bottom-left gear icon -> Organization Settings -> API Keys
3. Click **New Key**, name it (e.g. `kinesis-dsm-demo`), copy the value
4. Paste it into `.env` as `DD_API_KEY=<value>`

Check that `DD_SITE` in `.env` matches your account region:

| Region | DD_SITE value |
|--------|--------------|
| US (default) | `datadoghq.com` |
| EU | `datadoghq.eu` |
| US3 | `us3.datadoghq.com` |
| US5 | `us5.datadoghq.com` |

### Prerequisite 2: Datadog Agent (required for trace transport)

`dd-trace` sends spans to a locally running Datadog Agent on `localhost:8126`. The Agent forwards them to Datadog. Without the Agent running, all traces are silently dropped.

**Install on Ubuntu/Debian:**

Replace `<your-key>` and `<your-site>` with your actual values (e.g. `us5.datadoghq.com`):

```bash
DD_API_KEY=<your-key> DD_SITE="<your-site>" bash -c "$(curl -L https://install.datadoghq.com/scripts/install_script_agent7.sh)"
```

> **Critical:** Use your actual `DD_SITE` value here -- do NOT leave it as `datadoghq.com` if your account is on a different site (e.g. `us5.datadoghq.com`). The install script writes this value into `/etc/datadog-agent/datadog.yaml`. If it is wrong, all traces are silently forwarded to the wrong site and nothing appears in Datadog.

**Start and verify:**

```bash
sudo systemctl start datadog-agent
curl http://localhost:8126/info   # JSON with "version" = Agent trace endpoint is live
```

**If you already have the Agent installed** (e.g. switching to a new Datadog trial account), update `datadog.yaml` using shell commands. Do not use a text editor -- a single bad indent under `apm_config:` silently kills the trace-agent with no error message.

When switching to a new trial, update all three values in one pass:

```bash
# 1. Replace API key
sudo sed -i 's/^api_key:.*/api_key: <your-new-api-key>/' /etc/datadog-agent/datadog.yaml

# 2. Replace site
sudo sed -i 's/^site:.*/site: <your-site>/' /etc/datadog-agent/datadog.yaml

# 3. Rebuild the apm_config block cleanly (removes any corrupted version, appends a fresh one)
sudo sed -i '/^apm_config:/d' /etc/datadog-agent/datadog.yaml
sudo sed -i '/^  enabled:/d' /etc/datadog-agent/datadog.yaml
printf '\napm_config:\n  enabled: true\n' | sudo tee -a /etc/datadog-agent/datadog.yaml

# 4. Verify before restarting
sudo grep -E "^api_key:|^site:" /etc/datadog-agent/datadog.yaml
sudo tail -4 /etc/datadog-agent/datadog.yaml
```

The verify step should print:
```
api_key: <your-new-api-key>
site: <your-site>

apm_config:
  enabled: true
```

After confirming, restart and verify port 8126 is live:

```bash
sudo systemctl restart datadog-agent
sleep 10
curl http://localhost:8126/info   # must return JSON -- if "Connection refused", APM is not running
```

**If `curl http://localhost:8126/info` returns "Connection refused"** after restart, APM did not start. Diagnose with:

```bash
sudo datadog-agent configcheck 2>&1 | grep -i "error\|warn" | head -20
```

Fix any reported errors, restart, and re-run `curl http://localhost:8126/info` before starting the scripts.

The `DD_AGENT_HOST` variable in `.env` defaults to `localhost`, which is correct for a local setup. Change it only if the Agent runs on a different host.

### Prerequisite 3: Kinesis Enabled in Datadog AWS Integration

This step connects Datadog to your AWS account so it can pull Kinesis CloudWatch metrics. It is separate from DSM trace data but needed for the full observability picture.

1. In Datadog: **Integrations -> Amazon Web Services**
   (https://app.datadoghq.com/integrations/amazon-web-services)
2. If no AWS account is connected yet, click **Add AWS Account** and follow the guided IAM role setup. This creates a cross-account read role in your AWS account.
3. Once connected, open the integration and go to the **Metric Collection** tab.
4. Find **Kinesis AWS/Kinesis** in the service list and ensure the box is checked (Note: This covers standard Kinesis Data Streams and is usually enabled by default).
5. Click **Update Configuration** only if you made a change.

> DSM topology (producer -> stream -> consumer) comes from `dd-trace` via the Agent and does not require the AWS integration. The integration adds Kinesis CloudWatch metrics (shard count, iterator age, etc.) as a separate data source.

### Prerequisite 4: Datadog Agent Status Check (pre-flight)

Before running the producer or consumer, confirm the Agent is healthy:

```bash
curl http://localhost:8126/info
```

A JSON response with `"version"` confirms the Agent trace endpoint is reachable. If the command times out or returns a connection error, the Agent is not running.

## Quick Start

### 1. Deploy infrastructure

```bash
./scripts/deploy.sh
```

Copy the `StreamName` and `StreamArn` outputs into `.env`.

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your DD_API_KEY, StreamName, StreamArn
```

### 3. Create virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The `.venv` directory is listed in `.gitignore` and will not be committed.

> **Each new terminal needs the venv activated before running any Python commands:**
> ```bash
> source .venv/bin/activate
> ```
> Your prompt will show `(.venv)` when the venv is active.

### 4. Run producer and consumer in separate terminals

```bash
# Terminal 1 -- activate venv if not already active
source .venv/bin/activate
make run-producer

# Terminal 2 -- activate venv if not already active
source .venv/bin/activate
make run-consumer_dsm
```

> The Makefile targets set `DD_DATA_STREAMS_ENABLED=true`, `DD_TRACE_AGENT_URL=http://localhost:8126`,
> and `DD_SERVICE` in the shell environment before launching `ddtrace-run`. These variables must be
> present at interpreter startup -- they cannot be read from `.env` alone when using `ddtrace-run`.

### 5. View in Datadog

Navigate to: **APM -> Data Streams Monitoring**

There is no "Pipeline" sub-menu. Clicking "Data Streams Monitoring" in the APM sidebar takes you directly to the topology view.

> [!IMPORTANT]
> **New Trial Accounts:** If you are using a brand new Datadog organization, Data Streams Monitoring is dormant by default. You will see a "Get Started" splash screen. You **MUST** click the "Get Started" or "Try the Quick Start Guide" button on this page to instantiate the DSM trial on Datadog's backend. If you skip this, Datadog will silently drop all your Kinesis payloads!

Once past the splash screen, ensure the **Environment** dropdown at the top left of the UI is set to `demo` (the default `DD_ENV` used in this project). If it defaults to `prod` or `*`, the topology map may appear empty.

The topology map linking the producer to the consumer appears within 3-5 minutes of both services running simultaneously.

#### If you still see nothing after 5+ minutes

**Check 1: Agent is configured for the correct site, API key, and APM**

```bash
sudo grep -E "^api_key:|^site:|^apm_config:" /etc/datadog-agent/datadog.yaml
```

The `api_key:` must match the key in your `.env` and the `site:` must match `DD_SITE` in your `.env`. If any of these are wrong, use `sed` to fix them (do not use a text editor -- see Prerequisite 2 warning above):

```bash
sudo sed -i 's/^api_key:.*/api_key: <your-key>/' /etc/datadog-agent/datadog.yaml
sudo sed -i 's/^site:.*/site: <your-site>/' /etc/datadog-agent/datadog.yaml
sudo sed -i '/^apm_config:/d' /etc/datadog-agent/datadog.yaml
sudo sed -i '/^  enabled:/d' /etc/datadog-agent/datadog.yaml
printf '\napm_config:\n  enabled: true\n' | sudo tee -a /etc/datadog-agent/datadog.yaml
sudo systemctl restart datadog-agent
sleep 10
curl http://localhost:8126/info
```

**Check 2: Validate your API key reaches the correct account**

```bash
curl -s "https://api.<your-site>/api/v1/validate" \
  -H "DD-API-KEY: <your-key>"
# Expected: {"valid":true}
```

**Check 3: Do not use an Application Key**

API keys are plain 32-character hex strings with no prefix. If your key starts with `ddapp_`, you have an Application Key (wrong type). Go to Organization Settings -> **API Keys** (not Application Keys) and create a new key.

**Check 4: Confirm both scripts are running**

The topology requires both producer and consumer active at the same time. Confirm both terminals are running without errors before checking the DSM view.

## Demo: Simulate Consumer Lag

Stop the current consumer (Ctrl+C in Terminal 2), then restart with artificial lag:

```bash
source .venv/bin/activate
DD_DATA_STREAMS_ENABLED=true DD_TRACE_AGENT_URL=http://localhost:8126 \
  DD_SERVICE=kinesis-dsm-consumer CONSUMER_LAG_SECONDS=3 \
  ddtrace-run python kinesis/consumer.py
```

Watch the consumer lag metric build in the Datadog DSM view. The lag increases
at 3 seconds per record consumed -- with the producer publishing 1 record/second,
the consumer falls further behind in real time.

## Demo: APM Traces (Zero Additional Setup)

Navigate to **APM -> Services** -> `kinesis-dsm-consumer` -> **Traces**.

Open any trace. The flame graph shows exactly where time is spent in the consumer
loop -- how long each Kinesis `GetRecords` call took, how many records were processed,
and (in the DBM demo) how long the database write took.

Clicking a Kinesis span shows the full request context including stream name, shard,
and HTTP status. No instrumentation code was added to enable this -- `ddtrace-run`
patches the Kinesis client at interpreter startup.

## Demo: Log Correlation (5-Minute Setup)

Add `DD_LOGS_INJECTION=true` to your environment and restart the consumer:

```bash
DD_DATA_STREAMS_ENABLED=true DD_TRACE_AGENT_URL=http://localhost:8126 \
  DD_LOGS_INJECTION=true DD_SERVICE=kinesis-dsm-consumer \
  ddtrace-run python kinesis/consumer.py
```

Navigate to **Logs -> Live Tail** and filter by `service:kinesis-dsm-consumer`.
Click any log line. A **View trace** button appears alongside the log entry.
Clicking it opens the exact trace that produced that log line.

Log-to-trace correlation: one click, no timestamp hunting.

## Teardown

```bash
./scripts/teardown.sh
```

## Technical Notes

- `StreamARN` is required in all botocore Kinesis calls for DSM checkpointing to work.
  This is not prominent in Datadog docs but is required by the dd-trace botocore patch.
- `DD_DATA_STREAMS_ENABLED=true` must be in the shell environment when `ddtrace-run`
  launches -- it cannot be read from `.env` alone. The Makefile targets handle this.
- Requires ddtrace >= 2.8.0 for Kinesis DSM support.
- Do not use gzip-encoded records -- dd-trace-py warns on gzip Kinesis data.
- **"unnamed-python-service" in the DSM topology:** If a service labeled `unnamed-python-service` appears in the topology map, it came from a Python process that ran with `DD_DATA_STREAMS_ENABLED=true` but without `DD_SERVICE` set (e.g. a debug or test run). It is harmless and ages out of the topology within the time window selected in the UI. Always start producer and consumer with `DD_SERVICE=<name>` explicitly set (the run commands in Step 4 above include this).
- **New Datadog organizations:** DSM is dormant on a freshly created account until activated. If the DSM page shows "Get Started" after the Agent and scripts are fully configured and running, click the activation button on that splash screen. The pipeline data your Agent has already sent will appear within minutes after activation.

## What to Build Next

This project establishes the Kinesis stream and DSM pipeline. The Kinesis stream
remains running at the end of this demo and serves as the data source for the
next project.

**[datadog-postgres-dbm-demo](https://github.com/databased/datadog-postgres-dbm-demo)**
adds a PostgreSQL consumer that writes each Kinesis event to a database, then
demonstrates Datadog Database Monitoring (DBM):

- Query performance metrics and latency trends
- Explain plan capture: see `Seq Scan` on an unindexed query
- Live remediation: `CREATE INDEX` applied while everything is running, latency
  drops in real time, explain plan changes to `Index Scan`
- Two-tier observability: the same bad query visible in DBM metrics AND as a CPU
  spike in Infrastructure -> Containers

Because the Kinesis stream and Datadog Agent are already running at the end of
this demo, **time to first DBM query metric is approximately 5 minutes** -- just
deploy the Agent PostgreSQL config, start the consumer, and DBM shows the INSERT
workload immediately.

See [datadog-postgres-dbm-demo](https://github.com/databased/datadog-postgres-dbm-demo)
to continue.
