// Sentinel Person 3 benchmark: polling vs. RethinkDB changefeed
// Requirements: Node.js 18+, running Sentinel server, RethinkDB running.
// Run from project root: node scripts/benchmark.js
//
// This test creates one uniquely tagged incident per run. It measures
// detection latency from the POST request start until each monitor observes it.
// It measures monitoring HTTP requests separately from the event-ingestion POST.

const BASE_URL = process.env.SENTINEL_URL || "http://localhost:3000";
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 1000);
const TEST_TIMEOUT_MS = Number(process.env.TEST_TIMEOUT_MS || 15000);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForHealth() {
  const response = await fetch(`${BASE_URL}/api/health`);
  if (!response.ok) throw new Error(`Health endpoint returned HTTP ${response.status}`);
}

async function waitForPollingDetection(marker, stopSignal) {
  let requests = 0;
  const started = performance.now();

  while (!stopSignal.stopped && performance.now() - started < TEST_TIMEOUT_MS) {
    requests++;
    try {
      const response = await fetch(`${BASE_URL}/api/incidents`);
      if (response.ok) {
        const incidents = await response.json();
        if (JSON.stringify(incidents).includes(marker)) {
          return { latencyMs: performance.now() - started, requests };
        }
      }
    } catch (error) {
      // Transient polling errors are surfaced if the test times out.
    }
    await sleep(POLL_INTERVAL_MS);
  }
  return { latencyMs: null, requests, timedOut: true };
}

async function startChangefeed(marker) {
  const controller = new AbortController();
  const state = { requests: 1, latencyMs: null, error: null };
  const started = performance.now();

  const ready = (async () => {
    try {
      const response = await fetch(`${BASE_URL}/api/incidents/stream`, {
        headers: { Accept: "text/event-stream" },
        signal: controller.signal,
      });
      if (!response.ok) throw new Error(`Stream returned HTTP ${response.status}`);
      if (!response.body) throw new Error("Stream response has no body");

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const blocks = buffer.split(/\r?\n\r?\n/);
        buffer = blocks.pop() || "";

        for (const block of blocks) {
          const data = block.split(/\r?\n/)
            .filter((line) => line.startsWith("data:"))
            .map((line) => line.slice(5).trim())
            .join("\n");
          if (data && data.includes(marker)) {
            state.latencyMs = performance.now() - started;
            controller.abort();
            return;
          }
        }
      }
    } catch (error) {
      if (error.name !== "AbortError") state.error = error.message;
    }
  })();

  // Give the SSE connection a moment to establish before the test event.
  await sleep(500);
  return {
    state,
    stop: () => controller.abort(),
    ready,
  };
}

async function postTestIncident(marker) {
  const response = await fetch(`${BASE_URL}/api/events`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      service: `benchmark-${Date.now()}`,
      type: "benchmark_detection_test",
      severity: "LOW",
      message: marker,
    }),
  });

  const body = await response.text();
  if (!response.ok) {
    throw new Error(`POST /api/events returned HTTP ${response.status}: ${body}`);
  }
}

async function main() {
  await waitForHealth();
  console.log(`Sentinel benchmark target: ${BASE_URL}`);
  console.log(`Polling interval: ${POLL_INTERVAL_MS} ms; timeout: ${TEST_TIMEOUT_MS} ms`);
  console.log("Each run creates a test incident with a unique benchmark marker.\n");

  // Run the changefeed and polling monitors together for a comparable trigger.
  const marker = `benchmark-marker-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const stopSignal = { stopped: false };
  const changefeed = await startChangefeed(marker);
  const pollingPromise = waitForPollingDetection(marker, stopSignal);

  // Start the clock as close as possible to the ingestion request.
  const eventStart = performance.now();
  await postTestIncident(marker);
  const postCompleted = performance.now();
  const polling = await pollingPromise;

  // Allow a short grace period for the stream delivery to arrive.
  const deadline = performance.now() + 2000;
  while (changefeed.state.latencyMs === null && !changefeed.state.error && performance.now() < deadline) {
    await sleep(25);
  }
  stopSignal.stopped = true;
  changefeed.stop();

  console.log("Results (latency is measured from monitor start, just before the incident POST):");
  console.log(`Polling:    latency=${polling.latencyMs === null ? "TIMEOUT" : `${polling.latencyMs.toFixed(1)} ms`}, GET requests=${polling.requests}`);
  console.log(`Changefeed: latency=${changefeed.state.latencyMs === null ? "NOT DETECTED" : `${changefeed.state.latencyMs.toFixed(1)} ms`}, stream HTTP requests=${changefeed.state.requests}`);
  console.log(`Incident POST duration: ${(postCompleted - eventStart).toFixed(1)} ms (not included in monitor request counts)`);

  if (changefeed.state.error) console.log(`Changefeed error: ${changefeed.state.error}`);
  if (polling.timedOut) console.log("Polling did not detect the test marker before timeout.");
  if (changefeed.state.latencyMs === null) {
    console.log("NOTE: If the stream did not detect the marker, inspect the SSE payload format in src/server.js and adapt the marker check.");
  }

  console.log("\nRepeat this command at least 5 times and record each run. Request counts are for this test's monitor only, not total server traffic.");
}

main().catch((error) => {
  console.error(`Benchmark failed: ${error.message}`);
  process.exitCode = 1;
});
