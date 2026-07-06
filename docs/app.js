const STORAGE_KEY = "integration-system-demo-v1";

const apiCatalog = {
  "/api/po": {
    label: "Purchase Order",
    eventType: "PO",
    description: "High-value purchase order event with rich payload, duplicate detection, and downstream notifications.",
    payload: {
      eventType: "PO",
      orderId: "PO-00041",
      customerId: "CUST-108",
      region: "INDIA",
      item: "Laptop",
      quantity: 4,
      price: 50001,
      warehouse: "WH-1",
      timestamp: 1716240000000
    }
  },
  "/api/so": {
    label: "Sales Order",
    eventType: "SO",
    description: "Sales order flow with the same asynchronous pipeline and region routing behavior.",
    payload: {
      eventType: "SO",
      orderId: "SO-10017",
      customerId: "CUST-201",
      region: "US",
      item: "Software License",
      quantity: 15,
      price: 1200,
      timestamp: 1716240000000
    }
  },
  "/api/inventory": {
    label: "Inventory Update",
    eventType: "INVENTORY",
    description: "Inventory refresh event that travels through the same Kafka backbone.",
    payload: {
      eventType: "INVENTORY",
      sku: "SKU-5521",
      customerId: "CUST-311",
      region: "UK",
      stock: 240,
      warehouse: "WH-2",
      timestamp: 1716240000000
    }
  },
  "/api/checklist": {
    label: "Checklist",
    eventType: "CHECKLIST",
    description: "Checklist completion signal for workflow-driven integration runs.",
    payload: {
      eventType: "CHECKLIST",
      checklistId: "CHK-73",
      customerId: "CUST-108",
      region: "INDIA",
      status: "COMPLETE",
      timestamp: 1716240000000
    }
  },
  "/api/location": {
    label: "Location",
    eventType: "LOCATION",
    description: "Location payload showing region-based routing and region-service enrichment.",
    payload: {
      eventType: "LOCATION",
      locationId: "LOC-19",
      customerId: "CUST-459",
      region: "US",
      latitude: 37.7749,
      longitude: -122.4194,
      timestamp: 1716240000000
    }
  },
  "/api/media": {
    label: "Media Upload",
    eventType: "MEDIA",
    description: "Multipart media ingestion endpoint with the same idempotent ACK contract.",
    payload: {
      upload: "multipart/form-data",
      file: "invoice.pdf",
      customerId: "CUST-777",
      region: "UK",
      timestamp: 1716240000000
    }
  }
};

const regions = ["INDIA", "US", "UK"];
const channels = ["EMAIL", "SMS", "WEBHOOK"];
const serviceFlow = [
  { id: "ingestion", name: "ingestion-service", topic: "raw-events", label: "HTTP ingress", detail: "Accepts API calls, validates ids, and publishes an ACK." },
  { id: "kafka", name: "Kafka", topic: "raw-events", label: "event backbone", detail: "Partitions and buffers messages for async fan-out." },
  { id: "processing", name: "processing-service", topic: "region-*", label: "routing + state", detail: "Consumes raw events, dedupes, and routes by region." },
  { id: "regional", name: "regional-service", topic: "notification-events", label: "regional workflow", detail: "Executes region-specific work and publishes notification events." },
  { id: "notification", name: "notification-service", topic: "EMAIL/SMS/WEBHOOK", label: "fan-out delivery", detail: "Dispatches notifications and tracks delivery results." }
];

const scenarioMap = {
  happy: { label: "Happy path", retries: 0, terminal: "success", failureStage: null },
  retry: { label: "Retry then recover", retries: 2, terminal: "success", failureStage: "processing" },
  dlq: { label: "DLQ", retries: 3, terminal: "dlq", failureStage: "notification" }
};

const state = {
  api: "/api/po",
  region: "INDIA",
  customerId: "CUST-108",
  eventId: "evt-00041",
  requestId: "req-00041",
  duplicate: true,
  scenario: "retry",
  fanOut: true,
  channelFilter: "all",
  seenEventIds: new Set(),
  metrics: {
    accepted: 0,
    duplicates: 0,
    processed: 0,
    regional: 0,
    notifications: 0,
    retries: 0,
    dlq: 0,
    fanOut: 0,
    avgLatency: 0
  },
  events: []
};

let isRunning = false;
let seq = 0;

const elements = {};

function loadState() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY));
    if (!saved) {
      return;
    }
    Object.assign(state, {
      api: saved.api ?? state.api,
      region: saved.region ?? state.region,
      customerId: saved.customerId ?? state.customerId,
      eventId: saved.eventId ?? state.eventId,
      requestId: saved.requestId ?? state.requestId,
      duplicate: saved.duplicate ?? state.duplicate,
      scenario: saved.scenario ?? state.scenario,
      fanOut: saved.fanOut ?? state.fanOut,
      channelFilter: saved.channelFilter ?? state.channelFilter
    });
    state.metrics = { ...state.metrics, ...(saved.metrics || {}) };
    state.events = Array.isArray(saved.events) ? saved.events.slice(0, 8) : [];
    state.seenEventIds = new Set(saved.seenEventIds || []);
    seq = saved.seq || 0;
  } catch {
    return;
  }
}

function saveState() {
  const payload = {
    api: state.api,
    region: state.region,
    customerId: state.customerId,
    eventId: state.eventId,
    requestId: state.requestId,
    duplicate: state.duplicate,
    scenario: state.scenario,
    fanOut: state.fanOut,
    channelFilter: state.channelFilter,
    metrics: state.metrics,
    events: state.events.slice(0, 8),
    seenEventIds: Array.from(state.seenEventIds),
    seq
  };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
}

function $(id) {
  return elements[id];
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function formatTime(ts = Date.now()) {
  return new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(ts);
}

function formatJson(value) {
  return JSON.stringify(value, null, 2);
}

function endpointPreview() {
  const api = apiCatalog[state.api];
  const query = new URLSearchParams({
    id: state.requestId,
    customerId: state.customerId,
    region: state.region
  });
  return {
    url: `POST ${state.api}?${query.toString()}`,
    eventType: api.eventType,
    description: api.description,
    body: api.payload
  };
}

function renderControls() {
  document.querySelectorAll("[data-api]").forEach((button) => {
    button.classList.toggle("active", button.dataset.api === state.api);
  });
  document.querySelectorAll("[data-region]").forEach((button) => {
    button.classList.toggle("active", button.dataset.region === state.region);
  });

  $("customerId").value = state.customerId;
  $("eventId").value = state.eventId;
  $("requestId").value = state.requestId;
  $("duplicateToggle").checked = state.duplicate;
  $("fanoutToggle").checked = state.fanOut;
  $("scenario").value = state.scenario;
  $("channelFilter").value = state.channelFilter;

  const api = apiCatalog[state.api];
  $("apiDescription").textContent = api.description;
  $("requestPreview").textContent = endpointPreview().url;
  $("payloadPreview").textContent = state.api === "/api/media"
    ? formatJson({
        formData: {
          file: "invoice.pdf",
          id: state.requestId,
          customerId: state.customerId,
          region: state.region
        }
      })
    : formatJson(endpointPreview().body);

  $("responsePreview").textContent = formatJson(buildAck(false, false));
}

function renderMetrics() {
  $("metricAccepted").textContent = state.metrics.accepted;
  $("metricDuplicates").textContent = state.metrics.duplicates;
  $("metricProcessed").textContent = state.metrics.processed;
  $("metricRetries").textContent = state.metrics.retries;
  $("metricDlq").textContent = state.metrics.dlq;
  $("metricFanOut").textContent = state.metrics.fanOut;
  $("metricLatency").textContent = `${state.metrics.avgLatency.toFixed(0)} ms`;
  $("metricLive").textContent = `${Math.max(0, 1 + state.events.filter((event) => event.status === "active").length)}`;
}

function renderStages(activeStage = null, outcome = "idle") {
  document.querySelectorAll("[data-stage]").forEach((card, index) => {
    card.classList.remove("active", "success", "warning", "danger");
    if (activeStage !== null && index < activeStage) {
      card.classList.add("success");
    }
    if (activeStage === index) {
      card.classList.add("active");
    }
    if (outcome === "warning" && index === activeStage) {
      card.classList.add("warning");
    }
    if (outcome === "danger" && index === activeStage) {
      card.classList.add("danger");
    }
  });
}

function renderTimeline() {
  const list = $("timeline");
  if (!state.events.length) {
    list.innerHTML = `<div class="timeline-item"><div class="timeline-time">Ready</div><div><div class="timeline-title"><strong>No events yet</strong><span class="tag">live simulator</span></div><p>Submit a request to watch the event move through the ingestion, Kafka, processing, regional, and notification stages.</p></div></div>`;
    return;
  }

  list.innerHTML = state.events
    .map((event) => `
      <article class="timeline-item">
        <div class="timeline-time">${event.time}</div>
        <div>
          <div class="timeline-title">
            <strong>${event.title}</strong>
            <span class="tag">${event.tag}</span>
          </div>
          <p>${event.detail}</p>
        </div>
      </article>
    `)
    .join("");
}

function renderEventList() {
  const list = $("eventList");
  if (!state.events.length) {
    list.innerHTML = `<div class="subtle">The latest request will appear here, including duplicate detection, retry hops, and final delivery state.</div>`;
    return;
  }

  list.innerHTML = state.events
    .slice(0, 6)
    .map((event) => `
      <div class="timeline-item">
        <div class="timeline-time">${event.time}</div>
        <div>
          <div class="timeline-title">
            <strong>${event.title}</strong>
            <span class="tag">${event.status}</span>
          </div>
          <p>${event.detail}</p>
        </div>
      </div>
    `)
    .join("");
}

function renderAll(activeStage = null, outcome = "idle") {
  renderControls();
  renderMetrics();
  renderStages(activeStage, outcome);
  renderTimeline();
  renderEventList();
  saveState();
}

function createEventSummary(title, detail, tag, status = "ok") {
  state.events.unshift({
    time: formatTime(),
    title,
    detail,
    tag,
    status
  });
  state.events = state.events.slice(0, 8);
}

function buildAck(duplicate, dlq) {
  const base = {
    status: duplicate ? "DUPLICATE" : dlq ? "DLQ" : "ACCEPTED",
    message: duplicate
      ? "Duplicate request ignored"
      : dlq
        ? "Message routed to dead-letter queue"
        : "Event queued for asynchronous processing",
    eventId: state.eventId,
    correlationId: `corr-${state.eventId}`,
    topic: "raw-events"
  };

  if (!duplicate && !dlq) {
    return base;
  }

  if (dlq) {
    return {
      ...base,
      retryCount: scenarioMap[state.scenario].retries,
      sourceTopic: "notification-events"
    };
  }

  return base;
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function setScenario(value) {
  state.scenario = value;
  $("scenario").value = value;
  saveState();
}

function setRegion(value) {
  state.region = value;
  saveState();
}

function setApi(value) {
  state.api = value;
  renderControls();
  saveState();
}

function useSampleSeed() {
  const sample = apiCatalog[state.api].payload;
  const derivedCustomer = sample.customerId || state.customerId;
  state.customerId = derivedCustomer;
  state.eventId = state.eventId || `evt-${String(++seq).padStart(5, "0")}`;
  state.requestId = state.requestId || `req-${String(seq).padStart(5, "0")}`;
  saveState();
}

async function runSimulation() {
  if (isRunning) {
    return;
  }

  isRunning = true;
  $("runButton").disabled = true;
  $("runButton").textContent = "Simulating...";

  const currentScenario = scenarioMap[state.scenario];
  const endpoint = endpointPreview();
  const duplicate = state.duplicate && state.seenEventIds.has(state.eventId);
  const fanOutChannels = state.fanOut
    ? (state.channelFilter === "all" ? channels : [state.channelFilter])
    : [state.channelFilter === "all" ? "EMAIL" : state.channelFilter];

  state.seenEventIds.add(state.eventId);
  state.metrics.accepted += 1;

  if (duplicate) {
    state.metrics.duplicates += 1;
    $("responsePreview").textContent = formatJson(buildAck(true, false));
    createEventSummary("Ingestion duplicate", `Duplicate ` + state.eventId + " ignored at ingestion-service. Ack returned 202 DUPLICATE for " + endpoint.url + ".", "ingestion", "duplicate");
    renderAll(0, "warning");
    isRunning = false;
    $("runButton").disabled = false;
    $("runButton").textContent = "Send event";
    return;
  }

  const startedAt = performance.now();
  let latencyTotal = 0;

  const steps = [
    {
      title: "ingestion-service accepted request",
      detail: `${endpoint.url} returned 202 Accepted with correlation id corr-${state.eventId}.`,
      tag: "raw request"
    },
    {
      title: "Kafka published to raw-events",
      detail: `Message keyed by customerId ${state.customerId} entered the raw-events topic.`,
      tag: "producer"
    },
    {
      title: "processing-service routed event",
      detail: `Stage applied dedupe, normalized payload, and routed the event toward region-${state.region.toLowerCase()}.`,
      tag: "processing"
    },
    {
      title: "regional-service executed workflow",
      detail: `Regional workflow persisted state and published notification-events.`,
      tag: "regional"
    },
    {
      title: "notification-service fanned out delivery",
      detail: `Delivery fan-out targeted ${fanOutChannels.join(", ")} channels.`,
      tag: "delivery"
    }
  ];

  $("responsePreview").textContent = formatJson(buildAck(false, false));
  renderAll(0, "idle");

  for (let index = 0; index < steps.length; index += 1) {
    renderStages(index, "idle");
    const step = steps[index];
    const baseDelay = index === 0 ? 340 : 420;
    await sleep(baseDelay);
    latencyTotal += baseDelay;

    if (index === 2 && currentScenario.retries > 0) {
      for (let retry = 1; retry <= currentScenario.retries; retry += 1) {
        state.metrics.retries += 1;
        createEventSummary(
          `Retry ${retry} at processing-service`,
          `Message moved to retry-processing-${retry === 1 ? "5s" : retry === 2 ? "1m" : "10m"} after a simulated transient error.`,
          "retry",
          "warning"
        );
        renderAll(index, "warning");
        await sleep(280);
        latencyTotal += 280;
      }
    }

    if (index === 4 && currentScenario.terminal === "dlq") {
      state.metrics.dlq += 1;
      const dlqAck = buildAck(false, true);
      $("responsePreview").textContent = formatJson(dlqAck);
      createEventSummary(
        "DLQ emitted at notification-service",
        `After ${currentScenario.retries} retries, the event landed in dlq-notification with full exception context.`,
        "dlq",
        "danger"
      );
      renderAll(index, "danger");
      break;
    }

    if (index === 2) {
      state.metrics.processed += 1;
    }

    if (index === 3) {
      state.metrics.regional += 1;
    }

    if (index === 4) {
      state.metrics.notifications += 1;
      state.metrics.fanOut += fanOutChannels.length;
    }

    createEventSummary(step.title, step.detail, step.tag, "ok");
    renderAll(index + 1, index === 4 ? "success" : "idle");
  }

  const elapsed = performance.now() - startedAt;
  state.metrics.avgLatency = clamp((state.metrics.avgLatency * 0.7) + (elapsed * 0.3) + latencyTotal * 0.05, 42, 4200);

  if (state.duplicate) {
    await sleep(260);
    state.seenEventIds.add(state.eventId);
    state.metrics.duplicates += 1;
    createEventSummary(
      "Duplicate replay detected",
      `A second request reused eventId ${state.eventId} and was dropped at ingestion before Kafka publish.`,
      "duplicate",
      "warning"
    );
    renderAll(serviceFlow.length, "warning");
    $("responsePreview").textContent = formatJson(buildAck(true, false));
  }

  $("responsePreview").textContent = formatJson(currentScenario.terminal === "dlq" ? buildAck(false, true) : buildAck(false, false));
  createEventSummary(
    currentScenario.terminal === "dlq" ? "Flow completed with DLQ" : "Flow completed successfully",
    currentScenario.terminal === "dlq"
      ? "The pipeline preserved the payload, exception text, retry count, and source topic for triage."
      : `All stages completed and ${fanOutChannels.length} notification channel(s) were sent.`,
    currentScenario.terminal === "dlq" ? "dead-letter" : "complete",
    currentScenario.terminal === "dlq" ? "danger" : "ok"
  );

  renderAll(serviceFlow.length, currentScenario.terminal === "dlq" ? "danger" : "success");
  isRunning = false;
  $("runButton").disabled = false;
  $("runButton").textContent = "Send event";
}

function attachHandlers() {
  document.querySelectorAll("[data-api]").forEach((button) => {
    button.addEventListener("click", () => {
      setApi(button.dataset.api);
      renderAll();
    });
  });

  document.querySelectorAll("[data-region]").forEach((button) => {
    button.addEventListener("click", () => {
      state.region = button.dataset.region;
      renderAll();
    });
  });

  $("customerId").addEventListener("input", (event) => {
    state.customerId = event.target.value.trim() || "CUST-108";
    saveState();
    renderControls();
  });

  $("eventId").addEventListener("input", (event) => {
    state.eventId = event.target.value.trim() || "evt-00041";
    saveState();
    renderControls();
  });

  $("requestId").addEventListener("input", (event) => {
    state.requestId = event.target.value.trim() || "req-00041";
    saveState();
    renderControls();
  });

  $("duplicateToggle").addEventListener("change", (event) => {
    state.duplicate = event.target.checked;
    saveState();
  });

  $("fanoutToggle").addEventListener("change", (event) => {
    state.fanOut = event.target.checked;
    saveState();
    renderControls();
  });

  $("scenario").addEventListener("change", (event) => setScenario(event.target.value));
  $("channelFilter").addEventListener("change", (event) => {
    state.channelFilter = event.target.value;
    saveState();
    renderControls();
  });

  $("seedButton").addEventListener("click", () => {
    seq += 1;
    state.eventId = `evt-${String(seq).padStart(5, "0")}`;
    state.requestId = `req-${String(seq).padStart(5, "0")}`;
    state.customerId = `CUST-${String(100 + seq).padStart(3, "0")}`;
    renderControls();
  });

  $("resetButton").addEventListener("click", () => {
    state.metrics = {
      accepted: 0,
      duplicates: 0,
      processed: 0,
      regional: 0,
      notifications: 0,
      retries: 0,
      dlq: 0,
      fanOut: 0,
      avgLatency: 0
    };
    state.events = [];
    state.seenEventIds = new Set();
    seq = 0;
    renderAll(null, "idle");
  });

  $("runButton").addEventListener("click", () => {
    runSimulation().catch((error) => {
      console.error(error);
      isRunning = false;
      $("runButton").disabled = false;
      $("runButton").textContent = "Send event";
    });
  });
}

function initStageMarkup() {
  const stageGrid = $("stageGrid");
  stageGrid.innerHTML = serviceFlow
    .map((stage, index) => `
      <article class="stage-card" data-stage="${index}">
        <div class="index">Stage ${String(index + 1).padStart(2, "0")}</div>
        <h3>${stage.name}</h3>
        <p>${stage.detail}</p>
        <span class="pill dot">${stage.label}</span>
      </article>
    `)
    .join("");
}

function init() {
  Object.assign(elements, {
    apiDescription: document.getElementById("apiDescription"),
    requestPreview: document.getElementById("requestPreview"),
    payloadPreview: document.getElementById("payloadPreview"),
    responsePreview: document.getElementById("responsePreview"),
    timeline: document.getElementById("timeline"),
    eventList: document.getElementById("eventList"),
    stageGrid: document.getElementById("stageGrid"),
    runButton: document.getElementById("runButton"),
    seedButton: document.getElementById("seedButton"),
    resetButton: document.getElementById("resetButton"),
    customerId: document.getElementById("customerId"),
    eventId: document.getElementById("eventId"),
    requestId: document.getElementById("requestId"),
    duplicateToggle: document.getElementById("duplicateToggle"),
    fanoutToggle: document.getElementById("fanoutToggle"),
    scenario: document.getElementById("scenario"),
    channelFilter: document.getElementById("channelFilter"),
    metricAccepted: document.getElementById("metricAccepted"),
    metricDuplicates: document.getElementById("metricDuplicates"),
    metricProcessed: document.getElementById("metricProcessed"),
    metricRetries: document.getElementById("metricRetries"),
    metricDlq: document.getElementById("metricDlq"),
    metricFanOut: document.getElementById("metricFanOut"),
    metricLatency: document.getElementById("metricLatency"),
    metricLive: document.getElementById("metricLive")
  });

  loadState();
  initStageMarkup();
  attachHandlers();
  renderAll();
  useSampleSeed();
  renderAll();
}

document.addEventListener("DOMContentLoaded", init);
