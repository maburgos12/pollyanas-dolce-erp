(function (root) {
  "use strict";

  function isLegacyDelivery(item, payload) {
    return Boolean(
      item &&
      String(item.path || "").includes("/entrega/") &&
      payload &&
      typeof payload === "object" &&
      !String(payload.client_event_id || "").trim()
    );
  }

  function safeQueueId(value) {
    const normalized = String(value || "")
      .trim()
      .replace(/[^A-Za-z0-9._:-]/g, "-")
      .slice(0, 60);
    return normalized || "legacy-sin-id";
  }

  function prepareReplay(item) {
    if (!item || item.body?.kind !== "text") return item;
    let payload;
    try {
      payload = JSON.parse(item.body.value || "{}");
    } catch (error) {
      return item;
    }
    if (!isLegacyDelivery(item, payload)) return item;

    const queueId = safeQueueId(item.id);
    payload.client_event_id = `offline-v59-${queueId}`;
    payload.client_context = {
      causa: "GPS_SIN_SENAL",
      client_timestamp: item.queued_at || new Date().toISOString(),
      client_version: "pwa-v59-offline"
    };
    payload.notas = String(payload.notas || "").trim() || "Confirmación recuperada de cola offline v59.";
    if (!Array.isArray(payload.evidencias)) payload.evidencias = [];

    return {
      ...item,
      headers: {
        ...(item.headers || {}),
        "X-Logistica-Offline-Queue-Id": queueId
      },
      body: { ...item.body, value: JSON.stringify(payload) }
    };
  }

  root.PDLogisticaOfflineQueue = Object.freeze({ prepareReplay });
})(typeof globalThis !== "undefined" ? globalThis : window);
