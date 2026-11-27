/* StayStream Dashboard Shared Helpers */

export const fmtINR = (v) =>
  "₹" + Number(v || 0).toLocaleString("en-IN", { maximumFractionDigits: 0 });

export const REASON_COLORS = {
  invalid_json: "bg-rose-950 text-rose-300 border-rose-800",
  missing_required_field: "bg-amber-950 text-amber-300 border-amber-800",
  non_positive_price: "bg-purple-950 text-purple-300 border-purple-800",
};

export async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

export function updateHealthBanner(summary) {
  const sp = summary.spark_progress || {};
  const rateEl = document.getElementById("spark-rate");
  const batchEl = document.getElementById("spark-batch");
  const freshEl = document.getElementById("freshness");

  if (rateEl) {
    rateEl.textContent = `${Number(sp.processed_rows_per_second || 0).toFixed(1)} rows/s`;
  }
  if (batchEl) {
    batchEl.textContent = `${sp.batch_duration_ms || 0} ms`;
  }
  if (freshEl) {
    const fresh = summary.freshness_seconds;
    freshEl.textContent = fresh != null ? `${Math.round(fresh)}s ago` : "—";
  }
}

export function escapeHTML(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
