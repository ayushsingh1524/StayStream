/* StayStream Dashboard Shared Helpers – Oceanic Telemetry Palette */

export const PALETTE = {
  iceCyan: "#9FE7F5",
  oceanTeal: "#429EBD",
  abyssNavy: "#053F5C",
  amberGold: "#F7AD19",
  textPrimary: "#F0FAFC",
  textMuted: "#8BB8CA",
  gridLine: "rgba(66, 158, 189, 0.14)",
};

export const fmtINR = (v) =>
  "₹" + Number(v || 0).toLocaleString("en-IN", { maximumFractionDigits: 0 });

export const REASON_COLORS = {
  invalid_json: "bg-[#F7AD19]/15 text-[#F7AD19] border-[#F7AD19]/45",
  missing_required_field: "bg-[#429EBD]/20 text-[#9FE7F5] border-[#429EBD]/50",
  non_positive_price: "bg-[#053F5C]/80 text-[#9FE7F5] border-[#9FE7F5]/40",
};

export async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

export function setKpiText(id, nextText) {
  const el = document.getElementById(id);
  if (!el) return;
  if (el.textContent !== nextText) {
    el.textContent = nextText;
    el.classList.remove("kpi-updated");
    void el.offsetWidth; // trigger reflow to replay animation
    el.classList.add("kpi-updated");
  }
}

export function makeVerticalGradient(ctx, topColor, bottomColor) {
  const chart = ctx.chart;
  const { ctx: canvasCtx, chartArea } = chart;
  if (!chartArea) return topColor;
  const grad = canvasCtx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
  grad.addColorStop(0, topColor);
  grad.addColorStop(1, bottomColor);
  return grad;
}

export const CHART_TOOLTIP_THEME = {
  backgroundColor: "rgba(2, 24, 36, 0.94)",
  titleColor: "#9FE7F5",
  bodyColor: "#F0FAFC",
  borderColor: "rgba(66, 158, 189, 0.55)",
  borderWidth: 1,
  padding: 10,
  cornerRadius: 10,
};

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
