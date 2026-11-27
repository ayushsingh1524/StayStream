import {
  CHART_TOOLTIP_THEME,
  PALETTE,
  escapeHTML,
  fetchJSON,
  fmtINR,
  makeVerticalGradient,
  setKpiText,
  updateHealthBanner,
} from "/static/js/common.js";

const cityChart = new Chart(document.getElementById("cityChart"), {
  type: "bar",
  data: {
    labels: [],
    datasets: [
      {
        label: "Revenue (INR)",
        data: [],
        backgroundColor: (ctx) =>
          makeVerticalGradient(ctx, "rgba(159, 231, 245, 0.95)", "rgba(66, 158, 189, 0.45)"),
        hoverBackgroundColor: PALETTE.amberGold,
        borderColor: PALETTE.iceCyan,
        borderWidth: 1,
        borderRadius: 8,
        borderSkipped: false,
      },
    ],
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      tooltip: CHART_TOOLTIP_THEME,
    },
    scales: {
      x: { ticks: { color: PALETTE.textMuted }, grid: { display: false } },
      y: { ticks: { color: PALETTE.textMuted }, grid: { color: PALETTE.gridLine } },
    },
  },
});

const trendChart = new Chart(document.getElementById("trendChart"), {
  type: "line",
  data: {
    labels: [],
    datasets: [
      {
        label: "Bookings",
        data: [],
        borderColor: PALETTE.iceCyan,
        backgroundColor: (ctx) =>
          makeVerticalGradient(ctx, "rgba(159, 231, 245, 0.35)", "rgba(5, 63, 92, 0.02)"),
        pointBackgroundColor: PALETTE.iceCyan,
        pointBorderColor: "#021824",
        pointRadius: 3,
        pointHoverRadius: 5,
        fill: true,
        tension: 0.35,
      },
      {
        label: "Cancellations",
        data: [],
        borderColor: PALETTE.amberGold,
        backgroundColor: (ctx) =>
          makeVerticalGradient(ctx, "rgba(247, 173, 25, 0.28)", "rgba(247, 173, 25, 0.0)"),
        pointBackgroundColor: PALETTE.amberGold,
        pointBorderColor: "#021824",
        pointRadius: 3,
        pointHoverRadius: 5,
        fill: true,
        tension: 0.35,
      },
    ],
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color: PALETTE.textPrimary, usePointStyle: true } },
      tooltip: CHART_TOOLTIP_THEME,
    },
    scales: {
      x: { ticks: { color: PALETTE.textMuted, maxTicksLimit: 8 }, grid: { display: false } },
      y: { ticks: { color: PALETTE.textMuted }, grid: { color: PALETTE.gridLine } },
    },
  },
});

function renderAlerts(alerts) {
  const section = document.getElementById("alerts-section");
  const list = document.getElementById("alerts-list");
  const badge = document.getElementById("alert-count-badge");

  if (!alerts || !alerts.items || alerts.items.length === 0) {
    section.classList.add("hidden");
    return;
  }

  section.classList.remove("hidden");
  badge.textContent = alerts.items.length;

  list.innerHTML = alerts.items
    .map((a) => {
      const details = typeof a.details === "string" ? JSON.parse(a.details) : a.details;
      const rate = details.cancel_rate != null ? (details.cancel_rate * 100).toFixed(1) : "?";
      const time = a.created_at
        ? new Date(a.created_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
        : "—";
      return `
        <div class="flex items-center justify-between bg-[#021824]/65 border border-[#F7AD19]/35 rounded-xl px-3.5 py-2.5">
          <div class="flex flex-wrap items-center gap-3">
            <span class="text-[#F7AD19] font-semibold">${escapeHTML(a.city)}</span>
            <span class="font-mono-num text-[#9FE7F5]">Cancel rate: ${rate}%</span>
            <span class="font-mono-num text-[#8BB8CA]">Bookings: ${details.bookings || 0} · Cancellations: ${details.cancellations || 0}</span>
          </div>
          <span class="font-mono-num text-[#8BB8CA] whitespace-nowrap">${time}</span>
        </div>
      `;
    })
    .join("");
}

async function refreshOverview() {
  try {
    const [summary, cities, series, alerts] = await Promise.all([
      fetchJSON("/api/summary"),
      fetchJSON("/api/cities"),
      fetchJSON("/api/timeseries"),
      fetchJSON("/api/alerts?limit=10"),
    ]);

    updateHealthBanner(summary);
    setKpiText("kpi-bookings", (summary.total_bookings || 0).toLocaleString());
    setKpiText("kpi-revenue", fmtINR(summary.total_revenue));
    setKpiText("kpi-cancellations", (summary.total_cancellations || 0).toLocaleString());
    setKpiText("kpi-latency", `Avg latency: ${Math.round(summary.avg_latency_ms || 0)} ms`);
    setKpiText("kpi-dlq", (summary.dlq_count || 0).toLocaleString());

    cityChart.data.labels = cities.map((c) => c.city);
    cityChart.data.datasets[0].data = cities.map((c) => c.revenue);
    cityChart.update("none");

    trendChart.data.labels = series.map((s) =>
      new Date(s.window_start).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    );
    trendChart.data.datasets[0].data = series.map((s) => s.bookings);
    trendChart.data.datasets[1].data = series.map((s) => s.cancellations);
    trendChart.update("none");

    renderAlerts(alerts);
  } catch (err) {
    console.error("Overview refresh failed:", err);
  }
}

refreshOverview();
setInterval(refreshOverview, 2500);
