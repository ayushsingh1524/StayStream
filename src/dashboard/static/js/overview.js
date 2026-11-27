import { fetchJSON, fmtINR, updateHealthBanner, escapeHTML } from "/static/js/common.js";

const cityChart = new Chart(document.getElementById("cityChart"), {
  type: "bar",
  data: {
    labels: [],
    datasets: [
      {
        label: "Revenue (INR)",
        data: [],
        backgroundColor: "#34d399",
        borderRadius: 6,
      },
    ],
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false } },
    scales: {
      x: { ticks: { color: "#94a3b8" }, grid: { display: false } },
      y: { ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
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
        borderColor: "#38bdf8",
        backgroundColor: "rgba(56,189,248,0.15)",
        fill: true,
        tension: 0.3,
      },
      {
        label: "Cancellations",
        data: [],
        borderColor: "#fbbf24",
        backgroundColor: "rgba(251,191,36,0.1)",
        fill: false,
        tension: 0.3,
      },
    ],
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { labels: { color: "#cbd5e1" } } },
    scales: {
      x: { ticks: { color: "#94a3b8", maxTicksLimit: 8 }, grid: { display: false } },
      y: { ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
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
        <div class="flex items-center justify-between bg-amber-950/40 border border-amber-800/40 rounded-lg px-3 py-2">
          <div class="flex items-center gap-3">
            <span class="text-amber-400 font-semibold">${escapeHTML(a.city)}</span>
            <span class="text-amber-300/70">Cancel rate: ${rate}%</span>
            <span class="text-slate-500">Bookings: ${details.bookings || 0} · Cancellations: ${details.cancellations || 0}</span>
          </div>
          <span class="text-slate-500 whitespace-nowrap">${time}</span>
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
    document.getElementById("kpi-bookings").textContent = (summary.total_bookings || 0).toLocaleString();
    document.getElementById("kpi-revenue").textContent = fmtINR(summary.total_revenue);
    document.getElementById("kpi-cancellations").textContent = (summary.total_cancellations || 0).toLocaleString();
    document.getElementById("kpi-latency").textContent = `Avg latency: ${Math.round(summary.avg_latency_ms || 0)} ms`;
    document.getElementById("kpi-dlq").textContent = (summary.dlq_count || 0).toLocaleString();

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
