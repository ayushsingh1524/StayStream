import { fetchJSON, fmtINR, updateHealthBanner } from "/static/js/common.js";

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

async function refreshOverview() {
  try {
    const [summary, cities, series] = await Promise.all([
      fetchJSON("/api/summary"),
      fetchJSON("/api/cities"),
      fetchJSON("/api/timeseries"),
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
  } catch (err) {
    console.error("Overview refresh failed:", err);
  }
}

refreshOverview();
setInterval(refreshOverview, 2500);
