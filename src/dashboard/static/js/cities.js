import { fetchJSON, fmtINR, updateHealthBanner } from "/static/js/common.js";

const volumeChart = new Chart(document.getElementById("cityVolumeChart"), {
  type: "bar",
  data: {
    labels: [],
    datasets: [
      { label: "Bookings", data: [], backgroundColor: "#38bdf8", borderRadius: 4 },
      { label: "Cancellations", data: [], backgroundColor: "#fbbf24", borderRadius: 4 },
    ],
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { labels: { color: "#cbd5e1" } } },
    scales: {
      x: { ticks: { color: "#94a3b8" }, grid: { display: false } },
      y: { ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
    },
  },
});

async function refreshCities() {
  try {
    const [summary, cities] = await Promise.all([
      fetchJSON("/api/summary"),
      fetchJSON("/api/cities"),
    ]);

    updateHealthBanner(summary);

    volumeChart.data.labels = cities.map((c) => c.city);
    volumeChart.data.datasets[0].data = cities.map((c) => c.bookings);
    volumeChart.data.datasets[1].data = cities.map((c) => c.cancellations);
    volumeChart.update("none");

    document.getElementById("city-table").innerHTML = cities
      .map((c) => {
        const total = (c.bookings || 0) + (c.cancellations || 0);
        const cancelPct = total > 0 ? (((c.cancellations || 0) / total) * 100).toFixed(1) : "0.0";
        return `
          <tr class="hover:bg-slate-800/40">
            <td class="py-2.5 pr-3 font-medium text-white">${c.city}</td>
            <td class="py-2.5 pr-3 text-right">${(c.bookings || 0).toLocaleString()}</td>
            <td class="py-2.5 pr-3 text-right text-amber-400">${(c.cancellations || 0).toLocaleString()}</td>
            <td class="py-2.5 pr-3 text-right text-slate-300">${cancelPct}%</td>
            <td class="py-2.5 pr-3 text-right text-emerald-400 font-medium">${fmtINR(c.revenue)}</td>
            <td class="py-2.5 text-right text-slate-400">${Math.round(c.avg_latency_ms || 0)} ms</td>
          </tr>
        `;
      })
      .join("");
  } catch (err) {
    console.error("Cities refresh failed:", err);
  }
}

refreshCities();
setInterval(refreshCities, 2500);
