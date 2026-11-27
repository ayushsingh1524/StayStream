import {
  CHART_TOOLTIP_THEME,
  PALETTE,
  fetchJSON,
  fmtINR,
  makeVerticalGradient,
  updateHealthBanner,
} from "/static/js/common.js";

const volumeChart = new Chart(document.getElementById("cityVolumeChart"), {
  type: "bar",
  data: {
    labels: [],
    datasets: [
      {
        label: "Bookings",
        data: [],
        backgroundColor: (ctx) =>
          makeVerticalGradient(ctx, "rgba(159, 231, 245, 0.95)", "rgba(66, 158, 189, 0.5)"),
        borderColor: PALETTE.iceCyan,
        borderWidth: 1,
        borderRadius: 6,
      },
      {
        label: "Cancellations",
        data: [],
        backgroundColor: (ctx) =>
          makeVerticalGradient(ctx, "rgba(247, 173, 25, 0.95)", "rgba(247, 173, 25, 0.4)"),
        borderColor: PALETTE.amberGold,
        borderWidth: 1,
        borderRadius: 6,
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
      x: { ticks: { color: PALETTE.textMuted }, grid: { display: false } },
      y: { ticks: { color: PALETTE.textMuted }, grid: { color: PALETTE.gridLine } },
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
          <tr>
            <td class="py-3 pr-3 font-sans font-semibold text-[#F0FAFC]">${c.city}</td>
            <td class="py-3 pr-3 text-right text-[#F0FAFC]">${(c.bookings || 0).toLocaleString()}</td>
            <td class="py-3 pr-3 text-right text-[#F7AD19]">${(c.cancellations || 0).toLocaleString()}</td>
            <td class="py-3 pr-3 text-right text-[#8BB8CA]">${cancelPct}%</td>
            <td class="py-3 pr-3 text-right text-[#9FE7F5] font-semibold">${fmtINR(c.revenue)}</td>
            <td class="py-3 text-right text-[#8BB8CA]">${Math.round(c.avg_latency_ms || 0)} ms</td>
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
