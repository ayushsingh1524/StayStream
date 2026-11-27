import { escapeHTML, fetchJSON, REASON_COLORS, updateHealthBanner } from "/static/js/common.js";

let selectedReason = "";

document.getElementById("reason-filter").addEventListener("change", (e) => {
  selectedReason = e.target.value;
  refreshDLQ();
});

async function refreshDLQ() {
  try {
    const dlqUrl = selectedReason
      ? `/api/dlq?limit=50&reason=${encodeURIComponent(selectedReason)}`
      : "/api/dlq?limit=50";

    const [summary, dlq] = await Promise.all([
      fetchJSON("/api/summary"),
      fetchJSON(dlqUrl),
    ]);

    updateHealthBanner(summary);

    document.getElementById("dlq-badges").innerHTML = (dlq.breakdown || [])
      .map((b) => {
        const cls = REASON_COLORS[b.reason] || "bg-slate-800 text-slate-300 border-slate-700";
        return `<span class="px-2.5 py-1 rounded border ${cls}">${b.reason}: ${b.count}</span>`;
      })
      .join("");

    document.getElementById("dlq-table").innerHTML = (dlq.items || [])
      .map((d) => {
        const cls = REASON_COLORS[d.reason] || "bg-slate-800 text-slate-300 border-slate-700";
        const timeStr = d.received_at
          ? new Date(d.received_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
          : "—";
        return `
          <tr class="hover:bg-slate-800/40">
            <td class="py-2 pr-3 text-slate-400">#${d.id}</td>
            <td class="py-2 pr-3"><span class="px-2 py-0.5 rounded border ${cls}">${escapeHTML(d.reason)}</span></td>
            <td class="py-2 pr-3 text-slate-400 whitespace-nowrap">${timeStr}</td>
            <td class="py-2 text-slate-200 break-all">${escapeHTML(d.raw_value)}</td>
          </tr>
        `;
      })
      .join("");
  } catch (err) {
    console.error("DLQ refresh failed:", err);
  }
}

refreshDLQ();
setInterval(refreshDLQ, 2500);
