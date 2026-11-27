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
        const cls = REASON_COLORS[b.reason] || "bg-[#053F5C]/70 text-[#9FE7F5] border-[#429EBD]/40";
        return `<span class="px-3 py-1 rounded-full border ${cls}">${b.reason}: ${b.count}</span>`;
      })
      .join("");

    document.getElementById("dlq-table").innerHTML = (dlq.items || [])
      .map((d) => {
        const cls = REASON_COLORS[d.reason] || "bg-[#053F5C]/70 text-[#9FE7F5] border-[#429EBD]/40";
        const timeStr = d.received_at
          ? new Date(d.received_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
          : "—";
        return `
          <tr>
            <td class="py-2.5 pr-3 text-[#8BB8CA]">#${d.id}</td>
            <td class="py-2.5 pr-3"><span class="px-2.5 py-0.5 rounded-full border ${cls}">${escapeHTML(d.reason)}</span></td>
            <td class="py-2.5 pr-3 text-[#8BB8CA] whitespace-nowrap">${timeStr}</td>
            <td class="py-2.5 text-[#F0FAFC] break-all">${escapeHTML(d.raw_value)}</td>
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
