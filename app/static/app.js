function initResourceAutoRefresh() {
  const selector = "[data-resource-autorefresh]";

  const getPanel = () => document.querySelector(selector);
  let panel = getPanel();
  if (!panel) {
    return;
  }

  let signature = panel.dataset.resourceSignature || "";
  const pollMs = Number(panel.dataset.resourcePollMs || 10000);
  let inFlight = false;

  const setStatus = (message) => {
    const currentPanel = getPanel();
    const statusNode = currentPanel?.querySelector("[data-autorefresh-status]");
    if (statusNode) {
      statusNode.textContent = message;
    }
  };

  const syncBulkSelection = () => {
    const currentPanel = getPanel();
    const checkboxes = Array.from(currentPanel?.querySelectorAll("[data-resource-select-checkbox]") || []);
    const checkedCount = checkboxes.filter((checkbox) => checkbox.checked).length;
    const countNode = currentPanel?.querySelector("[data-resource-selected-count]");
    const selectAllNode = currentPanel?.querySelector("[data-resource-select-all]");

    if (countNode) {
      countNode.textContent = `${checkedCount} 件選択中`;
    }
    if (selectAllNode) {
      selectAllNode.checked = checkboxes.length > 0 && checkedCount === checkboxes.length;
      selectAllNode.indeterminate = checkedCount > 0 && checkedCount < checkboxes.length;
    }
  };

  const hasBulkSelection = () => {
    const currentPanel = getPanel();
    return Boolean(currentPanel?.querySelector("[data-resource-select-checkbox]:checked"));
  };

  const buildFragmentUrl = () => {
    const currentPanel = getPanel();
    const baseUrl = currentPanel?.dataset.resourceFragmentUrl;
    if (!baseUrl) {
      return null;
    }
    const url = new URL(baseUrl, window.location.origin);
    const currentParams = new URLSearchParams(window.location.search);
    currentParams.forEach((value, key) => {
      url.searchParams.append(key, value);
    });
    return url;
  };

  const refreshList = async (force = false) => {
    panel = getPanel();
    if (!panel || inFlight) {
      return;
    }
    if (document.hidden && !force) {
      return;
    }
    if (hasBulkSelection()) {
      setStatus("選択中");
      return;
    }

    const url = buildFragmentUrl();
    if (!url) {
      return;
    }

    if (force) {
      url.searchParams.set("_ts", String(Date.now()));
    }

    inFlight = true;
    setStatus("同期中...");

    try {
      const response = await fetch(url, {
        headers: {
          "X-Requested-With": "fetch",
        },
        credentials: "same-origin",
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const payload = await response.json();
      if (payload.signature && payload.signature !== signature && payload.html) {
        panel.outerHTML = payload.html;
        panel = getPanel();
        signature = panel?.dataset.resourceSignature || payload.signature;
        syncBulkSelection();
        setStatus("更新済み");
        window.setTimeout(() => setStatus("自動更新中"), 1800);
      } else {
        signature = payload.signature || signature;
        setStatus("自動更新中");
      }
    } catch (_error) {
      setStatus("再試行待ち");
    } finally {
      inFlight = false;
    }
  };

  window.setInterval(() => {
    refreshList(false);
  }, pollMs);

  window.addEventListener("focus", () => {
    refreshList(true);
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      refreshList(true);
    }
  });

  document.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) {
      return;
    }
    if (target.matches("[data-resource-select-all]")) {
      const currentPanel = getPanel();
      currentPanel?.querySelectorAll("[data-resource-select-checkbox]").forEach((checkbox) => {
        checkbox.checked = target.checked;
      });
      syncBulkSelection();
      return;
    }
    if (target.matches("[data-resource-select-checkbox]")) {
      syncBulkSelection();
    }
  });

  syncBulkSelection();
}

document.addEventListener("DOMContentLoaded", () => {
  initResourceAutoRefresh();
});
