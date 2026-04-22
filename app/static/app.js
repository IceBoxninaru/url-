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
        headers: { "X-Requested-With": "fetch" },
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
}

function initTabs() {
  document.querySelectorAll("[data-tab-group]").forEach((group) => {
    const buttons = Array.from(group.querySelectorAll("[data-tab-target]"));
    const panels = Array.from(group.querySelectorAll("[data-tab-panel]"));
    if (buttons.length === 0 || panels.length === 0) {
      return;
    }

    const activate = (target) => {
      buttons.forEach((button) => {
        button.classList.toggle("is-active", button.dataset.tabTarget === target);
      });
      panels.forEach((panel) => {
        panel.classList.toggle("is-active", panel.dataset.tabPanel === target);
      });
    };

    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        activate(button.dataset.tabTarget);
      });
    });
  });
}

function initBulkSelection() {
  document.querySelectorAll("[data-bulk-selection-root]").forEach((root) => {
    const collectSelectedIds = () => {
      const selectedIds = new Set();
      root.querySelectorAll("[data-bulk-persisted-selection]").forEach((input) => {
        if (input instanceof HTMLInputElement && input.value) {
          selectedIds.add(input.value);
        }
      });
      root.querySelectorAll("[data-bulk-select-item]:checked").forEach((input) => {
        if (input instanceof HTMLInputElement && input.value) {
          selectedIds.add(input.value);
        }
      });
      return Array.from(selectedIds);
    };

    const sync = () => {
      const checkboxes = Array.from(root.querySelectorAll("[data-bulk-select-item]"));
      const checkedOnPage = checkboxes.filter((checkbox) => checkbox.checked).length;
      const checkedCount = collectSelectedIds().length;
      const countNode = root.querySelector("[data-bulk-selected-count]");
      const selectAllNode = root.querySelector("[data-bulk-select-all]");

      if (countNode) {
        countNode.textContent = String(checkedCount);
      }
      if (selectAllNode instanceof HTMLInputElement) {
        selectAllNode.checked = checkboxes.length > 0 && checkedOnPage === checkboxes.length;
        selectAllNode.indeterminate = checkedOnPage > 0 && checkedOnPage < checkboxes.length;
      }
    };

    root.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement)) {
        return;
      }
      if (target.matches("[data-bulk-select-all]")) {
        root.querySelectorAll("[data-bulk-select-item]").forEach((checkbox) => {
          checkbox.checked = target.checked;
        });
      }
      sync();
    });

    root.querySelectorAll("[data-bulk-page-link]").forEach((link) => {
      link.addEventListener("click", (event) => {
        const currentLink = event.currentTarget;
        if (!(currentLink instanceof HTMLAnchorElement)) {
          return;
        }
        event.preventDefault();
        const url = new URL(currentLink.href, window.location.origin);
        url.searchParams.delete("resource_ids");
        collectSelectedIds().forEach((resourceId) => {
          url.searchParams.append("resource_ids", resourceId);
        });
        window.location.href = url.toString();
      });
    });

    sync();
  });
}

function initSaveReasonCustomField() {
  const customValue = "__custom__";
  document.querySelectorAll("select[name='save_reason']").forEach((selectNode) => {
    if (!(selectNode instanceof HTMLSelectElement)) {
      return;
    }
    const wrapper = selectNode.closest("form")?.querySelector("[data-save-reason-custom]");
    if (!(wrapper instanceof HTMLElement)) {
      return;
    }

    const sync = () => {
      wrapper.classList.toggle("is-hidden", selectNode.value !== customValue);
    };

    selectNode.addEventListener("change", sync);
    sync();
  });
}

document.addEventListener("DOMContentLoaded", () => {
  initResourceAutoRefresh();
  initTabs();
  initBulkSelection();
  initSaveReasonCustomField();
});
