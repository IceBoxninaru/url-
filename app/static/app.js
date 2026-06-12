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
  let actionInFlight = false;

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

  const restoreScrollPosition = (scrollPosition) => {
    if (!scrollPosition) {
      return;
    }
    window.requestAnimationFrame(() => {
      window.scrollTo(scrollPosition.x, scrollPosition.y);
    });
  };

  const updateInterestToggleGroup = (form, resource) => {
    const group = form.closest(".interest-toggle-group");
    const feedback = resource?.interest_feedback;
    if (!group || !feedback) {
      return false;
    }

    group.querySelectorAll("form").forEach((toggleForm) => {
      const input = toggleForm.querySelector("input[name='interest_feedback']");
      const button = toggleForm.querySelector(".interest-toggle");
      if (!(input instanceof HTMLInputElement) || !(button instanceof HTMLButtonElement)) {
        return;
      }
      const isActive = input.value === feedback;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
    return true;
  };

  const refreshList = async (force = false, options = {}) => {
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
    const scrollPosition = options.preserveScroll === false ? null : { x: window.scrollX, y: window.scrollY };

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
        if (options.skipReplace) {
          signature = payload.signature;
          panel.dataset.resourceSignature = signature;
          restoreScrollPosition(scrollPosition);
          setStatus("更新済み");
          window.setTimeout(() => setStatus("自動更新中"), 1800);
          return;
        }
        panel.outerHTML = payload.html;
        panel = getPanel();
        signature = panel?.dataset.resourceSignature || payload.signature;
        restoreScrollPosition(scrollPosition);
        setStatus("更新済み");
        window.setTimeout(() => setStatus("自動更新中"), 1800);
      } else {
        signature = payload.signature || signature;
        restoreScrollPosition(scrollPosition);
        setStatus("自動更新中");
      }
    } catch (_error) {
      setStatus("再試行待ち");
    } finally {
      inFlight = false;
    }
  };

  document.addEventListener("submit", async (event) => {
    if (event.defaultPrevented) {
      return;
    }

    const form = event.target;
    if (!(form instanceof HTMLFormElement) || !form.matches("[data-resource-action-form]")) {
      return;
    }
    if (!form.closest(selector)) {
      return;
    }

    event.preventDefault();
    if (actionInFlight) {
      return;
    }

    const submitButtons = Array.from(form.querySelectorAll("button[type='submit']"));
    actionInFlight = true;
    submitButtons.forEach((button) => {
      button.disabled = true;
    });
    setStatus("更新中...");

    try {
      const response = await fetch(form.action, {
        method: form.method || "POST",
        body: new FormData(form),
        headers: { "X-Requested-With": "fetch" },
        credentials: "same-origin",
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      if (form.querySelector("input[name='interest_feedback']")) {
        const payload = await response.json();
        if (updateInterestToggleGroup(form, payload.resource)) {
          setStatus("更新済み");
          window.setTimeout(() => setStatus("自動更新中"), 1800);
          await refreshList(true, { preserveScroll: true, skipReplace: true });
          return;
        }
      }
      await refreshList(true, { preserveScroll: true });
    } catch (_error) {
      setStatus("更新失敗");
    } finally {
      actionInFlight = false;
      submitButtons.forEach((button) => {
        button.disabled = false;
      });
    }
  });

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

function initMediaViewer() {
  const selector = "[data-media-viewer]";
  let viewer = null;
  let mediaSlot = null;
  let titleNode = null;
  let closeButton = null;

  const closeViewer = () => {
    if (!viewer || !mediaSlot) {
      return;
    }
    const video = mediaSlot.querySelector("video");
    if (video instanceof HTMLVideoElement) {
      video.pause();
    }
    mediaSlot.replaceChildren();
    viewer.classList.remove("is-open");
    viewer.setAttribute("aria-hidden", "true");
    document.body.classList.remove("media-viewer-open");
  };

  const ensureViewer = () => {
    if (viewer && mediaSlot && titleNode && closeButton) {
      return;
    }

    viewer = document.createElement("div");
    viewer.className = "media-viewer";
    viewer.setAttribute("aria-hidden", "true");
    viewer.setAttribute("role", "dialog");
    viewer.setAttribute("aria-modal", "true");
    viewer.innerHTML = `
      <div class="media-viewer__bar">
        <p class="media-viewer__title"></p>
        <button type="button" class="media-viewer__close" aria-label="閉じる">×</button>
      </div>
      <div class="media-viewer__stage" data-media-viewer-stage></div>
    `;
    document.body.appendChild(viewer);

    mediaSlot = viewer.querySelector("[data-media-viewer-stage]");
    titleNode = viewer.querySelector(".media-viewer__title");
    closeButton = viewer.querySelector(".media-viewer__close");

    closeButton?.addEventListener("click", closeViewer);
    viewer.addEventListener("click", (event) => {
      if (event.target === viewer || event.target === mediaSlot) {
        closeViewer();
      }
    });
  };

  const openViewer = (link) => {
    ensureViewer();
    if (!viewer || !mediaSlot || !titleNode || !closeButton) {
      return;
    }

    const source = link.href;
    const kind = link.dataset.mediaViewer;
    const title = link.dataset.mediaTitle || link.querySelector("img")?.alt || "";
    mediaSlot.replaceChildren();
    titleNode.textContent = title;

    if (kind === "video") {
      const video = document.createElement("video");
      video.src = source;
      video.controls = true;
      video.autoplay = true;
      video.playsInline = true;
      mediaSlot.appendChild(video);
    } else {
      const image = document.createElement("img");
      image.src = source;
      image.alt = title || "media";
      mediaSlot.appendChild(image);
    }

    viewer.classList.add("is-open");
    viewer.setAttribute("aria-hidden", "false");
    document.body.classList.add("media-viewer-open");
    closeButton.focus();
  };

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }
    const link = target.closest(selector);
    if (!(link instanceof HTMLAnchorElement)) {
      return;
    }
    event.preventDefault();
    openViewer(link);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeViewer();
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  initResourceAutoRefresh();
  initTabs();
  initBulkSelection();
  initSaveReasonCustomField();
  initMediaViewer();
});
