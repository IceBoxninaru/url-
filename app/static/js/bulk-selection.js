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
