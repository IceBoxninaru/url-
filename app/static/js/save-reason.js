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
