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
