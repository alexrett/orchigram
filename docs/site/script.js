const shot = document.querySelector("#product-shot");
const panel = document.querySelector("#shot-panel");
const tabs = [...document.querySelectorAll("[data-shot]")];

for (const tab of tabs) {
  tab.addEventListener("click", () => {
    for (const candidate of tabs) candidate.setAttribute("aria-selected", String(candidate === tab));
    shot.src = tab.dataset.shot;
    shot.alt = tab.dataset.alt;
    panel.setAttribute("aria-labelledby", tab.id);
  });
  tab.addEventListener("keydown", (event) => {
    if (event.key !== "ArrowRight" && event.key !== "ArrowLeft") return;
    event.preventDefault();
    const direction = event.key === "ArrowRight" ? 1 : -1;
    const next = tabs[(tabs.indexOf(tab) + direction + tabs.length) % tabs.length];
    next.focus();
    next.click();
  });
}

for (const button of document.querySelectorAll("[data-copy]")) {
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(button.dataset.copy);
      const previous = button.textContent;
      button.textContent = "copied";
      window.setTimeout(() => { button.textContent = previous; }, 1400);
    } catch {
      button.textContent = "select";
    }
  });
}
