/* abasift.vis — edges are routed in the browser from the boxes' real geometry.
   Nothing about the layout is baked into the page: the node cards are ordinary flow
   content laid out in columns, and the wires are drawn to wherever they landed. So a card
   can grow (long param list, a report overlay) without anything to re-tune here.

   Everything is re-bindable, because the live server replaces the whole of <main> when the
   YAML, a kernel or the report changes — see init(). */

const AbaSift = (() => {
  let graph = null;
  let wires = null;
  let edges = [];

  const cardOf = (name) => graph.querySelector(`[data-node="${CSS.escape(name)}"]`);

  // Card geometry in *content* coordinates, so a horizontally scrolled graph still lines up.
  function box(el) {
    const r = el.getBoundingClientRect();
    const g = graph.getBoundingClientRect();
    return {
      x: r.left - g.left + graph.scrollLeft,
      y: r.top - g.top + graph.scrollTop,
      w: r.width,
      h: r.height,
    };
  }

  function draw() {
    if (!graph || !wires) return;
    const w = graph.scrollWidth;
    const h = graph.scrollHeight;
    const svg = wires.ownerSVGElement;
    svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
    svg.style.width = `${w}px`;
    svg.style.height = `${h}px`;
    // One path per edge, in edge order — highlighting indexes straight into it.
    wires.innerHTML = edges
      .map((e) => {
        const a = cardOf(e.from);
        const b = cardOf(e.to);
        if (!a || !b) return "<path/>";
        const p = box(a);
        const q = box(b);
        // Anchor near the header, not the middle: cards differ a lot in height.
        const y1 = p.y + Math.min(p.h / 2, 30);
        const y2 = q.y + Math.min(q.h / 2, 30);
        const x1 = p.x + p.w;
        const x2 = q.x - 7; // leave room for the arrowhead
        const dx = Math.max(30, (x2 - x1) * 0.5);
        return `<path d="M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}" marker-end="url(#ah)"/>`;
      })
      .join("");
  }

  function light(name, on) {
    const paths = wires.querySelectorAll("path");
    edges.forEach((e, i) => {
      if (e.from === name || e.to === name) paths[i]?.classList.toggle("lit", on);
    });
  }

  function bindTheme() {
    const root = document.documentElement;
    const button = document.getElementById("theme"); // inside <main>: replaced on every swap
    button?.addEventListener("click", () => {
      const dark = getComputedStyle(root).getPropertyValue("--bg").trim() === "#14171a";
      const next = dark ? "light" : "dark";
      root.setAttribute("data-theme", next);
      localStorage.setItem("abasift-theme", next);
      draw();
    });
  }

  /* --- the detail sheet ---------------------------------------------------
     Cards carry only what you need to read the graph; each one holds the rest in an
     inert <template> that is cloned into the dialog on demand. `open` is remembered by
     node name so a live swap (which replaces the whole of <main>, dialog included) can
     put the sheet back exactly as it was — you can watch one node's verdicts land. */
  let open = null;

  function openSheet(name) {
    const card = cardOf(name);
    const sheet = document.getElementById("sheet");
    const template = card?.querySelector("template.detail");
    if (!sheet || !template) return;
    sheet.querySelector(".sheet-body").replaceChildren(template.content.cloneNode(true));
    open = name;
    if (!sheet.open) sheet.showModal();
  }

  function bindSheet() {
    const sheet = document.getElementById("sheet");
    if (!sheet) return;
    sheet.addEventListener("close", () => (open = null));
    // Clicking the backdrop reports the dialog itself as the target.
    sheet.addEventListener("click", (e) => {
      if (e.target === sheet) sheet.close();
    });
    sheet.querySelector(".sheet-close")?.addEventListener("click", () => sheet.close());
  }

  /** (Re)bind to whatever is in the document right now, and draw. */
  function init() {
    graph = document.getElementById("graph");
    wires = document.getElementById("wire-group");
    const tag = document.getElementById("edges");
    edges = tag ? JSON.parse(tag.textContent) : [];
    bindTheme();
    bindSheet();
    if (!graph || !wires) return; // e.g. the "won't load" page: nothing to wire
    graph.querySelectorAll(".node").forEach((el) => {
      const name = el.dataset.node;
      el.addEventListener("mouseenter", () => light(name, true));
      el.addEventListener("mouseleave", () => light(name, false));
      el.addEventListener("click", () => openSheet(name));
      el.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          openSheet(name);
        }
      });
    });
    draw();
    if (open) openSheet(open); // survive a live swap
    if (window.ResizeObserver) new ResizeObserver(draw).observe(graph);
  }

  return { init, draw };
})();

/* theme preference is on the document, so it survives a swap */
(() => {
  const saved = localStorage.getItem("abasift-theme");
  if (saved) document.documentElement.setAttribute("data-theme", saved);
})();

AbaSift.init();
addEventListener("resize", () => AbaSift.draw());
addEventListener("load", () => AbaSift.draw()); // fonts can settle after DOMContentLoaded

/* --- live mode ---------------------------------------------------------------
   Only when served: poll a cheap state digest, and when it moves, pull the new <main>.
   The server does the describing and the rendering, so there is exactly one renderer;
   the page just replaces its own body with the new one. A file opened from disk has no
   window.ABASIFT_LIVE and never polls. */
(async () => {
  if (!window.ABASIFT_LIVE) return;
  let state = window.ABASIFT_LIVE.state;
  const wait = (ms) => new Promise((r) => setTimeout(r, ms));
  for (;;) {
    await wait(1200);
    try {
      const res = await fetch("state", { cache: "no-store" });
      if (!res.ok) continue;
      const next = (await res.text()).trim();
      if (next === state) continue;
      const html = await (await fetch("body", { cache: "no-store" })).text();
      document.getElementById("main").innerHTML = html;
      state = next;
      AbaSift.init();
    } catch (e) {
      // server stopped or restarting: keep polling, the page recovers on its own
    }
  }
})();
