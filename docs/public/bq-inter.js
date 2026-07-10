// Interactive layer for the use-case diagrams: any element carrying a
// data-tip inside a [data-bq-interactive] wrapper gets hover/focus/tap
// behavior with a shared tooltip, node lift and sibling dimming.
(() => {
  if (!document.querySelector('[data-bq-interactive] [data-tip]')) return;

  const tip = document.createElement('div');
  tip.className = 'bq-tip';
  tip.setAttribute('role', 'tooltip');
  tip.id = 'bq-tip';
  document.body.appendChild(tip);

  let current = null;

  function place(el) {
    const r = el.getBoundingClientRect();
    const tr = tip.getBoundingClientRect();
    let x = r.left + r.width / 2 - tr.width / 2;
    x = Math.max(8, Math.min(x, window.innerWidth - tr.width - 8));
    let y = r.top - tr.height - 10;
    if (y < 8) y = r.bottom + 10;
    tip.style.left = `${Math.round(x)}px`;
    tip.style.top = `${Math.round(y + window.scrollY)}px`;
  }

  function show(el) {
    if (current && current !== el) hide();
    current = el;
    const wrap = el.closest('[data-bq-interactive]');
    if (wrap) wrap.classList.add('bq-dimming');
    el.classList.add('bq-tip-active');
    el.setAttribute('aria-describedby', 'bq-tip');
    tip.textContent = el.getAttribute('data-tip');
    tip.classList.add('bq-tip-show');
    place(el);
  }

  function hide() {
    if (!current) return;
    const wrap = current.closest('[data-bq-interactive]');
    if (wrap) wrap.classList.remove('bq-dimming');
    current.classList.remove('bq-tip-active');
    current.removeAttribute('aria-describedby');
    tip.classList.remove('bq-tip-show');
    current = null;
  }

  for (const el of document.querySelectorAll('[data-bq-interactive] [data-tip]')) {
    el.setAttribute('tabindex', '0');
    // hover only for real mice: on touch the tap sequence fires a synthetic
    // mouseenter before click, which would show then immediately toggle off
    el.addEventListener('pointerenter', (e) => {
      if (e.pointerType === 'mouse') show(el);
    });
    el.addEventListener('pointerleave', (e) => {
      if (e.pointerType === 'mouse') hide();
    });
    el.addEventListener('focus', () => show(el));
    el.addEventListener('blur', hide);
    el.addEventListener('click', () => (current === el ? hide() : show(el)));
    el.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') hide();
    });
  }

  window.addEventListener(
    'scroll',
    () => {
      // keyboard focus can scroll the node into view right after showing:
      // follow the node instead of hiding in that case
      if (current && current === document.activeElement) {
        place(current);
        return;
      }
      hide();
    },
    { passive: true }
  );
})();
