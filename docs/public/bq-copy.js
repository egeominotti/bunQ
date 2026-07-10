// Homepage conversion helpers: click-to-copy install chips + live social proof.

// Click-to-copy for the install command chips.
document.querySelectorAll('.bq-hero-install').forEach((el) => {
  el.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(el.textContent.trim());
      el.classList.add('bq-copied');
      setTimeout(() => el.classList.remove('bq-copied'), 1600);
    } catch {
      /* clipboard unavailable (http, old browser): selection still works */
    }
  });
});

// Live GitHub star count in the "Star on GitHub" buttons.
// Falls back silently to the static "Star" label on any failure.
(async () => {
  try {
    const res = await fetch('https://api.github.com/repos/egeominotti/bunqueue');
    if (!res.ok) return;
    const { stargazers_count: stars } = await res.json();
    if (!stars) return;
    const label =
      stars >= 1000 ? `${(stars / 1000).toFixed(1).replace(/\.0$/, '')}k` : String(stars);
    document.querySelectorAll('.bq-stars-slot').forEach((el) => {
      el.textContent = label;
    });
  } catch {
    /* offline or rate-limited: keep static label */
  }
})();

// Live npm weekly downloads in the proof strip; hidden unless the fetch succeeds.
(async () => {
  try {
    const res = await fetch('https://api.npmjs.org/downloads/point/last-week/bunqueue');
    if (!res.ok) return;
    const { downloads } = await res.json();
    if (!downloads || downloads < 1000) return; // only show once it reads as traction
    const label =
      downloads >= 1000
        ? `${(downloads / 1000).toFixed(1).replace(/\.0$/, '')}k`
        : String(downloads);
    const count = document.getElementById('bq-npm-count');
    const wrap = document.getElementById('bq-npm-proof');
    if (count && wrap) {
      count.textContent = label;
      wrap.hidden = false;
    }
  } catch {
    /* keep hidden */
  }
})();
