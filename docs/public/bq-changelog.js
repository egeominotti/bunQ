// Changelog page enhancements.
//
// The changelog is authored as one flat markdown file (every release since the
// first, 200+ and growing) because the release ritual appends to it. Rendered
// raw that is an unreadable wall. This script upgrades it in the browser into a
// filterable release feed: a summary strip, search, month and change-type
// filters, and one card per release. It is pure progressive enhancement — with
// JS off the page still renders every release in full.
(() => {
  const content = document.querySelector('.sl-markdown-content');
  if (!content) return;

  // `## [2.8.53] - 2026-07-31`. Anything else (e.g. `## Unreleased`) is pinned
  // to the top of the feed and never filtered out.
  const HEADING = /^\[?(\d+\.\d+\.\d+[^\]\s]*)\]?\s*[-–—]\s*(\d{4})-(\d{2})-(\d{2})/;

  // Change-type buckets. The changelog uses a wider vocabulary than
  // keep-a-changelog (Tests, Docs, Internal, Notes), all of which collapse into
  // "Other" so the filter stays legible.
  const BUCKETS = [
    { id: 'added', label: 'Added', match: /^added/ },
    { id: 'fixed', label: 'Fixed', match: /^fixed/ },
    { id: 'changed', label: 'Changed', match: /^changed/ },
    { id: 'performance', label: 'Performance', match: /^(performance|perf)/ },
    { id: 'security', label: 'Security', match: /^security/ },
    { id: 'removed', label: 'Removed', match: /^removed/ },
  ];
  const bucketOf = (heading) => {
    const text = heading.trim().toLowerCase();
    return BUCKETS.find((b) => b.match.test(text))?.id ?? 'other';
  };

  const dateFmt = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  });
  const monthFmt = new Intl.DateTimeFormat('en-US', {
    month: 'long',
    timeZone: 'UTC',
  });
  const relFmt = new Intl.RelativeTimeFormat('en-US', { numeric: 'auto' });
  const DAY = 86400000;
  const relative = (time, now) => {
    const days = Math.round((time - now) / DAY);
    if (Math.abs(days) < 31) return relFmt.format(days, 'day');
    if (Math.abs(days) < 365) return relFmt.format(Math.round(days / 30), 'month');
    return relFmt.format(Math.round(days / 365), 'year');
  };

  // --- Group the flat document into one section per release ------------------

  // Starlight wraps each heading in `<div class="sl-heading-wrapper level-hN">`
  // together with its anchor link, so a release boundary is either a bare h2 or
  // a wrapper holding one.
  const headingIn = (node, level) => {
    if (node.tagName === level.toUpperCase()) return node;
    if (!node.classList?.contains('sl-heading-wrapper')) return null;
    return node.querySelector(level);
  };

  // Bail out *before* touching the DOM. The loop below is what turns a release
  // into a card, so returning halfway — a changelog whose headings stopped
  // matching `HEADING` — would leave the card chrome on the page with none of
  // the controls that justify it.
  const children = Array.from(content.children);
  const parsable = children.filter((node) => {
    const h2 = headingIn(node, 'h2');
    return h2 && HEADING.test(h2.textContent || '');
  });
  if (parsable.length < 2) return;

  const releases = [];
  let current = null;
  let record = null;
  for (const node of children) {
    const h2 = headingIn(node, 'h2');
    const h3 = h2 ? null : headingIn(node, 'h3');
    if (h2) {
      const match = HEADING.exec(h2.textContent || '');
      current = document.createElement('section');
      current.className = 'bq-cl-release';
      record = {
        el: current,
        heading: h2,
        wrapper: node,
        version: match?.[1] ?? null,
        month: match ? `${match[2]}-${match[3]}` : 'pinned',
        time: match ? Date.UTC(+match[2], +match[3] - 1, +match[4]) : null,
        types: new Set(),
      };
      current.dataset.month = record.month;
      if (record.version) current.dataset.version = record.version;
      node.replaceWith(current);
      current.append(node);
      releases.push(record);
    } else if (current) {
      if (h3) {
        const bucket = bucketOf(h3.textContent || '');
        record.types.add(bucket);
        h3.dataset.bqType = bucket;
      }
      current.append(node);
    }
  }

  const dated = releases.filter((r) => r.version && r.time !== null);

  // Search matches the whole rendered release, so cache the text once.
  for (const release of releases) release.text = (release.el.textContent || '').toLowerCase();

  const counts = new Map();
  for (const release of dated) counts.set(release.month, (counts.get(release.month) || 0) + 1);
  const months = [...counts.keys()].sort().reverse();
  const newest = months[0];

  // --- Rewrite each release heading into a card header ------------------------

  const now = Date.now();
  for (const release of dated) {
    const iso = new Date(release.time).toISOString().slice(0, 10);
    const isLatest = release === dated[0];
    release.heading.innerHTML = '';
    release.heading.className = 'bq-cl-head';
    release.wrapper.classList.add('bq-cl-headwrap');
    const badge = document.createElement('span');
    badge.className = 'bq-cl-badge';
    badge.textContent = release.version;
    const when = document.createElement('time');
    when.className = 'bq-cl-date';
    when.dateTime = iso;
    when.textContent = dateFmt.format(release.time);
    const age = document.createElement('span');
    age.className = 'bq-cl-age';
    age.textContent = relative(release.time, now);
    release.heading.append(badge, when, age);
    if (isLatest) {
      const latest = document.createElement('span');
      latest.className = 'bq-cl-latest';
      latest.textContent = 'Latest';
      release.heading.append(latest);
    }
    if (release.types.size) {
      const tags = document.createElement('p');
      tags.className = 'bq-cl-tags';
      for (const bucket of BUCKETS) {
        if (!release.types.has(bucket.id)) continue;
        const tag = document.createElement('span');
        tag.className = 'bq-cl-tag';
        tag.dataset.type = bucket.id;
        tag.textContent = bucket.label;
        tags.append(tag);
      }
      if (tags.childElementCount) release.wrapper.after(tags);
    }
  }

  // --- Summary strip ---------------------------------------------------------

  const last90 = dated.filter((r) => now - r.time <= 90 * DAY).length;
  const stats = document.createElement('div');
  stats.className = 'bq-cl-stats';
  const stat = (key, value) => {
    const cell = document.createElement('div');
    cell.innerHTML = `<span class="bq-cl-k">${key}</span><span class="bq-cl-v"></span>`;
    cell.querySelector('.bq-cl-v').textContent = value;
    stats.append(cell);
  };
  stat('Latest release', dated[0].version);
  stat('Shipped', dateFmt.format(dated[0].time));
  stat('Total releases', String(dated.length));
  stat('Last 90 days', String(last90));

  // --- Controls --------------------------------------------------------------

  const controls = document.createElement('div');
  controls.className = 'bq-cl-controls';

  const search = document.createElement('input');
  search.type = 'search';
  search.className = 'bq-cl-search';
  search.placeholder = 'Search releases — version, feature, fix…';
  search.setAttribute('aria-label', 'Search releases');

  const monthPicker = document.createElement('select');
  monthPicker.className = 'bq-cl-month';
  monthPicker.setAttribute('aria-label', 'Filter by month');
  const all = document.createElement('option');
  all.value = 'all';
  all.textContent = `All releases (${dated.length})`;
  monthPicker.append(all);
  let group = null;
  for (const month of months) {
    const year = month.slice(0, 4);
    if (!group || group.label !== year) {
      group = document.createElement('optgroup');
      group.label = year;
      monthPicker.append(group);
    }
    const option = document.createElement('option');
    option.value = month;
    option.textContent = `${monthFmt.format(Date.UTC(+year, +month.slice(5) - 1, 1))} (${counts.get(month)})`;
    group.append(option);
  }

  const types = document.createElement('div');
  types.className = 'bq-cl-types';
  types.setAttribute('role', 'group');
  types.setAttribute('aria-label', 'Filter by change type');
  const present = BUCKETS.filter((b) => dated.some((r) => r.types.has(b.id)));
  for (const bucket of [{ id: 'all', label: 'All changes' }, ...present]) {
    const button = document.createElement('button');
    button.type = 'button';
    button.dataset.type = bucket.id;
    button.setAttribute('aria-pressed', String(bucket.id === 'all'));
    button.textContent = bucket.label;
    types.append(button);
  }

  const status = document.createElement('p');
  status.className = 'bq-cl-count';
  status.setAttribute('role', 'status');
  status.setAttribute('aria-live', 'polite');

  const empty = document.createElement('p');
  empty.className = 'bq-cl-empty';
  empty.hidden = true;
  empty.textContent = 'No releases match these filters.';

  controls.append(search, monthPicker, types);

  // --- Filtering -------------------------------------------------------------

  const state = { month: newest, type: 'all', query: '' };

  const apply = ({ silent = false } = {}) => {
    const query = state.query.trim().toLowerCase();
    // A query is a search across the whole history, so it overrides the month.
    const month = query ? 'all' : state.month;
    monthPicker.disabled = Boolean(query);
    let shown = 0;
    for (const release of releases) {
      const pinned = release.month === 'pinned';
      const visible =
        pinned ||
        ((month === 'all' || release.month === month) &&
          (state.type === 'all' || release.types.has(state.type)) &&
          (!query || release.text.includes(query)));
      release.el.hidden = !visible;
      if (visible && !pinned) shown += 1;
    }
    empty.hidden = shown > 0;
    for (const button of types.querySelectorAll('button')) {
      button.setAttribute('aria-pressed', String(button.dataset.type === state.type));
    }
    monthPicker.value = month;
    const scope = query
      ? `matching “${state.query.trim()}”`
      : month === 'all'
        ? 'in total'
        : 'in this month';
    status.textContent = `${shown} of ${dated.length} releases ${scope}`;
    if (silent) return;
    const url = new URL(location.href);
    const sync = (key, value, fallback) => {
      if (value === fallback) url.searchParams.delete(key);
      else url.searchParams.set(key, value);
    };
    sync('month', state.month, newest);
    sync('type', state.type, 'all');
    sync('q', state.query.trim(), '');
    history.replaceState(null, '', url);
  };

  monthPicker.addEventListener('change', () => {
    state.month = monthPicker.value;
    apply();
  });

  types.addEventListener('click', (event) => {
    const button = event.target.closest('button');
    if (!button) return;
    state.type = button.dataset.type;
    apply();
  });

  let debounce = 0;
  search.addEventListener('input', () => {
    clearTimeout(debounce);
    debounce = setTimeout(() => {
      state.query = search.value;
      apply();
    }, 120);
  });
  search.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape' || !search.value) return;
    search.value = '';
    state.query = '';
    apply();
  });
  addEventListener('keydown', (event) => {
    if (event.key !== '/' || event.metaKey || event.ctrlKey) return;
    const tag = document.activeElement?.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || document.activeElement?.isContentEditable) return;
    event.preventDefault();
    search.focus();
  });

  // --- Deep links ------------------------------------------------------------

  // A fragment is whatever was pasted into the address bar, and
  // `decodeURIComponent` throws on a malformed escape like `#%`. Unguarded that
  // exception lands between the card rewrite and the control insertion below,
  // leaving 229 styled releases and no way to filter them.
  const hashId = () => {
    const raw = location.hash.slice(1);
    if (!raw) return '';
    try {
      return decodeURIComponent(raw);
    } catch {
      return raw;
    }
  };

  const sectionOfHash = () => {
    const id = hashId();
    if (!id) return null;
    return content.querySelector(`[id="${CSS.escape(id)}"]`)?.closest('.bq-cl-release') ?? null;
  };

  // Reaching the fragment outranks every filter. "On this page" lists all 229
  // releases whatever is on screen, and a shared link carries its own filters,
  // so a filter that hides the target gives way instead of swallowing the click
  // and leaving an empty page behind.
  const revealHash = () => {
    const section = sectionOfHash();
    if (!section || !section.hidden) return false;
    state.query = '';
    search.value = '';
    state.type = 'all';
    const month = section.dataset.month;
    state.month = month && month !== 'pinned' ? month : 'all';
    return true;
  };

  const scrollToHash = () => {
    const id = hashId();
    if (id) document.querySelector(`[id="${CSS.escape(id)}"]`)?.scrollIntoView();
  };

  const params = new URL(location.href).searchParams;
  const requestedMonth = params.get('month');
  if (requestedMonth === 'all' || counts.has(requestedMonth)) state.month = requestedMonth;
  const requestedType = params.get('type');
  if (present.some((b) => b.id === requestedType)) state.type = requestedType;
  state.query = params.get('q') ?? '';
  search.value = state.query;
  apply({ silent: true });
  // Second pass: the filters have to run once before it is knowable whether they
  // hide the target. Not silent, so the URL stops advertising filters that were
  // just dropped.
  if (revealHash()) apply();

  addEventListener('hashchange', () => {
    if (revealHash()) apply();
    scrollToHash();
  });

  const hero = content.querySelector('.bq-hero');
  const anchor = hero ?? releases[0]?.el;
  if (!anchor) return;
  anchor[hero ? 'after' : 'before'](stats);
  stats.after(controls);
  controls.after(status);
  status.after(empty);

  // The browser already tried, and failed, to reach a filtered-out anchor.
  requestAnimationFrame(scrollToHash);
})();
