// :has() fallback for Safari 15.0-15.3 and Firefox 115-120: mirror the
// main:has(.bq-hero) selectors with a class so the custom hero pages
// still hide Starlight's auto title and go full width there.
(() => {
  try {
    if (CSS.supports('selector(:has(*))')) return;
  } catch {
    /* CSS.supports missing: fall through and add the class */
  }
  const main = document.querySelector('main');
  if (main && main.querySelector('.bq-hero')) main.classList.add('bq-has-hero');
})();
