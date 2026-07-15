(() => {
      const waiting = document.getElementById('bq-waiting');
      const active = document.getElementById('bq-active');
      const done = document.getElementById('bq-done');
      const dlqSlot = document.getElementById('bq-dlq-slot');
      if (!waiting) return;

      const POOL = [
        ['send-email', 5], ['charge-card', 9], ['resize-image', 2], ['render-pdf', 3],
        ['sync-crm', 1], ['daily-digest', 6], ['webhook-post', 4], ['train-model', 7],
      ];
      const rnd = (a, b) => a + Math.random() * (b - a);
      const counts = (col) => col.querySelectorAll('.bq-simchip').length;
      const refresh = () => {
        for (const col of [waiting, active, done]) {
          col.querySelector('.bq-count').textContent = String(counts(col));
        }
      };

      function chip(name, pri, attempt) {
        const el = document.createElement('span');
        el.className = 'bq-simchip';
        el.dataset.state = 'waiting';
        el.dataset.pri = String(pri);
        const label = attempt > 1 ? `retry ${attempt}/3` : `p:${pri}`;
        el.innerHTML = `${name} <span class="bq-pri">${label}</span><i class="bq-prog"></i>`;
        el._name = name; el._pri = pri; el._attempt = attempt;
        return el;
      }

      function enqueue(el) {
        // priority insert: higher priority jumps the line
        const siblings = [...waiting.querySelectorAll('.bq-simchip')];
        const after = siblings.find((s) => Number(s.dataset.pri) < el._pri);
        waiting.insertBefore(el, after ?? null);
        el.dataset.state = 'waiting';
        refresh();
      }

      function spawn() {
        if (counts(waiting) < 5) {
          const [name, pri] = POOL[Math.floor(Math.random() * POOL.length)];
          enqueue(chip(name, pri, 1));
        }
        setTimeout(spawn, rnd(500, 1200));
      }

      function pull() {
        if (counts(active) < 2) {
          const next = waiting.querySelector('.bq-simchip');
          if (next) {
            active.appendChild(next);
            next.dataset.state = 'active';
            const dur = rnd(2200, 4200);
            next.style.setProperty('--dur', dur + 'ms');
            refresh();
            setTimeout(() => settle(next), dur);
          }
        }
        setTimeout(pull, 450);
      }

      function settle(el) {
        const fails = Math.random() < 0.16;
        if (!fails) return complete(el);
        el.dataset.state = 'fail';
        setTimeout(() => {
          if (el._attempt >= 3) return toDlq(el);
          dlqSlot.appendChild(el);
          el.querySelector('.bq-pri').textContent = `backoff · retry ${el._attempt + 1}/3`;
          refresh();
          setTimeout(() => {
            el._attempt += 1;
            el.querySelector('.bq-pri').textContent = `retry ${el._attempt}/3`;
            enqueue(el);
          }, rnd(1500, 2600));
        }, 550);
      }

      function toDlq(el) {
        dlqSlot.appendChild(el);
        el.querySelector('.bq-pri').textContent = 'kept · retryable';
        refresh();
        setTimeout(() => { el.classList.add('bq-fade'); setTimeout(() => el.remove(), 700); }, 6000);
      }

      function complete(el) {
        el.dataset.state = 'done';
        done.insertBefore(el, done.querySelector('.bq-simchip'));
        el.querySelector('.bq-pri').textContent = rnd(2, 9).toFixed(0) + 'ms';
        refresh();
        const chips = [...done.querySelectorAll('.bq-simchip:not(.bq-fade)')];
        for (const extra of chips.slice(4)) {
          extra.classList.add('bq-fade');
          setTimeout(() => { extra.remove(); refresh(); }, 650);
        }
      }

      // reduced motion: render one representative still frame, no timers
      if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
        enqueue(chip('charge-card', 9, 1));
        enqueue(chip('send-email', 5, 1));
        const act = chip('render-pdf', 3, 1);
        active.appendChild(act);
        act.dataset.state = 'active';
        for (const [name, pri, ms] of [['resize-image', 2, '4ms'], ['webhook-post', 4, '7ms']]) {
          const d = chip(name, pri, 1);
          d.dataset.state = 'done';
          d.querySelector('.bq-pri').textContent = ms;
          done.appendChild(d);
        }
        const kept = chip('sync-crm', 1, 3);
        kept.dataset.state = 'fail';
        kept.querySelector('.bq-pri').textContent = 'kept · retryable';
        dlqSlot.appendChild(kept);
        refresh();
        const caption = document.querySelector('.bq-pipeline-caption');
        if (caption) caption.textContent = 'snapshot: higher priority jobs jump the line, failures retry with backoff, exhausted retries land in the dead letter queue';
        return;
      }

      // seed and start
      enqueue(chip('send-email', 5, 1));
      enqueue(chip('resize-image', 2, 1));
      spawn();
      pull();
    })();
