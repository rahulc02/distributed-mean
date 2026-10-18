// The dashboard holds no state of its own: every render is a pure function of
// the latest snapshot the server pushed over Server-Sent Events.

const $ = (selector) => document.querySelector(selector);

const esc = (value) =>
  String(value ?? '').replace(/[&<>"']/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[ch]);

const plural = (n, one, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;
const fmt = (n) => Number(n ?? 0).toLocaleString('en');

// ------------------------------------------------------------ views (pure)

export function fleetView(workers) {
  const cells = workers.workers.map((w) => {
    const state = w.status === 'RUNNING' ? (w.busy ? 'busy' : 'idle') : w.status.toLowerCase();
    const label = { busy: 'busy', idle: 'idle', starting: 'starting', draining: 'draining' }[state];
    return `<li class="cell ${state}" title="${esc(w.id)}: ${label}"><span class="visually-hidden">${esc(w.id)} ${label}</span></li>`;
  });
  const live = workers.starting + workers.running;
  for (let i = live; i < workers.desired; i++) cells.push('<li class="cell wanted" title="requested, not started yet"></li>');
  return cells.join('');
}

export function fleetCaption(w) {
  if (w.desired === 0 && w.running === 0) return 'No workers. Set a target to start processing.';
  const parts = [`${w.busy} busy`, `${w.free} free`];
  if (w.starting) parts.push(`${w.starting} starting`);
  if (w.draining) parts.push(`${w.draining} draining`);
  return `${parts.join(', ')}. Target ${w.desired}.`;
}

export function queueView(queue) {
  if (!queue || !queue.tasks) return '<div><dt>Queue statistics unavailable</dt><dd>–</dd></div>';
  const rate = queue.tasks.ackRate ? queue.tasks.ackRate.toFixed(1) : '0';
  return [
    ['Waiting', fmt(queue.tasks.waiting), ''],
    ['In progress', fmt(queue.tasks.inProgress), ''],
    ['Tasks per second', rate, ''],
    ['Dead-lettered', fmt(queue.deadLettered), queue.deadLettered ? 'warn' : ''],
  ]
    .map(([label, value, cls]) => `<div class="${cls}"><dt>${label}</dt><dd>${value}</dd></div>`)
    .join('');
}

const STATUS_TEXT = { GENERATING: 'Generating files', NEW: 'Queued', RUNNING: 'Running', DONE: 'Done', FAILED: 'Failed' };

function eta(seconds) {
  if (seconds == null) return '';
  if (seconds < 60) return `about ${seconds} s left`;
  return `about ${Math.round(seconds / 60)} min left`;
}

export function jobsView(jobs) {
  if (!jobs.length) return '<tr><td colspan="5" class="empty">No jobs yet. Start one with the form above.</td></tr>';
  return jobs
    .map((j) => {
      const pct = Math.round(j.progress * 100);
      const detail =
        j.status === 'RUNNING' ? `${pct}%, ${fmt(j.tasksDone)} of ${fmt(j.totalTasks)} tasks${j.etaSeconds != null ? `, ${eta(j.etaSeconds)}` : ''}`
        : j.status === 'DONE' ? `${fmt(j.totalTasks)} tasks`
        : j.status === 'FAILED' ? esc(j.error ?? 'failed')
        : '';
      const result = j.status === 'DONE' ? `<a href="/jobs/${esc(j.id)}/result" download>Download CSV</a>` : '';
      return `<tr>
        <td class="job-id" title="${esc(j.id)}">${esc(j.id.slice(0, 8))}</td>
        <td>${fmt(j.f)} × ${fmt(j.c)}</td>
        <td class="status-${esc(j.status)}">${STATUS_TEXT[j.status] ?? esc(j.status)}</td>
        <td><div class="bar${j.status === 'DONE' ? ' done' : j.status === 'FAILED' ? ' failed' : ''}" role="progressbar" aria-valuenow="${pct}" aria-valuemin="0" aria-valuemax="100"><span style="width:${pct}%"></span></div>
            <div class="bar-label">${detail}</div></td>
        <td>${result}</td></tr>`;
    })
    .join('');
}

export function logItem(e) {
  const time = new Date(e.at).toLocaleTimeString('en-GB', { hour12: false });
  const job = e.jobId ? `, job ${esc(e.jobId.slice(0, 8))}` : '';
  return `<li><span class="message">${esc(e.message)}</span><time datetime="${esc(e.at)}">${time}</time>
    <span class="source">${esc(e.source)}${job}</span></li>`;
}

// ------------------------------------------------------------ wiring

const MAX_LOG = 100;
let workersTarget = null;

function render(topic, data) {
  if (topic === 'system') {
    $('#fleet').innerHTML = fleetView(data.workers);
    $('#fleet-caption').textContent = fleetCaption(data.workers);
    $('#queue').innerHTML = queueView(data.queue);
    if (workersTarget === null || document.activeElement !== $('#w')) {
      workersTarget = data.workers.desired;
      $('#w').value = workersTarget;
    }
  } else if (topic === 'jobs') {
    $('#jobs').innerHTML = jobsView(data);
  } else if (topic === 'log') {
    $('#log').insertAdjacentHTML('afterbegin', logItem(data));
    while ($('#log').children.length > MAX_LOG) $('#log').lastElementChild.remove();
  }
}

function connect() {
  const state = $('#link-state');
  const source = new EventSource('/events'); // reconnects on its own
  source.onopen = () => { state.textContent = 'Live'; state.className = 'link-state live'; };
  source.onerror = () => { state.textContent = 'Reconnecting'; state.className = 'link-state lost'; };
  for (const topic of ['system', 'jobs', 'log']) {
    source.addEventListener(topic, (e) => render(topic, JSON.parse(e.data)));
  }
}

async function submit(form, request) {
  const button = form.querySelector('button');
  const error = $('#form-error');
  button.disabled = true;
  error.hidden = true;
  try {
    const res = await request();
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      const issue = body.issues?.[0];
      throw new Error(issue ? `${issue.path.join('.')}: ${issue.message}` : body.error ?? `Request failed (${res.status})`);
    }
  } catch (err) {
    error.textContent = err.message;
    error.hidden = false;
  } finally {
    button.disabled = false;
  }
}

const json = (method, url, body) =>
  fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });

$('#job-form').addEventListener('submit', (e) => {
  e.preventDefault();
  submit(e.target, () => json('POST', '/jobs', { f: Number($('#f').value), c: Number($('#c').value) }));
});

$('#workers-form').addEventListener('submit', (e) => {
  e.preventDefault();
  submit(e.target, () => json('PUT', '/workers', { count: Number($('#w').value) }));
});

// ------------------------------------------------------------ demo mode (?demo): sample data, no server

function demo() {
  const now = Date.now();
  const iso = (msAgo) => new Date(now - msAgo).toISOString();
  const workers = ['a1', 'b2', 'c3', 'd4', 'e5', 'f6', 'g7'].map((id, i) => ({
    id: `worker-${id}`, status: i === 6 ? 'DRAINING' : i === 5 ? 'STARTING' : 'RUNNING', busy: i < 4,
  }));
  render('system', {
    workers: { desired: 8, starting: 1, running: 5, draining: 1, busy: 4, free: 1, workers },
    queue: { tasks: { waiting: 1240, inProgress: 4, ackRate: 37.4 }, deadLettered: 0 },
    jobs: {},
  });
  render('jobs', [
    { id: '9c41e2aa-0000', f: 100000, c: 1000, status: 'RUNNING', progress: 0.42, tasksDone: 10480, totalTasks: 25000, etaSeconds: 95 },
    { id: '5b7d0f13-0000', f: 2000, c: 50000, status: 'GENERATING', progress: 0, tasksDone: 0, totalTasks: null },
    { id: '1f0e9d88-0000', f: 12, c: 3, status: 'DONE', progress: 1, tasksDone: 4, totalTasks: 4 },
    { id: '77aa21c0-0000', f: 500, c: 2000, status: 'FAILED', progress: 0.3, error: 'task 77aa21c0:1:42 dead-lettered' },
  ]);
  [
    ['api', null, 'desired workers set to 8', 60000],
    ['supervisor', null, 'worker worker-f6 started', 55000],
    ['coordinator', '1f0e9d88', 'job done', 40000],
    ['api', '5b7d0f13', 'job created: F=2000, C=50000', 20000],
    ['supervisor', null, 'worker worker-g7 draining', 5000],
  ].forEach(([source, jobId, message, ago]) => render('log', { source, jobId, message, at: iso(ago) }));
  const state = $('#link-state');
  state.textContent = 'Demo data';
}

if (new URLSearchParams(location.search).has('demo')) demo();
else connect();
