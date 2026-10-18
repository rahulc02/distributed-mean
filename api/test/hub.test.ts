import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Hub } from '../src/dashboard/hub.js';
import { withProgress } from '../src/dashboard/poller.js';
import type { JobView } from '../src/repos/jobRepo.js';

test('publishes only on change and replays the latest snapshot to new viewers', () => {
  const hub = new Hub();
  const seen: [string, unknown][] = [];
  hub.subscribe((t, d) => seen.push([t, d]));
  assert.equal(hub.publish('system', { a: 1 }), true);
  assert.equal(hub.publish('system', { a: 1 }), false);
  assert.equal(seen.length, 1);

  const late: [string, unknown][] = [];
  hub.subscribe((t, d) => late.push([t, d]));
  assert.deepEqual(late, [['system', { a: 1 }]]);
});

test('keeps a bounded log history', () => {
  const hub = new Hub(3);
  for (let i = 0; i < 5; i++) hub.append({ id: i });
  const seen: unknown[] = [];
  hub.subscribe((_t, d) => seen.push(d));
  assert.deepEqual(seen, [{ id: 2 }, { id: 3 }, { id: 4 }]);
});

test('a broken subscriber is dropped without affecting others', () => {
  const hub = new Hub();
  hub.subscribe(() => {
    throw new Error('closed');
  });
  let ok = 0;
  hub.subscribe(() => ok++);
  hub.publish('jobs', []);
  assert.equal(ok, 1);
  assert.equal(hub.size, 1);
});

const job = (over: Partial<JobView>): JobView => ({
  id: 'J', f: 12, c: 2, status: 'RUNNING', kLeaf: 5, kMerge: 5, tasksDone: 0, totalTasks: 4,
  operandsDone: 0, totalOperands: 15, resultKey: null, error: null, createdAt: '2026-01-01T00:00:00Z',
  genStartedAt: null, genFinishedAt: null, startedAt: '2026-01-01T00:00:00.000Z', finishedAt: null, ...over,
});

test('progress and ETA come from exact operand totals', () => {
  const now = Date.parse('2026-01-01T00:00:10.000Z');
  const row = withProgress(job({ operandsDone: 5 }), now); // a third done after 10 s
  assert.equal(row.progress, 5 / 15);
  assert.equal(row.etaSeconds, 20);
  assert.equal(withProgress(job({ status: 'DONE', operandsDone: 15 }), now).progress, 1);
  assert.equal(withProgress(job({ operandsDone: 0 }), now).etaSeconds, null);
});
