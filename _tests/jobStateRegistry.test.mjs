// tests/jobStateRegistry.full.test.mjs
import JobStateRegistry from '../jobStateRegistry.mjs';

jest.setTimeout(20000);

describe('JobStateRegistry comprehensive unit tests', () => {
  let reg;
  beforeEach(() => {
    reg = new JobStateRegistry({ workerId: 'reg-full' });
  });

  test('createJob, getJob, getTask, indices and tags work', () => {
    reg.createJob('J1', [{ taskId: 't1' }, { taskId: 't2', dependencies: ['t1'] }], { meta: 1 });
    const j = reg.getJob('J1');
    expect(j.jobId).toBe('J1');
    expect(j.totalTasks).toBe(2);
    expect(j.tasks.t2.dependencies).toContain('t1');

    reg.addTag('J1', 'blue');
    const byTag = reg.getJobsByTag('blue');
    expect(byTag.some(x => x.jobId === 'J1')).toBe(true);
    reg.removeTag('J1', 'blue');
    expect(reg.getJobsByTag('blue').length).toBe(0);
  });

  test('transitions enforce rules and counters update', () => {
    reg.createJob('J2', [{ taskId: 'a' }]);
    expect(() => reg.markTaskCompleted('a')).toThrow();
    reg.markTaskRunning('a', 'w1');
    expect(reg.getTask('a').status).toBe('RUNNING');
    reg.markTaskFailed('a', { message: 'boom' });
    expect(reg.getTask('a').status).toBe('FAILED');
    reg.retryTask('a');
    expect(reg.getTask('a').status).toBe('PENDING');
    expect(reg.getTask('a').retries).toBeGreaterThanOrEqual(1);
  });

  test('getChangeBatch coalescing preserves failure history and merges correctly', () => {
    reg.createJob('J3', [{ taskId: 'x' }]);
    reg.markTaskRunning('x', 'w1');
    reg.markTaskFailed('x', { message: 'err1', code: 'E1' });
    reg.retryTask('x');
    reg.markTaskFailed('x', { message: 'err2', code: 'E2' });
    reg.retryTask('x');
    reg.markTaskRunning('x', 'w2');

    const batch = reg.getChangeBatch(0, { maxEvents: 100, maxBytes: 1024 * 20, coalesce: true });
    expect(batch.meta.count).toBeGreaterThanOrEqual(1);
    const ev = batch.events.find(e => e.taskId === 'x');
    expect(ev).toBeDefined();
    // last status should be RUNNING
    expect(ev.payload.status).toBe('RUNNING');
    // retries should be >= 2
    expect(Number(ev.payload.retries || 0)).toBeGreaterThanOrEqual(2);
    // lastErrorHistory should contain previous errors
    expect(Array.isArray(ev.payload.lastErrorHistory)).toBe(true);
    expect(ev.payload.lastErrorHistory.length).toBeGreaterThanOrEqual(1);
  });

  test('exportState returns only state and exportLog returns changeLog', () => {
    reg.createJob('J4', [{ taskId: 't' }]);
    const s = reg.exportState();
    expect(s.state).toBeDefined();
    expect(s.changeLog).toBeUndefined();
    const l = reg.exportLog();
    expect(Array.isArray(l.changeLog)).toBe(true);
    expect(typeof l.sequence).toBe('number');
  });

  test('pruneCompletedJobs removes old completed jobs', () => {
    reg.createJob('J5', [{ taskId: 'a' }]);
    reg.markTaskRunning('a');
    reg.markTaskCompleted('a');
    // artificially set updatedAt to old timestamp
    const job = reg.getJob('J5');
    // internal access not available; use pruneCompletedJobs with small threshold
    const pruned = reg.pruneCompletedJobs(-1); // olderThanMs negative will prune immediately
    expect(pruned).toBeGreaterThanOrEqual(1);
  });
});
