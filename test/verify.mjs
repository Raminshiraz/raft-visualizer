/* Headless Raft invariant checker.
 *
 * Loads the SAME engine the browser runs (app.js), drives it under random
 * crashes, network partitions, per-link cuts and packet loss, and asserts
 * the four safety properties from the Raft paper after every 40ms tick.
 *
 *   run with:  npm test
 */
import fs from 'node:fs';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
let code = fs.readFileSync(path.join(here, '..', 'app.js'), 'utf8');
code += '\n;globalThis.__X={makeWorld,tick,SCENARIOS,quorum,broadcastAppendEntries,linkKey,LINK_CYCLE,'
      + 'makeTxnWorld,tickTxn,beginTxn,txnPhase,shardFate,coordFate,groupLeader,committedOn,TXN_SCENARIOS};';

// Minimal DOM/React stubs — we only exercise the simulation core.
const sandbox = {
  React: { useState: () => [0, () => {}], useRef: () => ({ current: null }),
           useEffect: () => {}, useCallback: f => f, createElement: () => ({}) },
  ReactDOM: { createRoot: () => ({ render: () => {} }) },
  document: { getElementById: () => ({}) },
  performance: { now: () => 0 },
  requestAnimationFrame: () => 0, cancelAnimationFrame: () => {},
  window: {}, console,
};
vm.createContext(sandbox);
vm.runInContext(code, sandbox);
const X = sandbox.__X;

/* ------------------------------------------------------------------ *
 *  Invariants (Raft paper §5, Figure 3)                               *
 * ------------------------------------------------------------------ */
function check(W, S, viol) {
  // Election Safety — at most one leader per term.
  for (const n of W.nodes) {
    if (n.state !== 'leader') continue;
    const prev = S.leaderOfTerm.get(n.currentTerm);
    if (prev !== undefined && prev !== n.id)
      viol.push(`ELECTION SAFETY: term ${n.currentTerm} had leader N${prev} and N${n.id}`);
    S.leaderOfTerm.set(n.currentTerm, n.id);
  }

  // Log Matching — same index + same term implies identical prefix.
  for (let i = 0; i < W.nodes.length; i++)
    for (let j = i + 1; j < W.nodes.length; j++) {
      const a = W.nodes[i].log, b = W.nodes[j].log;
      for (let k = Math.min(a.length, b.length) - 1; k >= 0; k--) {
        if (a[k].term !== b[k].term) continue;
        for (let p = 0; p <= k; p++)
          if (a[p].term !== b[p].term || a[p].value !== b[p].value)
            viol.push(`LOG MATCHING: N${W.nodes[i].id}/N${W.nodes[j].id} agree at index ${k + 1} but differ at ${p + 1}`);
        break;
      }
    }

  // State Machine Safety — a committed index never changes value.
  for (const n of W.nodes)
    for (let i = 1; i <= n.commitIndex; i++) {
      const e = n.log[i - 1];
      if (!e) { viol.push(`COMMIT GAP: N${n.id} commitIndex ${n.commitIndex} > log ${n.log.length}`); break; }
      const seen = S.committed.get(i);
      if (seen === undefined) S.committed.set(i, { term: e.term, value: e.value, by: n.id });
      else if (seen.term !== e.term || seen.value !== e.value)
        viol.push(`STATE MACHINE SAFETY: index ${i} committed as ${seen.value}@t${seen.term} by N${seen.by}, but N${n.id} holds ${e.value}@t${e.term}`);
    }

  // Leader Completeness — a leader holds every committed entry.
  const L = W.nodes.find(n => n.state === 'leader');
  if (L) for (const [i, e] of S.committed)
    if (L.log.length >= i && (L.log[i - 1].term !== e.term || L.log[i - 1].value !== e.value))
      viol.push(`LEADER COMPLETENESS: leader N${L.id} lost committed index ${i}`);
}

/* ------------------------------------------------------------------ *
 *  Harness                                                            *
 * ------------------------------------------------------------------ */
let failures = 0;

function run(name, build, ms, chaos) {
  const W = build();
  const S = { leaderOfTerm: new Map(), committed: new Map() };
  const viol = [];
  let t = 0, everLed = false, commits = 0;

  while (t < ms) {
    X.tick(W, 40); t += 40;
    if (chaos) chaos(W, t);
    check(W, S, viol);
    if (W.nodes.some(n => n.state === 'leader')) everLed = true;
    commits = Math.max(commits, ...W.nodes.map(n => n.commitIndex));
    if (viol.length) break;
  }

  const ok = viol.length === 0;
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name.padEnd(36)} leader=${everLed ? 'y' : 'n'}  maxCommit=${commits}`);
  viol.slice(0, 3).forEach(v => console.log('        ! ' + v));
  return ok;
}

const cfg = o => ({ dropRate: 0, jitter: 0.12, prevote: false, noop: false, ...o });

/** Push a client command onto whoever is leader, if anyone is. */
const write = (W, tag) => {
  const L = W.nodes.find(n => n.state === 'leader');
  if (!L) return;
  L.log.push({ term: L.currentTerm, value: tag + L.log.length });
  X.broadcastAppendEntries(W, L);
};

/** Advance one random link through its cut/one-way/healthy cycle. */
const flipLink = (W, n) => {
  const a = Math.floor(Math.random() * n);
  let b = Math.floor(Math.random() * n); if (b === a) b = (b + 1) % n;
  const k = X.linkKey(a, b), next = X.LINK_CYCLE[W.links[k]];
  if (next === undefined) delete W.links[k]; else W.links[k] = next;
};

console.log('Raft safety verification — engine loaded from app.js\n');

run('healthy cluster + client writes', () => X.makeWorld(5, cfg()), 40000,
  (W, t) => { if (t % 1600 === 0) write(W, 'v'); });

run('random crash / restart', () => X.makeWorld(5, cfg({ jitter: 0.4 })), 60000, (W, t) => {
  if (t % 1200 === 0) {
    const n = W.nodes[Math.floor(Math.random() * W.nodes.length)];
    if (n.state === 'down') { n.state = 'follower'; n.timeout = 3000 + Math.random() * 3000; n.lastHeard = 6000; }
    else if (W.nodes.filter(x => x.state !== 'down').length > 3) n.state = 'down';
  }
  if (t % 900 === 0) write(W, 'c');
});

run('flapping network partitions', () => X.makeWorld(5, cfg({ jitter: 0.3 })), 60000, (W, t) => {
  if (t % 6000 === 0) {
    const k = Math.floor(Math.random() * 3);
    W.nodes.forEach((n, i) => n.partition = i < 5 - k ? 0 : 1);
  }
  if (t % 1000 === 0) write(W, 'p');
});

run('35% packet loss', () => X.makeWorld(5, cfg({ dropRate: 0.35, jitter: 0.5 })), 60000,
  (W, t) => { if (t % 1500 === 0) write(W, 'd'); });

run('random link cuts (incl. one-way)', () => X.makeWorld(5, cfg({ jitter: 0.3 })), 60000, (W, t) => {
  if (t % 2000 === 0) flipLink(W, 5);
  if (t % 1000 === 0) write(W, 'l');
});

// Everything at once. Safety must hold; liveness is NOT asserted here —
// under this much simultaneous damage Raft is expected to stop making
// progress rather than risk split brain.
run('links + partitions + crashes + loss', () => X.makeWorld(7, cfg({ dropRate: 0.2, jitter: 0.5 })), 80000, (W, t) => {
  if (t % 1800 === 0) flipLink(W, 7);
  if (t % 5000 === 0) W.nodes.forEach(n => n.partition = Math.random() < 0.3 ? 1 : 0);
  if (t % 3000 === 0) {
    const n = W.nodes[Math.floor(Math.random() * 7)];
    if (n.state === 'down') { n.state = 'follower'; n.timeout = 3000 + Math.random() * 3000; n.lastHeard = 6000; }
    else if (W.nodes.filter(x => x.state !== 'down').length > 4) n.state = 'down';
  }
  if (t % 1100 === 0) write(W, 'x');
});

// A node that can send but never receive must never win an election.
run('one-way isolated node cannot win', () => {
  const W = X.makeWorld(5, cfg());
  for (let j = 0; j < 4; j++) W.links[X.linkKey(j, 4)] = 'lo2hi';
  return W;
}, 60000, (W) => {
  if (W.nodes[4].state === 'leader') throw new Error('isolated node became leader');
});

for (const key of Object.keys(X.SCENARIOS))
  run('scenario: ' + key, () => X.SCENARIOS[key].build(cfg()), 45000,
    (W, t) => { if (t % 2500 === 0) write(W, 's'); });

// Safety must also hold with PreVote turned on.
run('prevote + links + crashes + loss', () => X.makeWorld(5, cfg({ prevote: true, dropRate: 0.2, jitter: 0.5 })), 80000, (W, t) => {
  if (t % 1800 === 0) flipLink(W, 5);
  if (t % 3000 === 0) {
    const n = W.nodes[Math.floor(Math.random() * 5)];
    if (n.state === 'down') { n.state = 'follower'; n.timeout = 3000 + Math.random() * 3000; n.lastHeard = 6000; }
    else if (W.nodes.filter(x => x.state !== 'down').length > 3) n.state = 'down';
  }
  if (t % 1100 === 0) write(W, 'q');
});

/* ------------------------------------------------------------------ *
 *  PreVote — the whole point is that a node the leader cannot reach    *
 *  must not be able to depose it. Assert that directly.                *
 * ------------------------------------------------------------------ */
function preVoteHoldsLeader(name, cutState, prevote, expectDisruption) {
  const W = X.makeWorld(5, cfg({ prevote }));
  let t = 0, L = null, baseTerm = 0, victim = null, deposed = false, maxTerm = 0;
  while (t < 60000) {
    X.tick(W, 40); t += 40;
    const cur = W.nodes.find(n => n.state === 'leader');
    if (!L && cur) {                       // once a leader exists, cut one wire
      L = cur; baseTerm = cur.currentTerm;
      victim = W.nodes.find(n => n.id !== L.id);
      W.links[X.linkKey(L.id, victim.id)] = cutState;
    }
    if (L) {
      if (cur && cur.id !== L.id) deposed = true;
      maxTerm = Math.max(maxTerm, ...W.nodes.map(n => n.currentTerm));
    }
  }
  const disrupted = deposed || maxTerm > baseTerm;
  const ok = disrupted === expectDisruption;
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name.padEnd(36)} term ${baseTerm}->${maxTerm}  ` +
    `leader ${deposed ? 'DEPOSED' : 'held'}  (expected ${expectDisruption ? 'disruption' : 'no disruption'})`);
}

// Both directions dead, and each one-way direction, must all be survivable.
preVoteHoldsLeader('prevote: cut link, leader holds', 'cut', true, false);
preVoteHoldsLeader('prevote: one-way lo2hi, holds', 'lo2hi', true, false);
preVoteHoldsLeader('prevote: one-way hi2lo, holds', 'hi2lo', true, false);
// Control: without PreVote the same cut MUST disrupt, or the test proves nothing.
preVoteHoldsLeader('no prevote: same cut disrupts', 'cut', false, true);

// A node that can send but never receive must not raise the cluster's term.
{
  const W = X.makeWorld(5, cfg({ prevote: true }));
  for (let j = 0; j < 4; j++) W.links[X.linkKey(j, 4)] = 'lo2hi';
  let t = 0;
  while (t < 60000) { X.tick(W, 40); t += 40; }
  const disruptor = W.nodes[4];
  const others = W.nodes.slice(0, 4).map(n => n.currentTerm);
  const ok = disruptor.currentTerm <= Math.max(...others) && disruptor.state !== 'leader';
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${'prevote: disruptor stays quiet'.padEnd(36)} ` +
    `N4 term=${disruptor.currentTerm} state=${disruptor.state}  cluster terms=[${others}]`);
}

// PreVote must not cost liveness. It legitimately costs one extra round trip
// (probe + reply) before the real election starts, so compare the median
// against the same cluster with PreVote off rather than against a constant.
{
  const recover = (prevote) => {
    const times = [];
    for (let i = 0; i < 60; i++) {
      const W = X.makeWorld(5, cfg({ prevote }));
      let t = 0;
      while (t < 30000 && !W.nodes.some(n => n.state === 'leader')) { X.tick(W, 40); t += 40; }
      const L = W.nodes.find(n => n.state === 'leader');
      L.state = 'down';
      let e = 0;
      while (e < 60000 && !W.nodes.some(n => n.state === 'leader' && n.id !== L.id)) { X.tick(W, 40); e += 40; }
      times.push(e);
    }
    times.sort((a, b) => a - b);
    return { med: times[30], max: times[times.length - 1] };
  };
  const off = recover(false), on = recover(true);
  // One extra RTT is 2*TRAVEL = 1.4s; allow generous slack, but a regression
  // that reintroduces wasted pre-vote rounds blows straight through this.
  const ok = on.med <= off.med + 4000 && on.max < 60000;
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${'prevote: re-elect after crash'.padEnd(36)} ` +
    `median ${(off.med / 1000).toFixed(1)}s -> ${(on.med / 1000).toFixed(1)}s  ` +
    `max ${(off.max / 1000).toFixed(1)}s -> ${(on.max / 1000).toFixed(1)}s`);
}

// Liveness: a healthy cluster must elect promptly, every time.
let slow = 0;
for (let i = 0; i < 40; i++) {
  const W = X.makeWorld(5, cfg());
  let t = 0;
  while (t < 30000 && !W.nodes.some(n => n.state === 'leader')) { X.tick(W, 40); t += 40; }
  if (t > 15000) slow++;
}
if (slow) failures++;
console.log(`${slow ? 'FAIL' : 'PASS'}  ${'liveness: 40 cold starts'.padEnd(36)} slow(>15s)=${slow}`);

/* ------------------------------------------------------------------ *
 *  Two-phase commit over Raft                                          *
 *                                                                      *
 *  Raft safety is asserted per group, unchanged. On top of that, the    *
 *  properties an atomic commit protocol owes you: everyone reaches the  *
 *  same outcome, the outcome never changes once it is durable, and      *
 *  nobody applies anything the coordinator has not committed.           *
 * ------------------------------------------------------------------ */
const txnCfg = o => cfg({ noop: true, ...o });
const shardsOf = T => T.groups.filter(g => g.role === 'shard');

function checkTxn(T, S, viol) {
  // Raft safety inside every group. The memory MUST be per group: check()'s
  // committed map is keyed by bare log index, so one shared S would conflate
  // index 3 of COORD with index 3 of SHARD B and fail for no reason.
  for (const g of T.groups) {
    if (!S.raft[g.id]) S.raft[g.id] = { leaderOfTerm: new Map(), committed: new Map() };
    check(g.W, S.raft[g.id], viol);
  }
  if (!T.txn) return;
  const id = T.txn.id;

  // Atomicity — no two shards may reach opposite outcomes.
  const applied = shardsOf(T).map(g => X.shardFate(T, g, id).applied).filter(Boolean);
  if (applied.some(d => d !== applied[0]))
    viol.push(`2PC ATOMICITY: ${id} applied as ${[...new Set(applied)].join(' and ')} on different shards`);

  // Decision stability — snapshot on first sight; it must never move.
  const dec = X.coordFate(T, id).decision;
  if (dec) {
    if (S.decision[id] === undefined) S.decision[id] = dec;
    else if (S.decision[id] !== dec)
      viol.push(`2PC DECISION STABILITY: ${id} committed as ${S.decision[id]}, now reads ${dec}`);
  }

  // No premature apply. Checked against the sticky snapshot, not the live
  // coordinator: a fresh coordinator leader can legitimately show a lower
  // commitIndex than the leader that committed the decision.
  for (const d of applied) {
    if (S.decision[id] === undefined)
      viol.push(`2PC PREMATURE APPLY: a shard applied ${d} for ${id} with no committed coordinator decision`);
    else if (S.decision[id] !== d)
      viol.push(`2PC PREMATURE APPLY: a shard applied ${d} for ${id}, but the decision was ${S.decision[id]}`);
  }

  // No double record — the dedupe guard is what stops a crash committing both
  // an abort and a commit for the same transaction.
  for (const g of T.groups)
    for (const n of g.W.nodes) {
      const c = {};
      for (const e of n.log) if (e.txn === id && e.rec) c[e.rec] = (c[e.rec] || 0) + 1;
      for (const r of ['begin', 'decision', 'prepared', 'novote', 'applied'])
        if (c[r] > 1) viol.push(`2PC DOUBLE RECORD: ${g.name} N${n.id} holds ${c[r]} ${r} records for ${id}`);
      if ((c.prepared || 0) + (c.novote || 0) > 1)
        viol.push(`2PC DOUBLE RECORD: ${g.name} N${n.id} holds both a yes and a no vote for ${id}`);
    }
}

function runTxn(name, build, ms, chaos, verdict) {
  const T = build();
  const S = { raft: {}, decision: {} };
  const viol = [];
  let t = 0;

  while (t < ms) {
    X.tickTxn(T, 40); t += 40;
    if (chaos) chaos(T, t);
    checkTxn(T, S, viol);
    if (viol.length) break;
  }

  const fates = shardsOf(T).map(g => X.shardFate(T, g));
  const info = {
    T, phase: X.txnPhase(T), shards: fates.length,
    locked: fates.filter(f => f.locked).length,
    applied: fates.filter(f => f.applied).length,
  };
  const v = verdict ? verdict(info) : { ok: true, text: '' };
  const ok = viol.length === 0 && v.ok;
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name.padEnd(36)} ${v.text}`);
  viol.slice(0, 3).forEach(v2 => console.log('        ! ' + v2));
  return ok;
}

const buildTxn = (coord, shards, o) => X.makeTxnWorld(coord, shards, txnCfg(o));
const tally = i => `phase=${i.phase}  applied=${i.applied}/${i.shards}  locked=${i.locked}`;

/** Fire `f` the first time `when` holds, once per run. */
const once = (T, key, when, f) => { if (!T[key] && when()) { f(); T[key] = true; } };
const allVotedYes = T => shardsOf(T).every(g => X.shardFate(T, g).vote === 'yes');

console.log('');

runTxn('2pc: happy path commits everywhere', () => buildTxn(3, [3, 3]), 60000,
  (T, t) => { if (t === 2000) X.beginTxn(T); },
  i => ({ ok: i.phase === 'committed' && i.applied === i.shards, text: tally(i) }));

runTxn('2pc: a NO vote aborts everywhere', () => {
  const T = buildTxn(3, [3, 3]); T.groups[2].willVote = 'no'; return T;
}, 60000,
  (T, t) => { if (t === 2000) X.beginTxn(T); },
  i => ({ ok: i.phase === 'aborted' && i.applied === i.shards, text: tally(i) }));

// The marquee case: the only node that ever spoke to the coordinator dies, and
// a replacement that never saw the request answers for it out of the log.
runTxn('2pc: shard leader dies, log answers', () => buildTxn(3, [3, 3]), 90000,
  (T, t) => {
    if (t === 2000) X.beginTxn(T);
    once(T, '_k', () => T.txn && X.shardFate(T, T.groups[1]).vote === 'yes' && X.groupLeader(T.groups[1]),
      () => { X.groupLeader(T.groups[1]).state = 'down'; });
  },
  i => ({ ok: i.T._k && i.phase === 'committed' && i.applied === i.shards, text: tally(i) }));

/* NEGATIVE CONTROL. Without this row, "replicated coord recovers" proves
   nothing: a transaction that would have finished anyway also finishes with
   replication on. An unreplicated coordinator MUST leave the shards stuck. */
runTxn('2pc: lone coordinator BLOCKS (ctl)', () => buildTxn(1, [3, 3]), 60000,
  (T, t) => {
    if (t === 2000) X.beginTxn(T);
    once(T, '_k', () => T.txn && allVotedYes(T), () => { T.groups[0].W.nodes[0].state = 'down'; });
  },
  i => ({
    ok: i.T._k && i.locked === i.shards && i.applied === 0,
    text: `locked=${i.locked}/${i.shards}  applied=${i.applied}  (expected BLOCKED)`,
  }));

runTxn('2pc: replicated coord recovers', () => buildTxn(3, [3, 3]), 90000,
  (T, t) => {
    if (t === 2000) X.beginTxn(T);
    once(T, '_k', () => T.txn && allVotedYes(T) && X.groupLeader(T.groups[0]),
      () => { X.groupLeader(T.groups[0]).state = 'down'; });
  },
  i => ({
    ok: i.T._k && i.applied === i.shards,
    text: `${tally(i)}  (expected RECOVERY)`,
  }));

// A shard that cannot elect can never accept anything. 2PC does the only safe
// thing left: presume abort, and release the shard that had already locked.
runTxn('2pc: presumed abort on timeout', () => buildTxn(3, [3, 3]), 90000,
  (T, t) => {
    if (t === 2000) {
      T.groups[2].W.nodes[0].state = 'down';
      T.groups[2].W.nodes[1].state = 'down';
      X.beginTxn(T);
    }
  },
  i => ({
    ok: X.coordFate(i.T).decision === 'abort' && X.shardFate(i.T, i.T.groups[1]).applied === 'abort',
    text: `decision=${X.coordFate(i.T).decision}  shard A applied=${X.shardFate(i.T, i.T.groups[1]).applied}`,
  }));

// Safety only. Under this much damage a transaction may never finish, and that
// is correct behaviour — the same stance the Raft chaos rows take.
runTxn('2pc: atomicity under chaos', () => buildTxn(3, [3, 3, 3], { dropRate: 0.2, jitter: 0.3 }), 120000,
  (T, t) => {
    if (t % 20000 === 2000) X.beginTxn(T);
    if (t % 3000 === 0) {
      const g = T.groups[Math.floor(Math.random() * T.groups.length)];
      const n = g.W.nodes[Math.floor(Math.random() * g.W.nodes.length)];
      if (n.state === 'down') { n.state = 'follower'; n.lastHeard = 6000; }
      else if (g.W.nodes.filter(x => x.state !== 'down').length > 2) n.state = 'down';
    }
    if (t % 5000 === 0) {
      const b = 1 + Math.floor(Math.random() * (T.groups.length - 1));
      const k = X.linkKey(0, b), next = X.LINK_CYCLE[T.links[k]];
      if (next === undefined) delete T.links[k]; else T.links[k] = next;
    }
    if (t % 7000 === 0) {
      const g = T.groups[1 + Math.floor(Math.random() * (T.groups.length - 1))];
      g.partition = g.partition ? 0 : 1;
    }
  },
  i => ({ ok: true, text: `phase=${i.phase}  safety only, liveness not asserted` }));

/* Every preset, driven headlessly. No outcome is asserted — coordDies is
   supposed to hang, and shardNoQuorum is supposed to abort — but all of them
   must hold every invariant while they get there. */
for (const key of Object.keys(X.TXN_SCENARIOS))
  runTxn('2pc scenario: ' + key, () => X.TXN_SCENARIOS[key].build(txnCfg()), 60000, null,
    i => ({ ok: true, text: tally(i) }));

console.log('\n' + (failures ? `${failures} CHECK(S) FAILED` : 'ALL INVARIANTS HELD'));
process.exit(failures ? 1 : 0);
