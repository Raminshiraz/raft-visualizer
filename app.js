const {
  useState,
  useRef,
  useEffect,
  useCallback
} = React;

/* ================================================================== *
 *  CONSTANTS  (all times are simulated milliseconds)                  *
 * ================================================================== */
const ELECTION_MIN = 3000; // randomized election timeout, low end
const ELECTION_MAX = 6000; // ...high end. Spread prevents split votes.
const HEARTBEAT = 1000; // leader -> followers, must be << ELECTION_MIN
const TRAVEL = 700; // base one-way network latency
const MAX_STEP = 100; // largest dt fed to step() in one go

const rand = (a, b) => a + Math.random() * (b - a);
const quorum = n => Math.floor(n / 2) + 1;
const GROUP_NAMES = ['A', 'B', 'C', 'D'];

/* --- per-link connectivity -----------------------------------------
   W.links maps "lo-hi" (node ids, lo<hi) to one of:
     'cut'  both directions dead
     'lo2hi' only lo→hi dead   (hi can still be heard by lo)
     'hi2lo' only hi→lo dead
   Missing key = healthy link. One-way states model the nasty real-world
   case where a node can send but never receives.                     */
const linkKey = (a, b) => a < b ? a + '-' + b : b + '-' + a;
const LINK_CYCLE = {
  undefined: 'cut',
  cut: 'lo2hi',
  lo2hi: 'hi2lo',
  hi2lo: undefined
};
function linkBlocks(W, from, to) {
  const st = W.links[linkKey(from, to)];
  if (!st) return false;
  if (st === 'cut') return true;
  const fromIsLo = from < to;
  return st === 'lo2hi' ? fromIsLo : !fromIsLo;
}

/* ================================================================== *
 *  NODE MODEL                                                         *
 *  Mirrors Raft paper Figure 2 state exactly.                         *
 * ================================================================== */
let _nid = 0,
  _mid = 0,
  _eid = 0;
function makeNode(term, id) {
  return {
    id: id === undefined ? _nid++ : id,
    /* --- PERSISTENT state (survives a crash, on stable storage) --- */
    currentTerm: term | 0,
    votedFor: null,
    log: [],
    // 1-indexed logically: log[i-1] is index i

    /* --- VOLATILE state on all servers --- */
    state: 'follower',
    // follower | candidate | leader | down
    commitIndex: 0,
    leaderId: null,
    /* --- VOLATILE state on leaders (reset on election) --- */
    nextIndex: {},
    // follower id -> next log index to send
    matchIndex: {},
    // follower id -> highest index known replicated

    /* --- simulation-only bookkeeping --- */
    votes: {},
    // id -> true, for the current election
    preVotes: {},
    // id -> true, for the pre-vote round
    preVoteTerm: 0,
    // term this pre-vote round is probing for
    phase: null,
    // 'prevote' while running a pre-vote round
    timeout: rand(ELECTION_MIN, ELECTION_MAX),
    timeoutInit: 0,
    hbTimer: 0,
    lastHeard: 0,
    // ms since a valid AppendEntries arrived
    partition: 0 // network group; equal ids can talk
  };
}

/* log helpers — everything below speaks 1-based log indices */
const lastIndex = n => n.log.length;
const termAt = (n, i) => i <= 0 ? 0 : n.log[i - 1] ? n.log[i - 1].term : 0;
const lastTerm = n => termAt(n, n.log.length);

/* Raft §5.4.1 — is `cand` at least as up-to-date as `voter`? */
function logIsUpToDate(candLastTerm, candLastIndex, voter) {
  const vt = lastTerm(voter);
  if (candLastTerm !== vt) return candLastTerm > vt;
  return candLastIndex >= lastIndex(voter);
}

/* distinct colour per term so divergent logs are visually obvious */
function termColor(t) {
  const h = (t * 137.5 + 200) % 360;
  return {
    bg: `hsl(${h} 62% 26%)`,
    br: `hsl(${h} 70% 52%)`,
    fg: `hsl(${h} 90% 84%)`
  };
}
function nodeColor(s) {
  return s === 'leader' ? 'var(--leader)' : s === 'candidate' ? 'var(--candidate)' : s === 'down' ? 'var(--down)' : 'var(--follower)';
}
/* ================================================================== *
 *  RAFT ENGINE                                                        *
 *  Plain functions over a mutable world object `W`:                   *
 *    { nodes, msgs, events, cfg, cmdSeq, armed, clock }               *
 * ================================================================== */
const byId = (W, id) => W.nodes.find(n => n.id === id);
const nodeCount = W => W.nodes.length;
function emit(W, nodeId, text, kind) {
  W.events.unshift({
    id: _eid++,
    t: (W.clock / 1000).toFixed(1),
    nodeId,
    text,
    kind: kind || ''
  });
  if (W.events.length > 160) W.events.length = 160;
}
function send(W, from, to, type, payload) {
  const jitter = W.cfg.jitter;
  W.msgs.push({
    id: _mid++,
    from,
    to,
    type,
    payload,
    progress: 0,
    travel: TRAVEL * rand(1 - jitter, 1 + jitter),
    willDrop: Math.random() < W.cfg.dropRate,
    dropAt: rand(0.3, 0.7),
    dead: false,
    fade: 0,
    delivered: false
  });
}
function resetTimeout(W, n) {
  n.timeout = rand(ELECTION_MIN, ELECTION_MAX);
  n.timeoutInit = n.timeout;
}

/* Rule for all servers: term seen > ours  =>  become follower in that term. */
function stepDown(W, n, term, why) {
  const was = n.state;
  n.currentTerm = term;
  n.votedFor = null; // new term, vote is fresh again
  n.state = 'follower';
  n.phase = null;
  n.votes = {};
  n.preVotes = {};
  n.preVoteTerm = 0;
  n.leaderId = null;
  resetTimeout(W, n);
  if (was !== 'follower') emit(W, n.id, `sees term ${term} ${why} → steps down from ${was.toUpperCase()} to FOLLOWER`, 'down');
}

/* ------------------------------------------------------------------ *
 *  Elections                                                          *
 * ------------------------------------------------------------------ */
function beginPreVote(W, n) {
  // PreVote (Ongaro thesis §9.6): probe for votes WITHOUT bumping our term,
  // so a partitioned node cannot inflate the cluster's term and disrupt a
  // healthy leader when it reconnects.
  const retry = n.phase === 'prevote';
  n.state = 'follower'; // a failed campaign falls back; it never lingers
  n.phase = 'prevote';
  n.votes = {};
  n.leaderId = null;
  n.preVoteTerm = n.currentTerm + 1; // the round these replies must belong to
  n.preVotes = {
    [n.id]: true
  };
  resetTimeout(W, n);
  emit(W, n.id, retry ? `pre-vote for term ${n.preVoteTerm} failed → probes again, term stays ${n.currentTerm}` : `timeout → PRE-VOTE probe for term ${n.preVoteTerm} (term not incremented yet)`, 'cand');
  for (const o of W.nodes) {
    if (o.id === n.id) continue;
    send(W, n.id, o.id, 'PreVote', {
      term: n.preVoteTerm,
      candidateId: n.id,
      lastLogIndex: lastIndex(n),
      lastLogTerm: lastTerm(n)
    });
  }
}
function startElection(W, n) {
  n.state = 'candidate';
  n.phase = null;
  n.currentTerm++; // §5.2: increment term...
  n.votedFor = n.id; // ...vote for self...
  n.votes = {
    [n.id]: true
  };
  n.preVotes = {};
  n.preVoteTerm = 0;
  n.leaderId = null;
  resetTimeout(W, n); // ...reset timer, then fan out RequestVote
  emit(W, n.id, `becomes CANDIDATE for term ${n.currentTerm}, votes for itself`, 'cand');
  for (const o of W.nodes) {
    if (o.id === n.id) continue;
    send(W, n.id, o.id, 'RequestVote', {
      term: n.currentTerm,
      candidateId: n.id,
      lastLogIndex: lastIndex(n),
      lastLogTerm: lastTerm(n)
    });
  }
}
function becomeLeader(W, n) {
  n.state = 'leader';
  n.leaderId = n.id;
  n.phase = null;
  n.hbTimer = 0;
  n.nextIndex = {};
  n.matchIndex = {};
  for (const o of W.nodes) {
    if (o.id === n.id) continue;
    n.nextIndex[o.id] = lastIndex(n) + 1; // optimistic guess, §5.3
    n.matchIndex[o.id] = 0; // known-replicated: nothing yet
  }
  const votes = Object.keys(n.votes).length;
  emit(W, n.id, `wins term ${n.currentTerm} with ${votes}/${nodeCount(W)} votes → LEADER`, 'leader');
  if (W.cfg.noop) {
    n.log.push({
      term: n.currentTerm,
      value: '⊘',
      noop: true
    });
    emit(W, n.id, `appends a no-op entry at index ${lastIndex(n)} so older terms can commit`, 'log');
  }
}

/* ------------------------------------------------------------------ *
 *  Replication                                                        *
 * ------------------------------------------------------------------ */
function sendAppendEntries(W, leader, followerId) {
  const f = byId(W, followerId);
  if (!f) return;
  if (leader.nextIndex[followerId] === undefined) {
    leader.nextIndex[followerId] = lastIndex(leader) + 1;
    leader.matchIndex[followerId] = 0;
  }
  const ni = Math.max(1, leader.nextIndex[followerId]);
  const pli = ni - 1; // prevLogIndex
  send(W, leader.id, followerId, 'AppendEntries', {
    term: leader.currentTerm,
    leaderId: leader.id,
    prevLogIndex: pli,
    prevLogTerm: termAt(leader, pli),
    entries: leader.log.slice(pli).map(e => ({
      ...e
    })),
    leaderCommit: leader.commitIndex
  });
}
function broadcastAppendEntries(W, leader) {
  for (const o of W.nodes) if (o.id !== leader.id) sendAppendEntries(W, leader, o.id);
}

/* §5.3/5.4: advance commitIndex to the highest N replicated on a majority,
   but ONLY if log[N] belongs to the leader's own term (Figure 8). */
function advanceCommit(W, leader) {
  const N = nodeCount(W);
  for (let k = lastIndex(leader); k > leader.commitIndex; k--) {
    let count = 1; // the leader stores it too
    for (const id in leader.matchIndex) if (leader.matchIndex[id] >= k) count++;
    if (count >= quorum(N)) {
      if (termAt(leader, k) === leader.currentTerm) {
        const from = leader.commitIndex;
        leader.commitIndex = k;
        emit(W, leader.id, `index ${from + 1}..${k} replicated on ${count}/${N} → COMMITTED`, 'commit');
        return;
      }
      // replicated on a majority but from an older term: NOT safe to commit.
      // It commits later, implicitly, once a current-term entry commits.
    }
  }
}

/* ------------------------------------------------------------------ *
 *  RPC handlers                                                       *
 * ------------------------------------------------------------------ */
function deliver(W, m) {
  const to = byId(W, m.to);
  if (!to || to.state === 'down') return;
  const p = m.payload;

  /* ---------------- PreVote ---------------- */
  if (m.type === 'PreVote') {
    // Grant only if we have NOT heard from a live leader within one minimum
    // election timeout. That is what stops a node the leader cannot reach
    // from disrupting a cluster that is otherwise perfectly healthy.
    // A leader hears a leader every tick — itself — so it never grants.
    const heardRecently = to.state === 'leader' || to.lastHeard < ELECTION_MIN;
    const granted = !heardRecently && p.term >= to.currentTerm + 1 && logIsUpToDate(p.lastLogTerm, p.lastLogIndex, to);
    send(W, to.id, m.from, 'PreVoteReply', {
      term: to.currentTerm,
      granted,
      forTerm: p.term
    });
    if (!granted) {
      const why = heardRecently ? to.state === 'leader' ? 'it is the LEADER and still heartbeating' : to.leaderId !== null ? `it still hears N${to.leaderId}` : 'it heard a leader too recently' : p.term < to.currentTerm + 1 ? `term ${p.term} is not ahead of ours ${to.currentTerm}` : `its log ${p.lastLogTerm}/${p.lastLogIndex} is behind ours ${lastTerm(to)}/${lastIndex(to)}`;
      emit(W, to.id, `denies pre-vote to N${p.candidateId}: ${why}`, 'deny');
    }
    return;
  }
  if (m.type === 'PreVoteReply') {
    // A higher term demotes us whatever we are doing — check that first.
    if (p.term > to.currentTerm) {
      stepDown(W, to, p.term, 'in a pre-vote reply');
      return;
    }
    if (to.phase !== 'prevote') return;
    if (p.forTerm !== to.preVoteTerm) return; // reply from an older probe
    if (!p.granted) return;
    to.preVotes[m.from] = true;
    const pre = Object.keys(to.preVotes).length;
    if (pre >= quorum(nodeCount(W))) {
      emit(W, to.id, `pre-vote carried ${pre}/${nodeCount(W)} → now safe to increment term`, 'cand');
      startElection(W, to);
    }
    return;
  }

  /* ---------------- RequestVote ---------------- */
  if (m.type === 'RequestVote') {
    if (p.term < to.currentTerm) {
      // stale candidate
      send(W, to.id, m.from, 'RequestVoteReply', {
        term: to.currentTerm,
        granted: false
      });
      emit(W, to.id, `denies N${p.candidateId}: its term ${p.term} < ours ${to.currentTerm}`, 'deny');
      return;
    }
    if (p.term > to.currentTerm) stepDown(W, to, p.term, `from N${p.candidateId}`);
    const free = to.votedFor === null || to.votedFor === p.candidateId;
    const fresh = logIsUpToDate(p.lastLogTerm, p.lastLogIndex, to);
    const granted = free && fresh;
    if (granted) {
      to.votedFor = p.candidateId;
      resetTimeout(W, to);
      // We backed someone else, so abandon any pre-vote probe of our own —
      // otherwise the UI shows a "PRE-VOTE" node that has already voted.
      to.phase = null;
      to.preVotes = {};
      to.preVoteTerm = 0;
    }
    const why = granted ? '' : !free ? ` (already voted for N${to.votedFor} this term)` : ` (its log ${p.lastLogTerm}/${p.lastLogIndex} is behind ours ${lastTerm(to)}/${lastIndex(to)})`;
    emit(W, to.id, `${granted ? 'GRANTS' : 'denies'} vote to N${p.candidateId} in term ${to.currentTerm}${why}`, granted ? 'grant' : 'deny');
    send(W, to.id, m.from, 'RequestVoteReply', {
      term: to.currentTerm,
      granted
    });
    return;
  }
  if (m.type === 'RequestVoteReply') {
    // ANY reply carrying a higher term demotes us, whatever we are.
    if (p.term > to.currentTerm) {
      stepDown(W, to, p.term, 'in a vote reply');
      return;
    }
    if (to.state !== 'candidate' || p.term !== to.currentTerm) return;
    if (!p.granted) return;
    to.votes[m.from] = true;
    const got = Object.keys(to.votes).length;
    if (got >= quorum(nodeCount(W))) becomeLeader(W, to);
    return;
  }

  /* ---------------- AppendEntries ---------------- */
  if (m.type === 'AppendEntries') {
    // 1. Reject anything from a stale leader.
    if (p.term < to.currentTerm) {
      send(W, to.id, m.from, 'AppendEntriesReply', {
        term: to.currentTerm,
        success: false,
        conflictIndex: 1,
        reqTerm: p.term
      });
      emit(W, to.id, `rejects N${p.leaderId}: stale term ${p.term} < ${to.currentTerm}`, 'deny');
      return;
    }
    if (p.term > to.currentTerm) {
      to.currentTerm = p.term;
      to.votedFor = null;
    }
    if (to.state !== 'follower' && to.state !== 'down') {
      emit(W, to.id, `recognises N${p.leaderId} as leader of term ${p.term} → FOLLOWER`, 'down');
    }
    to.state = 'follower';
    to.phase = null;
    to.leaderId = p.leaderId;
    to.votes = {};
    to.preVotes = {};
    to.preVoteTerm = 0;
    resetTimeout(W, to);
    to.lastHeard = 0;

    // 2. Log consistency check: we must already hold prevLogIndex@prevLogTerm.
    if (p.prevLogIndex > lastIndex(to)) {
      // our log is too short — tell the leader where we actually end
      send(W, to.id, m.from, 'AppendEntriesReply', {
        term: to.currentTerm,
        success: false,
        conflictIndex: lastIndex(to) + 1,
        reqTerm: p.term
      });
      emit(W, to.id, `gap: leader assumed index ${p.prevLogIndex}, our log ends at ${lastIndex(to)}`, 'deny');
      return;
    }
    if (p.prevLogIndex > 0 && termAt(to, p.prevLogIndex) !== p.prevLogTerm) {
      // term mismatch — skip our whole conflicting term at once (fast backtrack)
      const badTerm = termAt(to, p.prevLogIndex);
      let i = p.prevLogIndex;
      while (i > 1 && termAt(to, i - 1) === badTerm) i--;
      send(W, to.id, m.from, 'AppendEntriesReply', {
        term: to.currentTerm,
        success: false,
        conflictIndex: i,
        reqTerm: p.term
      });
      emit(W, to.id, `mismatch at ${p.prevLogIndex}: ours term ${badTerm}, leader says ${p.prevLogTerm}`, 'deny');
      return;
    }

    // 3+4. Truncate conflicts, then append what is new.
    let appended = 0,
      truncated = 0;
    for (let k = 0; k < p.entries.length; k++) {
      const idx = p.prevLogIndex + k + 1;
      if (idx <= lastIndex(to)) {
        if (termAt(to, idx) !== p.entries[k].term) {
          truncated = lastIndex(to) - (idx - 1);
          to.log.length = idx - 1; // delete this entry and everything after
          to.log.push({
            ...p.entries[k]
          });
          appended++;
        }
      } else {
        to.log.push({
          ...p.entries[k]
        });
        appended++;
      }
    }
    if (truncated) emit(W, to.id, `TRUNCATES ${truncated} conflicting entr${truncated > 1 ? 'ies' : 'y'}, adopts leader's`, 'trunc');else if (appended) emit(W, to.id, `appends ${appended} entr${appended > 1 ? 'ies' : 'y'} from N${p.leaderId}`, 'log');

    // 5. Follower learns what is committed.
    if (p.leaderCommit > to.commitIndex) {
      to.commitIndex = Math.min(p.leaderCommit, p.prevLogIndex + p.entries.length);
    }
    send(W, to.id, m.from, 'AppendEntriesReply', {
      term: to.currentTerm,
      success: true,
      matchIndex: p.prevLogIndex + p.entries.length,
      reqTerm: p.term
    });
    return;
  }
  if (m.type === 'AppendEntriesReply') {
    if (p.term > to.currentTerm) {
      stepDown(W, to, p.term, 'in an append reply');
      return;
    }
    if (to.state !== 'leader' || p.reqTerm !== to.currentTerm) return; // ignore stale replies
    if (p.success) {
      to.matchIndex[m.from] = Math.max(to.matchIndex[m.from] || 0, p.matchIndex);
      to.nextIndex[m.from] = to.matchIndex[m.from] + 1;
      advanceCommit(W, to);
    } else {
      const before = to.nextIndex[m.from];
      to.nextIndex[m.from] = Math.max(1, p.conflictIndex);
      if (to.nextIndex[m.from] < before) {
        emit(W, to.id, `backs off nextIndex[N${m.from}] ${before} → ${to.nextIndex[m.from]}, retries`, 'retry');
      }
      sendAppendEntries(W, to, m.from); // retry immediately with an older prefix
    }
    return;
  }
}

/* ------------------------------------------------------------------ *
 *  Message fate: partitions, drops, dead targets                      *
 * ------------------------------------------------------------------ */
function blockReason(W, m) {
  const a = byId(W, m.from),
    b = byId(W, m.to);
  if (!b) return 'node gone';
  if (b.state === 'down') return 'target crashed';
  if (a && a.state === 'down') return 'sender crashed';
  if (a && a.partition !== b.partition) return 'partitioned';
  if (linkBlocks(W, m.from, m.to)) return 'link cut';
  if (m.willDrop) return 'packet lost';
  return null;
}
function dieAt(reason) {
  return reason === 'partitioned' ? 0.5 : reason === 'link cut' ? 0.5 : reason === 'target crashed' ? 0.9 : reason === 'sender crashed' ? 0.25 : 0.55;
}

/* ------------------------------------------------------------------ *
 *  One simulation tick                                                *
 * ------------------------------------------------------------------ */
function tick(W, dt) {
  W.clock += dt;
  for (const n of W.nodes) {
    if (n.state === 'down') continue;
    n.lastHeard += dt;
    if (n.state === 'leader') {
      n.lastHeard = 0; // a leader hears a leader continuously: itself
      n.hbTimer -= dt;
      if (n.hbTimer <= 0) {
        broadcastAppendEntries(W, n);
        n.hbTimer = HEARTBEAT;
      }
    } else {
      n.timeout -= dt;
      if (n.timeout <= 0) {
        // A pre-vote that failed retries as another PRE-VOTE. It must never
        // fall through into a real election, or the whole mechanism is moot:
        // the second timeout would bump the term and disrupt a live leader.
        if (W.cfg.prevote) beginPreVote(W, n);else startElection(W, n);
      }
    }
  }
  for (const m of W.msgs) {
    if (m.dead) {
      m.fade += dt;
      continue;
    }
    const reason = blockReason(W, m);
    if (reason && m.progress >= dieAt(reason)) {
      m.dead = true;
      m.fade = 0;
      m.reason = reason;
      continue;
    }
    m.progress += dt / m.travel;
    if (m.progress >= 1) {
      if (!reason) deliver(W, m);
      m.delivered = true;
    }
  }
  W.msgs = W.msgs.filter(m => !m.delivered && !(m.dead && m.fade > 450));

  // deferred scenario action, e.g. "crash the leader once one exists"
  if (W.armed) {
    const done = W.armed(W);
    if (done) W.armed = null;
  }
}
/* ================================================================== *
 *  WORLD + SCENARIOS                                                  *
 * ================================================================== */
function makeWorld(count, cfg) {
  _nid = 0;
  _mid = 0;
  _eid = 0;
  const nodes = [];
  for (let i = 0; i < count; i++) nodes.push(makeNode(0));
  nodes.forEach(n => {
    n.timeoutInit = n.timeout;
  });
  return {
    nodes,
    msgs: [],
    events: [],
    cfg,
    links: {},
    cmdSeq: 0,
    armed: null,
    clock: 0,
    note: null
  };
}
const SCENARIOS = {
  fresh: {
    label: 'Cold start',
    note: 'Five empty nodes. The first to burn through its randomized election timeout campaigns and normally wins outright.',
    build(cfg) {
      return makeWorld(5, cfg);
    }
  },
  killLeader: {
    label: 'Kill the leader',
    note: 'Waits for a leader, then crashes it. Watch the survivors time out, campaign in a higher term, and elect a replacement.',
    build(cfg) {
      const W = makeWorld(5, cfg);
      W.armed = W => {
        const L = W.nodes.find(n => n.state === 'leader');
        if (!L) return false;
        L.state = 'down';
        emit(W, L.id, `CRASHED by scenario (was LEADER of term ${L.currentTerm})`, 'crash');
        return true;
      };
      return W;
    }
  },
  splitVote: {
    label: 'Split vote',
    note: 'Four nodes — an even cluster — with two timers firing together and heavy latency jitter. Votes often split 2-2, nobody reaches 3, and the term is wasted. Randomized timeouts then break the tie.',
    build(cfg) {
      const W = makeWorld(4, {
        ...cfg,
        jitter: 0.6
      });
      W.nodes[0].timeout = 60;
      W.nodes[1].timeout = 60;
      W.nodes[2].timeout = 5200;
      W.nodes[3].timeout = 5600;
      W.nodes.forEach(n => {
        n.timeoutInit = Math.max(n.timeout, ELECTION_MIN);
      });
      return W;
    }
  },
  partition: {
    label: 'Partition 3–2',
    note: 'The link splits into a 3-node majority and a 2-node minority. Only the majority can elect. The minority spins its term up forever and commits nothing. Press Heal to reconnect.',
    build(cfg) {
      const W = makeWorld(5, cfg);
      W.nodes[3].partition = 1;
      W.nodes[4].partition = 1;
      return W;
    }
  },
  repair: {
    label: 'Stale follower repair',
    note: "N1 holds four entries from a dead term-2 leader that never committed. N0 has the authoritative log. Watch nextIndex walk backwards until the logs match, then N1's bad tail is truncated and overwritten.",
    build(cfg) {
      const W = makeWorld(5, {
        ...cfg,
        noop: true
      });
      const good = [{
        term: 1,
        value: 'A'
      }, {
        term: 1,
        value: 'B'
      }, {
        term: 3,
        value: 'C'
      }, {
        term: 3,
        value: 'D'
      }];
      for (const n of W.nodes) {
        n.currentTerm = 3;
        n.log = good.map(e => ({
          ...e
        }));
        n.commitIndex = 2;
        n.timeout = 5500;
      }
      // N1 diverges from index 3 onward with entries from a stale term 2
      W.nodes[1].log = [{
        term: 1,
        value: 'A'
      }, {
        term: 1,
        value: 'B'
      }, {
        term: 2,
        value: 'X'
      }, {
        term: 2,
        value: 'Y'
      }, {
        term: 2,
        value: 'Z'
      }, {
        term: 2,
        value: 'W'
      }];
      W.nodes[1].commitIndex = 2;
      W.nodes[0].timeout = 80; // N0 campaigns first and must win (term 3 log)
      W.nodes.forEach(n => {
        n.timeoutInit = Math.max(n.timeout, ELECTION_MIN);
      });
      return W;
    }
  },
  asymmetric: {
    label: 'One-way link (disruptive node)',
    note: 'N4 can still SEND to everyone, but receives nothing — every inbound link to it is cut one-way. It never hears a heartbeat, so it times out forever, campaigns in ever-higher terms, and its RequestVotes keep knocking the healthy leader down. Turn on PreVote to watch the disruption stop.',
    build(cfg) {
      const W = makeWorld(5, cfg);
      // block every j -> N4 direction, leaving N4 -> j intact
      for (let j = 0; j < 4; j++) W.links[linkKey(j, 4)] = 'lo2hi';
      return W;
    }
  },
  figure8: {
    label: 'Figure 8 — old-term entry',
    note: "Every node already stores entry 2 from term 2, yet nothing is committed. The new leader will replicate it to all five and still refuse to commit it, because Raft only commits entries from its OWN term. Press \"Client command\" and both commit at once.",
    build(cfg) {
      const W = makeWorld(5, {
        ...cfg,
        noop: false
      });
      for (const n of W.nodes) {
        n.currentTerm = 3;
        n.log = [{
          term: 1,
          value: 'A'
        }, {
          term: 2,
          value: 'B'
        }];
        n.commitIndex = 0;
        n.timeout = 5500;
      }
      W.nodes[0].timeout = 80;
      W.nodes.forEach(n => {
        n.timeoutInit = Math.max(n.timeout, ELECTION_MIN);
      });
      return W;
    }
  }
};

/* ================================================================== *
 *  LAYOUT — nodes arc-grouped by network partition                    *
 * ================================================================== */
function computeLayout(nodes, W, H) {
  const cx = W / 2,
    cy = H / 2;
  const R = Math.min(215, 148 + nodes.length * 6);
  const pos = {};
  const groups = {};
  for (const n of nodes) (groups[n.partition] = groups[n.partition] || []).push(n);
  const keys = Object.keys(groups).map(Number).sort((a, b) => a - b);
  if (keys.length <= 1) {
    nodes.forEach((n, i) => {
      const a = -Math.PI / 2 + i * 2 * Math.PI / nodes.length;
      pos[n.id] = {
        x: cx + R * Math.cos(a),
        y: cy + R * Math.sin(a)
      };
    });
    return {
      pos,
      cx,
      cy,
      R,
      groups: [],
      split: false
    };
  }

  // split the ring into one arc per group, separated by visible gaps
  const GAP = 0.62;
  const usable = 2 * Math.PI - keys.length * GAP;
  let angle = -Math.PI / 2 - Math.PI / keys.length;
  const shapes = [];
  for (const k of keys) {
    const members = groups[k];
    const span = usable * (members.length / nodes.length);
    members.forEach((n, i) => {
      const a = members.length === 1 ? angle + span / 2 : angle + span * (i / (members.length - 1));
      pos[n.id] = {
        x: cx + R * Math.cos(a),
        y: cy + R * Math.sin(a)
      };
    });
    const xs = members.map(n => pos[n.id].x),
      ys = members.map(n => pos[n.id].y);
    const gx = xs.reduce((s, v) => s + v, 0) / xs.length;
    const gy = ys.reduce((s, v) => s + v, 0) / ys.length;
    let rad = 0;
    members.forEach(n => {
      rad = Math.max(rad, Math.hypot(pos[n.id].x - gx, pos[n.id].y - gy));
    });
    shapes.push({
      key: k,
      gx,
      gy,
      r: rad + 58,
      size: members.length
    });
    angle += span + GAP;
  }
  return {
    pos,
    cx,
    cy,
    R,
    groups: shapes,
    split: true
  };
}

/* ================================================================== *
 *  EXPLAIN PANEL — narrates whatever the cluster is doing right now   *
 * ================================================================== */
function explain(W) {
  const N = W.nodes.length,
    q = quorum(N);
  const live = W.nodes.filter(n => n.state !== 'down');
  const leader = W.nodes.find(n => n.state === 'leader');
  const cands = W.nodes.filter(n => n.state === 'candidate');
  const pre = W.nodes.filter(n => n.phase === 'prevote');
  const parts = new Set(W.nodes.map(n => n.partition));
  if (live.length < q) {
    return {
      ttl: 'No quorum — cluster is stuck',
      tone: 'var(--down)',
      bd: `Only <em>${live.length}</em> of <em>${N}</em> nodes are alive, but a majority needs <em>${q}</em>. No candidate can ever collect enough votes, so no leader can exist and nothing new can commit.`,
      why: 'Raft trades availability for safety: it would rather stop than risk two leaders. Revive a node to get back above the line.'
    };
  }
  if (parts.size > 1) {
    const sizes = {};
    for (const n of W.nodes) sizes[n.partition] = (sizes[n.partition] || 0) + 1;
    const big = Math.max(...Object.values(sizes));
    return {
      ttl: 'Network is partitioned',
      tone: 'var(--candidate)',
      bd: `The cluster is split into <em>${parts.size}</em> groups of ${Object.values(sizes).join(' and ')} nodes. A quorum is <em>${q}</em>, so ${big >= q ? 'the larger side can still elect and commit' : 'no side is large enough to elect anything'}. Messages crossing the boundary die mid-flight.`,
      why: 'Only one side can ever hold a majority, which is exactly why Raft can never produce two committing leaders. The minority may keep raising its term, but it can never win.'
    };
  }
  const cuts = Object.keys(W.links).length;
  if (cuts && !leader && !cands.length) {
    return {
      ttl: `${cuts} link${cuts > 1 ? 's' : ''} cut — connectivity is uneven`,
      tone: 'var(--candidate)',
      bd: `The cluster is not partitioned into clean groups; individual wires are down. A node can be reachable by some peers and invisible to others, so different nodes disagree about who is even alive.`,
      why: 'Raft only needs a majority that can all hear the leader. Uneven links are worse than a clean split: a node that can send but not receive keeps timing out and disrupting everyone else.'
    };
  }
  if (pre.length) {
    return {
      ttl: `N${pre[0].id} is running a pre-vote`,
      tone: 'var(--pv)',
      bd: `N${pre[0].id} timed out but has <em>not</em> incremented its term. It first asks whether the others would vote for it. Only if <em>${q}</em> agree does it start a real election.`,
      why: 'Pre-vote stops a node that was isolated from returning with an inflated term and knocking a perfectly healthy leader out of office.'
    };
  }
  if (cands.length > 1) {
    return {
      ttl: `Split vote — ${cands.length} candidates at once`,
      tone: 'var(--candidate)',
      bd: `${cands.map(c => `N${c.id} (${Object.keys(c.votes).length} votes)`).join(' and ')} are both campaigning. Each node gets exactly <em>one</em> vote per term, so if neither reaches <em>${q}</em> the term ends leaderless and everyone tries again.`,
      why: 'This is why election timeouts are randomized over a wide range — the next round almost always has a clear first mover.'
    };
  }
  if (cands.length === 1) {
    const c = cands[0],
      got = Object.keys(c.votes).length;
    return {
      ttl: `N${c.id} is campaigning for term ${c.currentTerm}`,
      tone: 'var(--candidate)',
      bd: `It voted for itself and asked everyone else. It holds <em>${got}</em> of the <em>${q}</em> votes it needs. Voters only say yes if they haven't voted this term <em>and</em> the candidate's log is at least as up-to-date as their own.`,
      why: 'That log check is what guarantees a new leader already holds every committed entry — so committed data can never be lost in an election.'
    };
  }
  if (leader) {
    const behind = W.nodes.filter(n => n.id !== leader.id && n.state !== 'down' && (leader.matchIndex[n.id] || 0) < lastIndex(leader)).length;
    const pending = lastIndex(leader) - leader.commitIndex;
    return {
      ttl: `N${leader.id} leads term ${leader.currentTerm}`,
      tone: 'var(--leader)',
      bd: `It heartbeats every ${HEARTBEAT}ms to stop followers timing out. Its log holds <em>${lastIndex(leader)}</em> entries, <em>${leader.commitIndex}</em> committed${pending ? `, <em>${pending}</em> still waiting on a majority` : ''}.${behind ? ` <em>${behind}</em> follower(s) are still catching up.` : ' All followers are in sync.'}`,
      why: 'An entry commits the moment it is stored on a majority — the leader then tells everyone via leaderCommit on the next heartbeat.'
    };
  }
  return {
    ttl: 'Idle — waiting for a timeout',
    tone: 'var(--follower)',
    bd: `No leader and no candidate. Every node is counting down its own randomized timeout (${ELECTION_MIN}–${ELECTION_MAX}ms — the shrinking ring around each node). The first to hit zero campaigns.`,
    why: 'Different deadlines mean one node almost always moves first, which keeps elections short.'
  };
}
/* ================================================================== *
 *  APP                                                                *
 * ================================================================== */
const DEFAULT_CFG = {
  dropRate: 0,
  jitter: 0.12,
  prevote: false,
  noop: false
};
function App() {
  const world = useRef(null);
  if (world.current === null) world.current = SCENARIOS.fresh.build({
    ...DEFAULT_CFG
  });
  const ui = useRef({
    running: true,
    speed: 1
  });
  const [running, setRunning] = useState(true);
  const [speed, setSpeed] = useState(1);
  const [mode, setMode] = useState('crash'); // crash | partition
  const [cfg, setCfgState] = useState({
    ...DEFAULT_CFG
  });
  const [scenario, setScenario] = useState('fresh');
  const [tip, setTip] = useState(null);
  const [, force] = useState(0);

  /* main loop */
  useEffect(() => {
    let raf,
      last = performance.now();
    const loop = now => {
      const dt = now - last;
      last = now;
      if (ui.current.running) {
        let sd = Math.min(dt, 200) * ui.current.speed;
        while (sd > 0) {
          const s = Math.min(sd, MAX_STEP);
          tick(world.current, s);
          sd -= s;
        }
      }
      force(f => f + 1);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, []);
  const W = world.current;

  /* ---- controls ---- */
  const toggleRun = () => {
    ui.current.running = !ui.current.running;
    setRunning(ui.current.running);
  };
  const pause = () => {
    ui.current.running = false;
    setRunning(false);
  };
  const stepOnce = () => {
    pause();
    tick(W, 120);
    force(f => f + 1);
  };
  const stepEvent = () => {
    pause();
    const before = W.events.length;
    let spent = 0;
    while (spent < 8000 && W.events.length === before) {
      tick(W, 40);
      spent += 40;
    }
    force(f => f + 1);
  };
  const setCfg = patch => {
    Object.assign(W.cfg, patch);
    setCfgState({
      ...W.cfg
    });
  };
  const loadScenario = key => {
    world.current = SCENARIOS[key].build({
      ...W.cfg
    });
    world.current.note = SCENARIOS[key].note;
    setScenario(key);
    setCfgState({
      ...world.current.cfg
    });
    ui.current.running = true;
    setRunning(true);
    emit(world.current, -1, `scenario loaded: ${SCENARIOS[key].label}`, 'sys');
  };
  const clickNode = id => {
    const n = byId(W, id);
    if (!n) return;
    if (mode === 'partition') {
      const groups = Math.max(2, new Set(W.nodes.map(x => x.partition)).size);
      n.partition = (n.partition + 1) % Math.min(groups + 1, 3);
      emit(W, n.id, `moved to network group ${GROUP_NAMES[n.partition]}`, 'sys');
    } else {
      if (n.state === 'down') {
        // currentTerm / votedFor / log are persistent — they survive the crash
        n.state = 'follower';
        n.votes = {};
        n.preVotes = {};
        n.preVoteTerm = 0;
        n.phase = null;
        n.leaderId = null;
        n.lastHeard = ELECTION_MAX;
        resetTimeout(W, n);
        emit(W, n.id, `restarts as FOLLOWER — keeps term ${n.currentTerm}, votedFor ${n.votedFor === null ? '∅' : 'N' + n.votedFor}, ${lastIndex(n)} log entries`, 'sys');
      } else {
        const was = n.state;
        n.state = 'down';
        n.votes = {};
        n.preVotes = {};
        n.preVoteTerm = 0;
        n.phase = null;
        n.leaderId = null;
        emit(W, n.id, `CRASHED (was ${was.toUpperCase()})`, 'crash');
      }
    }
  };
  const clickLink = (a, b) => {
    const k = linkKey(a, b);
    const next = LINK_CYCLE[W.links[k]];
    if (next === undefined) delete W.links[k];else W.links[k] = next;
    const lo = Math.min(a, b),
      hi = Math.max(a, b);
    emit(W, -1, next === undefined ? `link N${lo}–N${hi} restored` : next === 'cut' ? `link N${lo}–N${hi} CUT in both directions` : next === 'lo2hi' ? `link N${lo}→N${hi} cut one-way (N${hi}→N${lo} still works)` : `link N${hi}→N${lo} cut one-way (N${lo}→N${hi} still works)`, 'sys');
  };
  const healAll = () => {
    W.nodes.forEach(n => n.partition = 0);
    W.links = {};
    emit(W, -1, 'network healed — all partitions merged and every link restored', 'sys');
  };
  const addNode = () => {
    if (W.nodes.length >= 9) return;
    const t = Math.max(0, ...W.nodes.map(n => n.currentTerm));
    const n = makeNode(t);
    n.timeoutInit = n.timeout;
    n.lastHeard = 0;
    W.nodes.push(n);
    emit(W, n.id, `joins the cluster — quorum is now ${quorum(W.nodes.length)}/${W.nodes.length}`, 'sys');
  };
  const removeNode = () => {
    if (W.nodes.length <= 3) return;
    const n = W.nodes.pop();
    W.msgs = W.msgs.filter(m => m.from !== n.id && m.to !== n.id);
    W.nodes.forEach(o => {
      delete o.nextIndex[n.id];
      delete o.matchIndex[n.id];
      delete o.votes[n.id];
    });
    emit(W, n.id, `removed — quorum is now ${quorum(W.nodes.length)}/${W.nodes.length}`, 'sys');
  };
  const clientCmd = () => {
    const L = W.nodes.find(n => n.state === 'leader');
    if (!L) {
      emit(W, -1, 'client command REJECTED — there is no leader to accept it', 'deny');
      return;
    }
    const v = String.fromCharCode(65 + W.cmdSeq++ % 26);
    L.log.push({
      term: L.currentTerm,
      value: v
    });
    emit(W, L.id, `accepts client command "${v}" at index ${lastIndex(L)} (term ${L.currentTerm}) — replicating`, 'log');
    broadcastAppendEntries(W, L);
    L.hbTimer = HEARTBEAT;
  };

  /* ---- derived ---- */
  const maxTerm = Math.max(0, ...W.nodes.map(n => n.currentTerm));
  const leader = W.nodes.find(n => n.state === 'leader');
  const live = W.nodes.filter(n => n.state !== 'down').length;
  const ex = explain(W);
  const q = quorum(W.nodes.length);
  return /*#__PURE__*/React.createElement("div", {
    className: "app"
  }, /*#__PURE__*/React.createElement("div", {
    className: "head"
  }, /*#__PURE__*/React.createElement("h2", null, "\u2699\uFE0F Raft Consensus Visualizer"), /*#__PURE__*/React.createElement("span", {
    className: "sub"
  }, "leader election \xB7 log replication \xB7 partitions"), /*#__PURE__*/React.createElement("div", {
    className: "stat"
  }, /*#__PURE__*/React.createElement("span", {
    className: "chip"
  }, "term ", /*#__PURE__*/React.createElement("b", null, maxTerm)), /*#__PURE__*/React.createElement("span", {
    className: "chip"
  }, "quorum ", /*#__PURE__*/React.createElement("b", null, q), "/", /*#__PURE__*/React.createElement("b", null, W.nodes.length)), /*#__PURE__*/React.createElement("span", {
    className: "chip"
  }, "alive ", /*#__PURE__*/React.createElement("b", {
    style: {
      color: live >= q ? 'var(--leader)' : 'var(--down)'
    }
  }, live)), /*#__PURE__*/React.createElement("span", {
    className: "chip"
  }, "leader ", /*#__PURE__*/React.createElement("b", {
    style: {
      color: leader ? 'var(--leader)' : 'var(--down)'
    }
  }, leader ? 'N' + leader.id : 'none')))), /*#__PURE__*/React.createElement("div", {
    className: "bar"
  }, /*#__PURE__*/React.createElement("span", {
    className: "barlab"
  }, "Run"), /*#__PURE__*/React.createElement("button", {
    className: "btn primary",
    onClick: toggleRun
  }, running ? '⏸ Pause' : '▶ Play'), /*#__PURE__*/React.createElement("button", {
    className: "btn",
    onClick: stepOnce
  }, "\u23E9 Step 120ms"), /*#__PURE__*/React.createElement("button", {
    className: "btn",
    onClick: stepEvent
  }, "\u23ED Next event"), /*#__PURE__*/React.createElement("div", {
    className: "sl"
  }, /*#__PURE__*/React.createElement("span", null, "speed"), /*#__PURE__*/React.createElement("input", {
    type: "range",
    min: "0.25",
    max: "4",
    step: "0.25",
    value: speed,
    onChange: e => {
      const v = parseFloat(e.target.value);
      ui.current.speed = v;
      setSpeed(v);
    }
  }), /*#__PURE__*/React.createElement("b", null, speed, "\xD7")), /*#__PURE__*/React.createElement("div", {
    className: "sep"
  }), /*#__PURE__*/React.createElement("button", {
    className: "btn warn",
    onClick: () => loadScenario(scenario)
  }, "\u21BA Restart scenario")), /*#__PURE__*/React.createElement("div", {
    className: "bar"
  }, /*#__PURE__*/React.createElement("span", {
    className: "barlab"
  }, "Cluster"), /*#__PURE__*/React.createElement("button", {
    className: 'btn ' + (mode === 'crash' ? 'on' : ''),
    onClick: () => setMode('crash')
  }, "\uD83D\uDCA5 Crash mode"), /*#__PURE__*/React.createElement("button", {
    className: 'btn ' + (mode === 'partition' ? 'on' : ''),
    onClick: () => setMode('partition')
  }, "\u2702\uFE0F Partition mode"), /*#__PURE__*/React.createElement("button", {
    className: "btn",
    onClick: healAll
  }, "\uD83D\uDD17 Heal network"), /*#__PURE__*/React.createElement("div", {
    className: "sep"
  }), /*#__PURE__*/React.createElement("button", {
    className: "btn",
    onClick: addNode,
    disabled: W.nodes.length >= 9
  }, "\uFF0B node"), /*#__PURE__*/React.createElement("button", {
    className: "btn",
    onClick: removeNode,
    disabled: W.nodes.length <= 3
  }, "\uFF0D node"), /*#__PURE__*/React.createElement("div", {
    className: "sep"
  }), /*#__PURE__*/React.createElement("button", {
    className: "btn primary",
    onClick: clientCmd
  }, "\u2B06 Client command")), /*#__PURE__*/React.createElement("div", {
    className: "bar"
  }, /*#__PURE__*/React.createElement("span", {
    className: "barlab"
  }, "Network"), /*#__PURE__*/React.createElement("div", {
    className: "sl"
  }, /*#__PURE__*/React.createElement("span", null, "packet loss"), /*#__PURE__*/React.createElement("input", {
    type: "range",
    min: "0",
    max: "0.6",
    step: "0.05",
    value: cfg.dropRate,
    onChange: e => setCfg({
      dropRate: parseFloat(e.target.value)
    })
  }), /*#__PURE__*/React.createElement("b", null, Math.round(cfg.dropRate * 100), "%")), /*#__PURE__*/React.createElement("div", {
    className: "sl"
  }, /*#__PURE__*/React.createElement("span", null, "latency jitter"), /*#__PURE__*/React.createElement("input", {
    type: "range",
    min: "0",
    max: "0.8",
    step: "0.05",
    value: cfg.jitter,
    onChange: e => setCfg({
      jitter: parseFloat(e.target.value)
    })
  }), /*#__PURE__*/React.createElement("b", null, "\xB1", Math.round(cfg.jitter * 100), "%")), /*#__PURE__*/React.createElement("div", {
    className: "sep"
  }), /*#__PURE__*/React.createElement("button", {
    className: 'btn ' + (cfg.prevote ? 'on' : ''),
    onClick: () => setCfg({
      prevote: !cfg.prevote
    }),
    title: "Probe for votes before incrementing the term"
  }, cfg.prevote ? '✓ ' : '', "PreVote"), /*#__PURE__*/React.createElement("button", {
    className: 'btn ' + (cfg.noop ? 'on' : ''),
    onClick: () => setCfg({
      noop: !cfg.noop
    }),
    title: "New leader appends a no-op so older-term entries can commit"
  }, cfg.noop ? '✓ ' : '', "Leader no-op")), /*#__PURE__*/React.createElement("div", {
    className: "bar"
  }, /*#__PURE__*/React.createElement("span", {
    className: "barlab"
  }, "Scenario"), Object.keys(SCENARIOS).map(k => /*#__PURE__*/React.createElement("button", {
    key: k,
    className: 'btn ' + (scenario === k ? 'on' : ''),
    onClick: () => loadScenario(k)
  }, SCENARIOS[k].label))), /*#__PURE__*/React.createElement("div", {
    className: "grid"
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("div", {
    className: "card"
  }, /*#__PURE__*/React.createElement("h2", null, "Cluster", /*#__PURE__*/React.createElement("span", {
    style: {
      marginLeft: 'auto',
      fontWeight: 600,
      letterSpacing: 0,
      textTransform: 'none',
      color: 'var(--dim)'
    }
  }, mode === 'partition' ? 'click a node to move it between network groups' : 'click a node to crash or restart it')), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: 6
    }
  }, /*#__PURE__*/React.createElement(Stage, {
    W: W,
    onClick: clickNode,
    mode: mode,
    onTip: setTip,
    onLink: clickLink
  })), /*#__PURE__*/React.createElement("div", {
    className: "legend"
  }, /*#__PURE__*/React.createElement(L, {
    c: "var(--follower)",
    t: "Follower"
  }), /*#__PURE__*/React.createElement(L, {
    c: "var(--candidate)",
    t: "Candidate"
  }), /*#__PURE__*/React.createElement(L, {
    c: "var(--leader)",
    t: "Leader"
  }), /*#__PURE__*/React.createElement(L, {
    c: "var(--down)",
    t: "Crashed"
  }), /*#__PURE__*/React.createElement(L, {
    c: "var(--pv)",
    t: "PreVote"
  }), /*#__PURE__*/React.createElement(L, {
    c: "var(--rv)",
    t: "RequestVote"
  }), /*#__PURE__*/React.createElement(L, {
    c: "var(--grant)",
    t: "granted \u2713"
  }), /*#__PURE__*/React.createElement(L, {
    c: "var(--deny)",
    t: "denied \u2717"
  }), /*#__PURE__*/React.createElement(L, {
    c: "var(--ae)",
    t: "AppendEntries / \u2665 heartbeat"
  }), /*#__PURE__*/React.createElement(L, {
    c: "#ef4444",
    t: "\u2715 link cut both ways"
  }), /*#__PURE__*/React.createElement(L, {
    c: "#f59e0b",
    t: "\u27A4 link cut one way"
  })), /*#__PURE__*/React.createElement("div", {
    className: "hint"
  }, /*#__PURE__*/React.createElement("b", {
    style: {
      color: 'var(--muted)'
    }
  }, "Click a wire"), " between two nodes to cycle it: healthy \u2192 fully cut \u2192 one-way \u2192 the other one-way \u2192 healthy. A one-way cut lets a node send but never receive \u2014 that is how a node becomes unreachable to only ", /*#__PURE__*/React.createElement("i", null, "some"), " peers without being offline. Ring around each node = its election-timeout countdown on a shared scale. Hover any flying message for its RPC payload.")), /*#__PURE__*/React.createElement("div", {
    className: "card"
  }, /*#__PURE__*/React.createElement("h2", null, "Replicated logs ", /*#__PURE__*/React.createElement("span", {
    style: {
      marginLeft: 'auto',
      fontWeight: 600,
      letterSpacing: 0,
      textTransform: 'none',
      color: 'var(--dim)'
    }
  }, "solid = committed \xB7 faded dashed = not yet committed \xB7 colour = term")), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: '3px 0'
    }
  }, W.nodes.map(n => /*#__PURE__*/React.createElement(LogRow, {
    key: n.id,
    node: n,
    leader: leader
  }))))), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("div", {
    className: "card"
  }, /*#__PURE__*/React.createElement("h2", null, "What is happening"), /*#__PURE__*/React.createElement("div", {
    className: "explain"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ttl",
    style: {
      color: ex.tone
    }
  }, ex.ttl), /*#__PURE__*/React.createElement("div", {
    className: "bd",
    dangerouslySetInnerHTML: {
      __html: ex.bd
    }
  }), /*#__PURE__*/React.createElement("div", {
    className: "why"
  }, ex.why), W.note && /*#__PURE__*/React.createElement("div", {
    className: "why",
    style: {
      color: 'var(--muted)'
    }
  }, /*#__PURE__*/React.createElement("b", {
    style: {
      color: 'var(--txt)'
    }
  }, "Scenario: "), W.note))), /*#__PURE__*/React.createElement("div", {
    className: "card"
  }, /*#__PURE__*/React.createElement("h2", null, "Nodes"), /*#__PURE__*/React.createElement("div", {
    className: "insp"
  }, W.nodes.map(n => /*#__PURE__*/React.createElement(NodeRow, {
    key: n.id,
    node: n,
    W: W,
    onClick: clickNode,
    mode: mode
  })))), /*#__PURE__*/React.createElement("div", {
    className: "card"
  }, /*#__PURE__*/React.createElement("h2", null, "Event log"), /*#__PURE__*/React.createElement("div", {
    className: "log"
  }, W.events.map(e => {
    const n = e.nodeId < 0 ? null : byId(W, e.nodeId);
    return /*#__PURE__*/React.createElement("div", {
      className: "row",
      key: e.id
    }, /*#__PURE__*/React.createElement("span", {
      className: "t"
    }, e.t, "s"), /*#__PURE__*/React.createElement("span", {
      className: "n",
      style: {
        color: n ? nodeColor(n.state) : 'var(--dim)'
      }
    }, e.nodeId < 0 ? '—' : 'N' + e.nodeId), /*#__PURE__*/React.createElement("span", {
      style: {
        color: EVENT_COLOR[e.kind] || 'var(--txt)'
      }
    }, e.text));
  }))))), tip && /*#__PURE__*/React.createElement(Tip, {
    tip: tip
  }));
}
const EVENT_COLOR = {
  grant: '#7ee2b8',
  deny: '#f8a5a5',
  leader: '#6ee7b7',
  cand: '#fcd34d',
  commit: '#34d399',
  trunc: '#fca5a5',
  retry: '#c4b5fd',
  crash: '#ef7d7d',
  log: '#93c5fd',
  down: '#cbd5e1',
  sys: '#8aa0d0'
};
const L = ({
  c,
  t
}) => /*#__PURE__*/React.createElement("span", {
  className: "it"
}, /*#__PURE__*/React.createElement("span", {
  className: "dot",
  style: {
    background: c
  }
}), t);
/* ================================================================== *
 *  STAGE                                                              *
 * ================================================================== */
const MSG_STYLE = {
  PreVote: {
    c: 'var(--pv)',
    l: 'PV'
  },
  PreVoteReply: {
    c: 'var(--pv)',
    l: 'pv'
  },
  RequestVote: {
    c: 'var(--rv)',
    l: 'RV'
  },
  RequestVoteReply: {
    c: 'var(--rv)',
    l: 'rv'
  },
  AppendEntries: {
    c: 'var(--ae)',
    l: 'AE'
  },
  AppendEntriesReply: {
    c: 'var(--ae)',
    l: 'ae'
  }
};
function msgLook(m) {
  const base = MSG_STYLE[m.type] || {
    c: '#94a3b8',
    l: '·'
  };
  if (m.type === 'RequestVoteReply' || m.type === 'PreVoteReply') return {
    c: m.payload.granted ? 'var(--grant)' : 'var(--deny)',
    l: m.payload.granted ? '✓' : '✗'
  };
  if (m.type === 'AppendEntriesReply') return {
    c: m.payload.success ? 'var(--grant)' : 'var(--deny)',
    l: m.payload.success ? '✓' : '✗'
  };
  if (m.type === 'AppendEntries') return {
    c: base.c,
    l: m.payload.entries.length ? String(m.payload.entries.length) : '♥'
  };
  return base;
}
function payloadText(m) {
  const p = m.payload;
  const rows = [['type', m.type], ['from', 'N' + m.from], ['to', 'N' + m.to]];
  if (m.type === 'PreVote' || m.type === 'RequestVote') rows.push(['term', p.term], ['candidateId', 'N' + p.candidateId], ['lastLogIndex', p.lastLogIndex], ['lastLogTerm', p.lastLogTerm]);else if (m.type === 'PreVoteReply' || m.type === 'RequestVoteReply') rows.push(['term', p.term], ['granted', String(p.granted)]);else if (m.type === 'AppendEntries') rows.push(['term', p.term], ['leaderId', 'N' + p.leaderId], ['prevLogIndex', p.prevLogIndex], ['prevLogTerm', p.prevLogTerm], ['entries', '[' + p.entries.map(e => `${e.value}@t${e.term}`).join(', ') + ']'], ['leaderCommit', p.leaderCommit]);else if (m.type === 'AppendEntriesReply') rows.push(['term', p.term], ['success', String(p.success)], p.success ? ['matchIndex', p.matchIndex] : ['conflictIndex', p.conflictIndex]);
  return rows;
}

/* One inter-node wire. Fat transparent overlay makes it easy to hit. */
function Wire({
  a,
  b,
  pa,
  pb,
  st,
  onLink,
  onTip
}) {
  const partSplit = a.partition !== b.partition;
  const lo = Math.min(a.id, b.id),
    hi = Math.max(a.id, b.id);
  const mx = (pa.x + pb.x) / 2,
    my = (pa.y + pb.y) / 2;
  let stroke = '#1b2650',
    dash = 'none',
    w = 1;
  if (partSplit) {
    stroke = '#3b1d2c';
    dash = '2 8';
  }
  if (st === 'cut') {
    stroke = '#ef4444';
    dash = '4 5';
    w = 1.6;
  } else if (st) {
    stroke = '#f59e0b';
    dash = '7 5';
    w = 1.6;
  }

  // for a one-way cut, point the arrow along the DEAD direction
  const src = st === 'lo2hi' ? lo : hi,
    dst = st === 'lo2hi' ? hi : lo;
  const sp = src === a.id ? pa : pb,
    dp = dst === a.id ? pa : pb;
  const ang = Math.atan2(dp.y - sp.y, dp.x - sp.x) * 180 / Math.PI;
  const label = st === 'cut' ? `N${lo} ⇄ N${hi} fully cut` : st ? `N${src} ⇢ N${dst} blocked (reverse still works)` : `N${lo} ⇄ N${hi} healthy — click to cut`;
  return /*#__PURE__*/React.createElement("g", null, /*#__PURE__*/React.createElement("line", {
    x1: pa.x,
    y1: pa.y,
    x2: pb.x,
    y2: pb.y,
    stroke: stroke,
    strokeWidth: w,
    strokeDasharray: dash
  }), /*#__PURE__*/React.createElement("line", {
    x1: pa.x,
    y1: pa.y,
    x2: pb.x,
    y2: pb.y,
    stroke: "transparent",
    strokeWidth: "14",
    style: {
      cursor: 'pointer'
    },
    onClick: e => {
      e.stopPropagation();
      onLink(a.id, b.id);
    },
    onMouseEnter: e => onTip({
      rows: [['link', label], ['click', 'cycles healthy → cut → one-way → one-way → healthy']],
      x: e.clientX,
      y: e.clientY
    }),
    onMouseLeave: () => onTip(null)
  }), st === 'cut' && /*#__PURE__*/React.createElement("g", {
    pointerEvents: "none"
  }, /*#__PURE__*/React.createElement("circle", {
    cx: mx,
    cy: my,
    r: "9",
    fill: "#2a1420",
    stroke: "#ef4444",
    strokeWidth: "1.5"
  }), /*#__PURE__*/React.createElement("text", {
    x: mx,
    y: my + 3.5,
    textAnchor: "middle",
    fontSize: "10",
    fontWeight: "900",
    fill: "#ef4444"
  }, "\u2715")), st && st !== 'cut' && /*#__PURE__*/React.createElement("g", {
    pointerEvents: "none",
    transform: `translate(${mx},${my}) rotate(${ang})`
  }, /*#__PURE__*/React.createElement("path", {
    d: "M -9 -5 L 2 0 L -9 5 Z",
    fill: "#f59e0b"
  }), /*#__PURE__*/React.createElement("line", {
    x1: "4",
    y1: "-7",
    x2: "12",
    y2: "7",
    stroke: "#ef4444",
    strokeWidth: "2.2"
  }), /*#__PURE__*/React.createElement("line", {
    x1: "12",
    y1: "-7",
    x2: "4",
    y2: "7",
    stroke: "#ef4444",
    strokeWidth: "2.2"
  })));
}
function Stage({
  W,
  onClick,
  mode,
  onTip,
  onLink
}) {
  const VW = 700,
    VH = 580,
    NR = 33;
  const nodes = W.nodes;
  const lay = computeLayout(nodes, VW, VH);
  const pos = lay.pos;
  const q = quorum(nodes.length);
  return /*#__PURE__*/React.createElement("svg", {
    className: "stage",
    viewBox: `0 0 ${VW} ${VH}`,
    onMouseLeave: () => onTip(null)
  }, /*#__PURE__*/React.createElement("defs", null, /*#__PURE__*/React.createElement("filter", {
    id: "gl",
    x: "-60%",
    y: "-60%",
    width: "220%",
    height: "220%"
  }, /*#__PURE__*/React.createElement("feGaussianBlur", {
    stdDeviation: "4",
    result: "b"
  }), /*#__PURE__*/React.createElement("feMerge", null, /*#__PURE__*/React.createElement("feMergeNode", {
    in: "b"
  }), /*#__PURE__*/React.createElement("feMergeNode", {
    in: "SourceGraphic"
  })))), lay.groups.map(g => {
    const maj = g.size >= q;
    const col = maj ? 'var(--leader)' : 'var(--down)';
    return /*#__PURE__*/React.createElement("g", {
      key: g.key
    }, /*#__PURE__*/React.createElement("circle", {
      cx: g.gx,
      cy: g.gy,
      r: g.r,
      fill: maj ? '#0d2b22' : '#2a1420',
      opacity: "0.5",
      stroke: col,
      strokeWidth: "1.5",
      strokeDasharray: "7 6"
    }), /*#__PURE__*/React.createElement("text", {
      x: g.gx,
      y: g.gy - g.r - 9,
      textAnchor: "middle",
      fontSize: "11",
      fontWeight: "800",
      fill: col,
      letterSpacing: "1"
    }, "NETWORK ", GROUP_NAMES[g.key], " \xB7 ", g.size, " node", g.size > 1 ? 's' : '', " \xB7 ", maj ? 'MAJORITY' : 'minority — cannot elect'));
  }), nodes.map((a, i) => nodes.slice(i + 1).map(b => /*#__PURE__*/React.createElement(Wire, {
    key: a.id + '_' + b.id,
    a: a,
    b: b,
    pa: pos[a.id],
    pb: pos[b.id],
    st: W.links[linkKey(a.id, b.id)],
    onLink: onLink,
    onTip: onTip
  }))), W.msgs.map(m => {
    const p = pos[m.from],
      r = pos[m.to];
    if (!p || !r) return null;
    const t = Math.min(m.progress, 1);
    const x = p.x + (r.x - p.x) * t,
      y = p.y + (r.y - p.y) * t;
    const look = msgLook(m);
    if (m.dead) {
      const o = Math.max(0, 1 - m.fade / 450);
      return /*#__PURE__*/React.createElement("g", {
        key: m.id,
        opacity: o
      }, /*#__PURE__*/React.createElement("circle", {
        cx: x,
        cy: y,
        r: 11 + m.fade / 28,
        fill: "none",
        stroke: "var(--down)",
        strokeWidth: "2"
      }), /*#__PURE__*/React.createElement("text", {
        x: x,
        y: y + 4,
        textAnchor: "middle",
        fontSize: "13",
        fontWeight: "900",
        fill: "var(--down)"
      }, "\u2715"), /*#__PURE__*/React.createElement("text", {
        x: x,
        y: y - 16,
        textAnchor: "middle",
        fontSize: "8.5",
        fontWeight: "800",
        fill: "var(--down)"
      }, m.reason));
    }
    return /*#__PURE__*/React.createElement("g", {
      key: m.id,
      style: {
        cursor: 'help'
      },
      onMouseEnter: e => onTip({
        rows: payloadText(m),
        x: e.clientX,
        y: e.clientY
      })
    }, /*#__PURE__*/React.createElement("line", {
      x1: p.x,
      y1: p.y,
      x2: x,
      y2: y,
      stroke: look.c,
      strokeWidth: "1.3",
      opacity: "0.3",
      strokeDasharray: "3 4"
    }), /*#__PURE__*/React.createElement("circle", {
      cx: x,
      cy: y,
      r: "13",
      fill: look.c,
      opacity: "0.15"
    }), /*#__PURE__*/React.createElement("circle", {
      cx: x,
      cy: y,
      r: "8.5",
      fill: look.c,
      filter: "url(#gl)"
    }), /*#__PURE__*/React.createElement("text", {
      x: x,
      y: y + 3.4,
      textAnchor: "middle",
      fontSize: "9",
      fontWeight: "900",
      fill: "#08122b",
      pointerEvents: "none"
    }, look.l));
  }), nodes.map(n => {
    const {
      x,
      y
    } = pos[n.id];
    const col = nodeColor(n.state);
    const down = n.state === 'down';
    const showRing = n.state === 'follower' || n.state === 'candidate';
    const frac = Math.max(0, Math.min(1, n.timeout / ELECTION_MAX));
    const C = 2 * Math.PI * (NR + 7);
    const votes = n.state === 'candidate' ? Object.keys(n.votes).length : n.phase === 'prevote' ? Object.keys(n.preVotes).length : 0;
    return /*#__PURE__*/React.createElement("g", {
      key: n.id,
      style: {
        cursor: 'pointer'
      },
      onClick: () => onClick(n.id)
    }, showRing && /*#__PURE__*/React.createElement("circle", {
      cx: x,
      cy: y,
      r: NR + 7,
      fill: "none",
      stroke: "#22305c",
      strokeWidth: "4"
    }), showRing && /*#__PURE__*/React.createElement("circle", {
      cx: x,
      cy: y,
      r: NR + 7,
      fill: "none",
      stroke: n.phase === 'prevote' ? 'var(--pv)' : n.state === 'candidate' ? 'var(--candidate)' : '#3d5aa8',
      strokeWidth: "4",
      strokeLinecap: "round",
      strokeDasharray: C,
      strokeDashoffset: C * (1 - frac),
      transform: `rotate(-90 ${x} ${y})`
    }), n.state === 'leader' && /*#__PURE__*/React.createElement("circle", {
      cx: x,
      cy: y,
      r: NR + 8,
      fill: "none",
      stroke: "var(--leader)",
      strokeWidth: "2"
    }, /*#__PURE__*/React.createElement("animate", {
      attributeName: "r",
      values: `${NR + 6};${NR + 17};${NR + 6}`,
      dur: "1.5s",
      repeatCount: "indefinite"
    }), /*#__PURE__*/React.createElement("animate", {
      attributeName: "opacity",
      values: "0.6;0;0.6",
      dur: "1.5s",
      repeatCount: "indefinite"
    })), /*#__PURE__*/React.createElement("circle", {
      cx: x,
      cy: y,
      r: NR,
      fill: down ? '#2a1420' : '#0f1a3a',
      stroke: col,
      strokeWidth: n.state === 'leader' ? 4 : 2.5,
      strokeDasharray: down ? '5 4' : 'none',
      filter: n.state === 'leader' ? 'url(#gl)' : 'none'
    }), /*#__PURE__*/React.createElement("text", {
      x: x,
      y: y - 5,
      textAnchor: "middle",
      fontSize: "15",
      fontWeight: "900",
      fill: col
    }, "N", n.id), /*#__PURE__*/React.createElement("text", {
      x: x,
      y: y + 9,
      textAnchor: "middle",
      fontSize: "9",
      fontWeight: "700",
      fill: "var(--muted)"
    }, "term ", n.currentTerm), /*#__PURE__*/React.createElement("text", {
      x: x,
      y: y + 20,
      textAnchor: "middle",
      fontSize: "8",
      fontWeight: "700",
      fill: "var(--dim)"
    }, "log ", lastIndex(n), "\xB7c", n.commitIndex), /*#__PURE__*/React.createElement("text", {
      x: x,
      y: y + NR + 15,
      textAnchor: "middle",
      fontSize: "10",
      fontWeight: "900",
      fill: col
    }, n.phase === 'prevote' ? 'PRE-VOTE' : n.state.toUpperCase()), votes > 0 && /*#__PURE__*/React.createElement("g", null, /*#__PURE__*/React.createElement("rect", {
      x: x + NR - 8,
      y: y - NR - 8,
      width: "34",
      height: "19",
      rx: "9.5",
      fill: n.phase === 'prevote' ? 'var(--pv)' : 'var(--candidate)'
    }), /*#__PURE__*/React.createElement("text", {
      x: x + NR + 9,
      y: y - NR + 5.5,
      textAnchor: "middle",
      fontSize: "10",
      fontWeight: "900",
      fill: "#2a1c00"
    }, votes, "/", q)), n.state === 'leader' && /*#__PURE__*/React.createElement("text", {
      x: x,
      y: y - NR - 10,
      textAnchor: "middle",
      fontSize: "15"
    }, "\uD83D\uDC51"), down && /*#__PURE__*/React.createElement("text", {
      x: x,
      y: y - NR - 10,
      textAnchor: "middle",
      fontSize: "14"
    }, "\uD83D\uDC80"), mode === 'partition' && /*#__PURE__*/React.createElement("text", {
      x: x - NR - 4,
      y: y - NR + 2,
      textAnchor: "middle",
      fontSize: "11",
      fontWeight: "900",
      fill: "var(--muted)"
    }, GROUP_NAMES[n.partition]));
  }));
}

/* ================================================================== *
 *  SIDE PANELS                                                        *
 * ================================================================== */
function NodeRow({
  node,
  W,
  onClick,
  mode
}) {
  const col = nodeColor(node.state);
  const frac = Math.max(0, Math.min(1, node.timeout / ELECTION_MAX));
  const leader = W.nodes.find(n => n.state === 'leader');
  const isFollowerOfLeader = leader && leader.id !== node.id && node.state !== 'down';
  return /*#__PURE__*/React.createElement("div", {
    className: "nrow"
  }, /*#__PURE__*/React.createElement("div", {
    className: "top"
  }, /*#__PURE__*/React.createElement("b", {
    style: {
      color: col
    }
  }, "N", node.id), /*#__PURE__*/React.createElement("span", {
    className: "pill",
    style: {
      background: col,
      color: '#07102a'
    }
  }, node.phase === 'prevote' ? 'pre-vote' : node.state), mode === 'partition' && /*#__PURE__*/React.createElement("span", {
    className: "pill",
    style: {
      background: '#243466',
      color: 'var(--txt)'
    }
  }, "net ", GROUP_NAMES[node.partition]), /*#__PURE__*/React.createElement("button", {
    className: "mini",
    onClick: () => onClick(node.id)
  }, mode === 'partition' ? 'move net' : node.state === 'down' ? 'restart' : 'crash')), /*#__PURE__*/React.createElement("div", {
    className: "meta"
  }, /*#__PURE__*/React.createElement("span", null, "term ", node.currentTerm), /*#__PURE__*/React.createElement("span", null, "votedFor ", node.votedFor === null ? '∅' : 'N' + node.votedFor), /*#__PURE__*/React.createElement("span", null, "log ", lastIndex(node)), /*#__PURE__*/React.createElement("span", null, "commit ", node.commitIndex)), node.state === 'leader' && /*#__PURE__*/React.createElement("div", {
    className: "meta",
    style: {
      color: 'var(--muted)'
    }
  }, W.nodes.filter(o => o.id !== node.id).map(o => /*#__PURE__*/React.createElement("span", {
    key: o.id,
    title: "nextIndex / matchIndex"
  }, "N", o.id, ":", /*#__PURE__*/React.createElement("b", {
    style: {
      color: '#c4b5fd'
    }
  }, node.nextIndex[o.id] ?? '?'), "/", /*#__PURE__*/React.createElement("b", {
    style: {
      color: '#7ee2b8'
    }
  }, node.matchIndex[o.id] ?? 0)))), (node.state === 'follower' || node.state === 'candidate') && /*#__PURE__*/React.createElement("div", {
    className: "tobar"
  }, /*#__PURE__*/React.createElement("i", {
    style: {
      width: frac * 100 + '%',
      background: node.phase === 'prevote' ? 'var(--pv)' : node.state === 'candidate' ? 'var(--candidate)' : '#3d5aa8'
    }
  })));
}
function LogRow({
  node,
  leader
}) {
  const col = nodeColor(node.state);
  const nx = leader && leader.id !== node.id ? leader.nextIndex[node.id] : null;
  return /*#__PURE__*/React.createElement("div", {
    className: "lrRow",
    style: {
      opacity: node.state === 'down' ? 0.45 : 1
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "lrName"
  }, /*#__PURE__*/React.createElement("span", {
    className: "dot",
    style: {
      background: col,
      width: 9,
      height: 9
    }
  }), "N", node.id, node.state === 'leader' && ' 👑', node.state === 'down' && ' 💀'), /*#__PURE__*/React.createElement("div", {
    className: "entries"
  }, node.log.length === 0 && /*#__PURE__*/React.createElement("span", {
    className: "empty"
  }, "empty log"), node.log.map((e, i) => {
    const c = termColor(e.term);
    const committed = i + 1 <= node.commitIndex;
    return /*#__PURE__*/React.createElement("div", {
      key: i,
      className: 'ent ' + (committed ? '' : 'pending'),
      title: `index ${i + 1} · term ${e.term} · ${committed ? 'committed' : 'not committed'}`,
      style: {
        background: c.bg,
        borderColor: c.br,
        color: c.fg
      }
    }, e.value, /*#__PURE__*/React.createElement("small", null, i + 1, "\xB7t", e.term));
  }), nx != null && /*#__PURE__*/React.createElement("span", {
    style: {
      fontSize: 10.5,
      color: '#c4b5fd',
      fontFamily: 'Consolas',
      marginLeft: 6,
      whiteSpace: 'nowrap'
    }
  }, "next\u2192", nx)));
}
function Tip({
  tip
}) {
  const style = {
    left: Math.min(tip.x + 16, window.innerWidth - 310),
    top: tip.y + 16
  };
  return /*#__PURE__*/React.createElement("div", {
    className: "tip",
    style: style
  }, tip.rows.map(([k, v], i) => /*#__PURE__*/React.createElement("div", {
    key: i
  }, /*#__PURE__*/React.createElement("span", {
    className: "k"
  }, k, ": "), String(v))));
}
try {
  ReactDOM.createRoot(document.getElementById('root')).render(/*#__PURE__*/React.createElement(App, null));
} catch (err) {
  window.__raftPanic('Render failed', err && err.stack || String(err));
}
