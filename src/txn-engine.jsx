/* Fault-tolerant two-phase commit built on top of the Raft engine above.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ================================================================== *
 *  FAULT-TOLERANT TWO-PHASE COMMIT                                    *
 *                                                                     *
 *  Plain 2PC has one famous flaw: the coordinator writes its decision  *
 *  to one disk on one machine. Kill it between "collect the votes" and *
 *  "announce the outcome" and every participant sits in PREPARED,      *
 *  holding locks, forbidden to guess, forever.                        *
 *                                                                     *
 *  Gray & Lamport, "Consensus on Transaction Commit" (2006): make      *
 *  every decision a consensus-replicated log record instead of one     *
 *  node's write. That is what Spanner does — 2PC across Paxos groups.  *
 *  Here it is 2PC across Raft groups, using the same engine above.     *
 *                                                                     *
 *  The rule this layer never breaks: nothing is said out loud until    *
 *  it has been committed by the Raft group that says it. Every arrow   *
 *  on the screen is preceded by a commit in the sending group.         *
 * ================================================================== */
/* The prepare deadline has to outlast a participant losing its leader and
   electing a new one — which can cost a split vote, so budget two elections
   (2*ELECTION_MAX) plus the round trips and the new leader's no-op. Tighter
   than that and the coordinator presumes abort on faults the cluster would
   have survived: a real 2PC tuning mistake, just not the one this view is
   here to teach. */
const PREPARE_TIMEOUT = 25000;  // coordinator waits this long for votes, then presumes abort
const TXN_RETRY       = 2500;   // re-send an unanswered Prepare / Decide
const SHARD_NAMES     = ['A','B','C'];

/* ------------------------------------------------------------------ *
 *  World                                                              *
 * ------------------------------------------------------------------ */
function makeGroup(id, role, size, cfg){
  const W = makeWorld(size, cfg);
  return {
    id, role,                                   // 'coord' | 'shard'
    name: role==='coord' ? 'COORD' : 'SHARD '+SHARD_NAMES[id-1],
    W,                                          // a full, live Raft world
    partition: 0,                               // cross-group network group
    willVote: 'yes',                            // what this shard answers when asked
  };
}

/* `id` and `partition` are named to match the node fields that Wire and
   linkBlocks already read, so both work on group objects unchanged. */
function makeTxnWorld(coordSize, shardSizes, cfg){
  const groups = [ makeGroup(0,'coord',coordSize,cfg) ];
  shardSizes.forEach((k,i)=> groups.push(makeGroup(i+1,'shard',k,cfg)));
  return {
    groups, msgs:[], events:[], cfg, links:{},
    txn:null, pending:[], txnSeq:0,
    clock:0, armed:null, note:null,
    nextMid:0, nextEid:0, coordLeader:null, coordGone:false,
  };
}

function emitT(T, gid, text, kind){
  T.events.unshift({ id:T.nextEid++, t:(T.clock/1000).toFixed(1), nodeId:gid, text, kind:kind||'' });
  if(T.events.length>160) T.events.length=160;
}

const groupLeader = g => g.W.nodes.find(n=>n.state==='leader');
const shardsOf    = T => T.groups.filter(g=>g.role==='shard');

/* ------------------------------------------------------------------ *
 *  Durable state lives in the logs — nothing about the transaction is  *
 *  stored anywhere else. That is what makes coordinator recovery fall  *
 *  out for free instead of needing a recovery path of its own.         *
 * ------------------------------------------------------------------ */
function scanLog(n, txnId, upTo){
  const r = { begin:false, decision:null, vote:null, applied:null, at:{} };
  const top = Math.min(upTo, n.log.length);
  for(let i=1;i<=top;i++){
    const e = n.log[i-1];
    if(!e || e.txn!==txnId) continue;
    if(e.rec==='begin')         { r.begin=true;      r.at.begin=i; }
    else if(e.rec==='decision') { r.decision=e.dec;  r.at.decision=i; }
    else if(e.rec==='prepared') { r.vote='yes';      r.at.vote=i; }
    else if(e.rec==='novote')   { r.vote='no';       r.at.vote=i; }
    else if(e.rec==='applied')  { r.applied=e.dec;   r.at.applied=i; }
  }
  return r;
}
/* committedOn gates ACTIONS: only a committed fact may be acted on.
   anywhereOn gates the DEDUPE GUARD: a leader must never append a second
   decision while it already holds an uncommitted one, or a crash could
   commit both abort@4 and commit@9. Sound because in Raft only entries in
   the current leader's log can ever commit. */
const committedOn = (n,id)=> scanLog(n, id, n.commitIndex);
const anywhereOn  = (n,id)=> scanLog(n, id, n.log.length);

/* What a GROUP durably knows, as opposed to what its leader can currently
   say. Scans every node on purpose: a vote that survives on the followers
   after the leader dies is still durable, and that distinction is the whole
   point of the exercise. Only the leader is ever allowed to answer, though. */
function shardFate(T, g, txnId){
  const id = txnId || (T.txn && T.txn.id);
  let vote=null, applied=null;
  if(id) for(const n of g.W.nodes){
    const r = committedOn(n, id);
    if(r.vote && !vote) vote = r.vote;
    if(r.applied && !applied) applied = r.applied;
  }
  return { vote, applied, locked: vote==='yes' && applied===null };
}
function coordFate(T, txnId){
  const id = txnId || (T.txn && T.txn.id);
  let begin=false, decision=null;
  if(id) for(const n of T.groups[0].W.nodes){
    const r = committedOn(n, id);
    if(r.begin) begin = true;
    if(r.decision && !decision) decision = r.decision;
  }
  return { begin, decision };
}

function txnPhase(T){
  if(!T.txn) return 'idle';
  const c = coordFate(T);
  if(!c.begin)    return 'beginning';
  if(!c.decision) return 'preparing';
  const done = shardsOf(T).every(g=>shardFate(T,g).applied);
  if(c.decision==='commit') return done ? 'committed' : 'committing';
  return done ? 'aborted' : 'aborting';
}
const txnDone   = T => { const p = txnPhase(T); return p==='committed' || p==='aborted' || p==='idle'; };
const txnLocked = T => shardsOf(T).filter(g=>shardFate(T,g).locked);

/* ------------------------------------------------------------------ *
 *  Raft <-> 2PC coupling: append, then wait for the commit, then act.  *
 *                                                                     *
 *  advanceCommit only runs inside deliver()'s AppendEntriesReply       *
 *  branch and there is no hook there. Rather than teach the Raft       *
 *  engine about transactions, park a continuation and poll it.         *
 * ------------------------------------------------------------------ */
function raftAppend(T, g, entry, kind){
  const L = groupLeader(g);
  if(!L) return false;                            // caller retries later
  L.log.push({ term:L.currentTerm, ...entry });
  const index = lastIndex(L);
  emit(g.W, L.id, `2PC ${entry.rec.toUpperCase()} for ${entry.txn} at index ${index} — not durable until a majority has it`, 'log');
  broadcastAppendEntries(g.W, L);
  L.hbTimer = HEARTBEAT;
  advanceCommit(g.W, L);   // a one-node group has no follower to reply for it
  T.pending.push({ gid:g.id, index, term:L.currentTerm, kind, txn:entry.txn, dec:entry.dec, done:false });
  return true;
}

function pumpPending(T){
  for(const p of T.pending){
    const g = T.groups[p.gid];
    if(!g){ p.done = true; continue; }             // group removed under us
    const L = groupLeader(g);
    if(!L) continue;                               // mid-election: just wait
    if(L.commitIndex >= p.index && termAt(L,p.index) === p.term){
      p.done = true; onCommitted(T, g, p);
    } else if(L.currentTerm > p.term && termAt(L,p.index) !== p.term){
      // While the term has not moved, the same leader still holds it (Log
      // Matching). Once it has, the entry is lost iff the current leader does
      // not hold it here. Nothing is re-appended: the driving step notices the
      // record is missing on its next retry and writes it again.
      p.done = true;
      emitT(T, g.id, `the ${p.kind} record for ${p.txn} was truncated by the new leader of term ${L.currentTerm} — it never committed`, 'block');
    }
  }
  if(T.pending.some(p=>p.done)) T.pending = T.pending.filter(p=>!p.done);
}

function onCommitted(T, g, p){
  if(p.kind==='begin'){
    emitT(T, 0, `BEGIN ${p.txn} is durable on a majority of COORD — only now does PREPARE go out`, 'prep');
    // The clock on the participants starts when they are actually asked, not
    // when the client said "begin" — making BEGIN durable can itself take a
    // coordinator election, and that time is not the shards' fault.
    if(T.txn) T.txn.deadline = T.clock + PREPARE_TIMEOUT;
    sendPrepares(T);
  } else if(p.kind==='vote'){
    emitT(T, g.id, `its ${p.dec==='yes'?'YES':'NO'} for ${p.txn} is durable at index ${p.index} — answering the coordinator`, p.dec==='yes'?'prep':'block');
    sendTxn(T, g.id, 0, 'Prepared', { txn:p.txn, vote:p.dec, index:p.index });
  } else if(p.kind==='decision'){
    emitT(T, 0, `decision ${p.dec.toUpperCase()} for ${p.txn} is DURABLE at index ${p.index} — losing the coordinator can no longer lose it`, 'decide');
    sendDecides(T);
  } else if(p.kind==='applied'){
    emitT(T, g.id, `applied ${p.dec.toUpperCase()} for ${p.txn} — locks released`, 'decide');
    sendTxn(T, g.id, 0, 'Ack', { txn:p.txn, dec:p.dec, index:p.index });
  }
}

/* ------------------------------------------------------------------ *
 *  Cross-group transport — leader to leader, over T.msgs.             *
 *  A parallel of send() rather than a generalisation of it: send has   *
 *  eight call sites inside the Raft engine and its shape is what the   *
 *  safety suite drives.                                               *
 * ------------------------------------------------------------------ */
function sendTxn(T, from, to, type, payload){
  const j = T.cfg.jitter;
  const a = T.groups[from], b = T.groups[to];
  const la = a && groupLeader(a), lb = b && groupLeader(b);
  T.msgs.push({
    id:T.nextMid++, from, to, type, payload,
    fromNode: la ? la.id : null,     // captured for the tooltip only; never geometry
    toNode:   lb ? lb.id : null,
    progress:0,
    travel: TRAVEL * rand(1-j, 1+j),
    willDrop: Math.random() < T.cfg.dropRate,
    dead:false, fade:0, delivered:false,
  });
}

function txnBlockReason(T, m){
  const a = T.groups[m.from], b = T.groups[m.to];
  if(!a || !b) return 'group gone';
  if(a.partition !== b.partition) return 'partitioned';
  if(linkBlocks(T, m.from, m.to)) return 'link cut';
  if(!groupLeader(a)) return 'sender lost leadership';
  if(!groupLeader(b)) return 'no leader there';
  if(m.willDrop) return 'packet lost';
  return null;
}

function sendPrepares(T){
  const t = T.txn; if(!t) return;
  for(const g of shardsOf(T)) if(!t.votes[g.id]) sendTxn(T, 0, g.id, 'Prepare', { txn:t.id });
  t.lastRetry = T.clock;
}
function sendDecides(T){
  const t = T.txn; if(!t) return;
  const dec = coordFate(T).decision;
  if(!dec) return;
  for(const g of shardsOf(T)) if(!t.acks[g.id]) sendTxn(T, 0, g.id, 'Decide', { txn:t.id, dec });
  t.lastRetry = T.clock;
}

/* The destination leader is resolved HERE, at delivery, never at send time —
   which is what lets a brand-new leader answer for a transaction it has
   never heard of, straight out of the log its predecessor replicated. */
function deliverTxn(T, m){
  const g = T.groups[m.to];  if(!g) return;
  const L = groupLeader(g);  if(!L) return;
  const p = m.payload;
  if(!T.txn || T.txn.id !== p.txn) return;        // stale transaction

  if(m.type==='Prepare'){
    const C = committedOn(L, p.txn), A = anywhereOn(L, p.txn);
    if(C.vote){
      emitT(T, g.id, `answers ${C.vote==='yes'?'PREPARED':'NO'} for ${p.txn} from its committed log (index ${C.at.vote}) — this leader may never have seen the original request`, 'prep');
      sendTxn(T, g.id, 0, 'Prepared', { txn:p.txn, vote:C.vote, index:C.at.vote });
    } else if(A.vote){
      // Written but not committed. Saying it out loud would be a promise the
      // group cannot keep if this leader dies before the entry commits.
      emit(g.W, L.id, `holds an uncommitted ${p.txn} vote at index ${A.at.vote} from term ${termAt(L,A.at.vote)} — it cannot answer until that commits`, 'retry');
    } else {
      const yes = g.willVote !== 'no';
      emitT(T, g.id, `votes ${yes?'YES':'NO'} on ${p.txn} — into its own Raft log first, so the promise outlives this node`, yes?'prep':'block');
      raftAppend(T, g, { value: yes?'Y':'N', txn:p.txn, rec: yes?'prepared':'novote', dec: yes?'yes':'no' }, 'vote');
    }
    return;
  }

  if(m.type==='Prepared'){
    T.txn.votes[m.from] = p.vote;
    const got = Object.keys(T.txn.votes).length;
    emitT(T, 0, `records ${T.groups[m.from].name} voted ${p.vote.toUpperCase()} (${got}/${shardsOf(T).length} in)`, p.vote==='yes'?'prep':'block');
    return;
  }

  if(m.type==='Decide'){
    const C = committedOn(L, p.txn), A = anywhereOn(L, p.txn);
    if(C.applied){ sendTxn(T, g.id, 0, 'Ack', { txn:p.txn, dec:C.applied, index:C.at.applied }); return; }
    if(A.applied) return;                          // in flight — do not double-apply
    emitT(T, g.id, `learns the decision is ${p.dec.toUpperCase()} — records it before releasing anything`, 'decide');
    raftAppend(T, g, { value: p.dec==='commit'?'✔':'✘', txn:p.txn, rec:'applied', dec:p.dec }, 'applied');
    return;
  }

  if(m.type==='Ack'){ T.txn.acks[m.from] = p.dec; return; }
}

/* ------------------------------------------------------------------ *
 *  The coordinator, driven entirely by what its log says.             *
 * ------------------------------------------------------------------ */
function coordinatorStep(T){
  const t = T.txn; if(!t) return;
  const g0 = T.groups[0];
  const L = groupLeader(g0);
  // No coordinator leader: 2PC simply stops. This IS the blocking window. Note
  // that whatever tally we are holding belonged to a leader that is now gone —
  // comparing ids alone would miss a crash-and-restart of the SAME node, which
  // is exactly what "Coordinator dies — one machine" invites you to try. Its
  // in-memory votes did not survive that; only its log did.
  if(!L){ if(T.coordLeader !== null) T.coordGone = true; return; }

  // A new leader inherits the durable records but none of the tallies.
  if(T.coordLeader !== L.id || T.coordGone){
    T.coordGone = false;
    if(T.coordLeader !== null){
      t.votes = {}; t.acks = {};
      t.deadline  = T.clock + PREPARE_TIMEOUT;
      t.lastRetry = T.clock - TXN_RETRY;
      emitT(T, 0, `COORD leader is now N${L.id} — the in-memory vote tally died with the old one, the durable records did not`, 'block');
    }
    T.coordLeader = L.id;
  }

  const C = committedOn(L, t.id), A = anywhereOn(L, t.id);
  if(!A.begin){ raftAppend(T, g0, { value:'B', txn:t.id, rec:'begin' }, 'begin'); return; }
  if(!C.begin) return;                    // BEGIN not durable yet — nobody is asked anything

  const shards = shardsOf(T);
  if(C.decision === null){
    if(A.decision !== null) return;       // a decision is written and settling; never append a second
    const votes = shards.map(g=>t.votes[g.id]);
    if(votes.some(v=>v==='no')){
      emitT(T, 0, `a shard refused — recording ABORT`, 'block');
      raftAppend(T, g0, { value:'✘', txn:t.id, rec:'decision', dec:'abort' }, 'decision');
    } else if(shards.length && votes.every(v=>v==='yes')){
      emitT(T, 0, `every shard voted YES — recording COMMIT`, 'decide');
      raftAppend(T, g0, { value:'✔', txn:t.id, rec:'decision', dec:'commit' }, 'decision');
    } else if(T.clock > t.deadline){
      emitT(T, 0, `prepare deadline expired with ${votes.filter(Boolean).length}/${shards.length} votes in — PRESUMED ABORT. It is not allowed to wait, and it is not allowed to guess yes`, 'block');
      raftAppend(T, g0, { value:'✘', txn:t.id, rec:'decision', dec:'abort' }, 'decision');
    } else if(T.clock - t.lastRetry > TXN_RETRY) sendPrepares(T);
    return;
  }
  if(shards.every(g=>t.acks[g.id])) return;                     // terminal
  if(T.clock - t.lastRetry > TXN_RETRY) sendDecides(T);
}

function beginTxn(T){
  if(T.txn && !txnDone(T)){
    emitT(T, 0, `${T.txn.id} is still running — let it finish first`, 'block');
    return false;
  }
  T.txn = { id:'T'+(++T.txnSeq), votes:{}, acks:{},
            deadline:T.clock+PREPARE_TIMEOUT, lastRetry:T.clock-TXN_RETRY, startedAt:T.clock };
  T.pending = [];
  T.coordLeader = null; T.coordGone = false;
  emitT(T, 0, `client begins ${T.txn.id} across ${shardsOf(T).length} shards`, 'sys');
  return true;
}
function resetTxn(T, why){
  if(!T.txn) return;
  emitT(T, 0, `${T.txn.id} discarded — ${why}`, 'sys');
  T.txn = null; T.pending = []; T.msgs = []; T.coordLeader = null; T.coordGone = false;
}

/* ------------------------------------------------------------------ *
 *  One 2PC tick: every group's Raft, then the cross-group wire, then   *
 *  the commit continuations, then the coordinator.                     *
 * ------------------------------------------------------------------ */
function tickTxn(T, dt){
  T.clock += dt;

  for(const g of T.groups){
    tick(g.W, dt);
    const L = groupLeader(g);
    if(L && g.W.nodes.length===1) advanceCommit(g.W, L);   // nobody will ever reply to it
  }

  for(const m of T.msgs){
    if(m.dead){ m.fade += dt; continue; }
    const reason = txnBlockReason(T,m);
    if(reason && m.progress >= dieAt(reason)){
      m.dead=true; m.fade=0; m.reason=reason; continue;
    }
    m.progress += dt / m.travel;
    if(m.progress>=1){
      if(!reason) deliverTxn(T,m);
      m.delivered=true;
    }
  }
  T.msgs = T.msgs.filter(m => !m.delivered && !(m.dead && m.fade>450));

  pumpPending(T);
  coordinatorStep(T);

  if(T.armed){ if(T.armed(T)) T.armed = null; }
}
