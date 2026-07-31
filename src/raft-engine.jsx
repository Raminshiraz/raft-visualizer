/* The Raft engine: elections, replication, RPC delivery, message fate, tick.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ================================================================== *
 *  RAFT ENGINE                                                        *
 *  Plain functions over a mutable world object `W`:                   *
 *    { nodes, msgs, events, cfg, cmdSeq, armed, clock }               *
 * ================================================================== */
const byId   = (W,id)=> W.nodes.find(n=>n.id===id);
const nodeCount = W => W.nodes.length;

function emit(W, nodeId, text, kind){
  W.events.unshift({ id:W.nextEid++, t:(W.clock/1000).toFixed(1), nodeId, text, kind:kind||'' });
  if(W.events.length>160) W.events.length=160;
}

function send(W, from, to, type, payload){
  const jitter = W.cfg.jitter;
  W.msgs.push({
    id:W.nextMid++, from, to, type, payload,
    progress:0,
    travel: TRAVEL * rand(1-jitter, 1+jitter),
    willDrop: Math.random() < W.cfg.dropRate,
    dropAt: rand(0.3,0.7),
    dead:false, fade:0, delivered:false,
  });
}

function resetTimeout(W,n){
  n.timeout = rand(ELECTION_MIN, ELECTION_MAX);
  n.timeoutInit = n.timeout;
}

/* Rule for all servers: term seen > ours  =>  become follower in that term. */
function stepDown(W, n, term, why){
  const was = n.state;
  n.currentTerm = term;
  n.votedFor = null;          // new term, vote is fresh again
  n.state = 'follower';
  n.phase = null;
  n.votes = {}; n.preVotes = {}; n.preVoteTerm = 0;
  n.leaderId = null;
  resetTimeout(W,n);
  if(was!=='follower') emit(W, n.id, `sees term ${term} ${why} → steps down from ${was.toUpperCase()} to FOLLOWER`, 'down');
}

/* ------------------------------------------------------------------ *
 *  Elections                                                          *
 * ------------------------------------------------------------------ */
function beginPreVote(W,n){
  // PreVote (Ongaro thesis §9.6): probe for votes WITHOUT bumping our term,
  // so a partitioned node cannot inflate the cluster's term and disrupt a
  // healthy leader when it reconnects.
  const retry = n.phase==='prevote';
  n.state = 'follower';       // a failed campaign falls back; it never lingers
  n.phase = 'prevote';
  n.votes = {};
  n.leaderId = null;
  n.preVoteTerm = n.currentTerm+1;   // the round these replies must belong to
  n.preVotes = { [n.id]: true };
  resetTimeout(W,n);
  emit(W, n.id, retry
    ? `pre-vote for term ${n.preVoteTerm} failed → probes again, term stays ${n.currentTerm}`
    : `timeout → PRE-VOTE probe for term ${n.preVoteTerm} (term not incremented yet)`, 'cand');
  for(const o of W.nodes){
    if(o.id===n.id) continue;
    send(W, n.id, o.id, 'PreVote', {
      term:n.preVoteTerm, candidateId:n.id,
      lastLogIndex:lastIndex(n), lastLogTerm:lastTerm(n),
    });
  }
  // A candidate counts its own vote. For every N>1 this is already false here
  // (1 >= quorum(2) is 2), so it only fires in a one-node group, where there
  // is nobody to reply and the round would otherwise hang forever.
  if(Object.keys(n.preVotes).length >= quorum(nodeCount(W))) startElection(W,n);
}

function startElection(W,n){
  n.state='candidate';
  n.phase=null;
  n.currentTerm++;            // §5.2: increment term...
  n.votedFor=n.id;            // ...vote for self...
  n.votes={ [n.id]:true };
  n.preVotes={}; n.preVoteTerm=0;
  n.leaderId=null;
  resetTimeout(W,n);          // ...reset timer, then fan out RequestVote
  emit(W, n.id, `becomes CANDIDATE for term ${n.currentTerm}, votes for itself`, 'cand');
  for(const o of W.nodes){
    if(o.id===n.id) continue;
    send(W, n.id, o.id, 'RequestVote', {
      term:n.currentTerm, candidateId:n.id,
      lastLogIndex:lastIndex(n), lastLogTerm:lastTerm(n),
    });
  }
  // Same reasoning as in beginPreVote: inert for N>1, and the only way a
  // one-node group ever gets a leader. Without it an unreplicated coordinator
  // could not run at all, and the whole 2PC blocking demo would be a fiction.
  if(Object.keys(n.votes).length >= quorum(nodeCount(W))) becomeLeader(W,n);
}

function becomeLeader(W,n){
  n.state='leader';
  n.leaderId=n.id;
  n.phase=null;
  n.hbTimer=0;
  n.nextIndex={}; n.matchIndex={};
  for(const o of W.nodes){
    if(o.id===n.id) continue;
    n.nextIndex[o.id]  = lastIndex(n)+1;  // optimistic guess, §5.3
    n.matchIndex[o.id] = 0;               // known-replicated: nothing yet
  }
  const votes = Object.keys(n.votes).length;
  emit(W, n.id, `wins term ${n.currentTerm} with ${votes}/${nodeCount(W)} votes → LEADER`, 'leader');
  if(W.cfg.noop){
    n.log.push({ term:n.currentTerm, value:'⊘', noop:true });
    emit(W, n.id, `appends a no-op entry at index ${lastIndex(n)} so older terms can commit`, 'log');
  }
}

/* ------------------------------------------------------------------ *
 *  Replication                                                        *
 * ------------------------------------------------------------------ */
function sendAppendEntries(W, leader, followerId){
  const f = byId(W, followerId);
  if(!f) return;
  if(leader.nextIndex[followerId]===undefined){
    leader.nextIndex[followerId]  = lastIndex(leader)+1;
    leader.matchIndex[followerId] = 0;
  }
  const ni  = Math.max(1, leader.nextIndex[followerId]);
  const pli = ni-1;                       // prevLogIndex
  send(W, leader.id, followerId, 'AppendEntries', {
    term:leader.currentTerm, leaderId:leader.id,
    prevLogIndex: pli,
    prevLogTerm:  termAt(leader, pli),
    entries: leader.log.slice(pli).map(e=>({...e})),
    leaderCommit: leader.commitIndex,
  });
}

function broadcastAppendEntries(W, leader){
  for(const o of W.nodes) if(o.id!==leader.id) sendAppendEntries(W, leader, o.id);
}

/* §5.3/5.4: advance commitIndex to the highest N replicated on a majority,
   but ONLY if log[N] belongs to the leader's own term (Figure 8). */
function advanceCommit(W, leader){
  const N = nodeCount(W);
  for(let k=lastIndex(leader); k>leader.commitIndex; k--){
    let count = 1;  // the leader stores it too
    for(const id in leader.matchIndex) if(leader.matchIndex[id] >= k) count++;
    if(count >= quorum(N)){
      if(termAt(leader,k) === leader.currentTerm){
        const from = leader.commitIndex;
        leader.commitIndex = k;
        emit(W, leader.id, `index ${from+1}..${k} replicated on ${count}/${N} → COMMITTED`, 'commit');
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
function deliver(W, m){
  const to = byId(W, m.to);
  if(!to || to.state==='down') return;
  const p = m.payload;

  /* ---------------- PreVote ---------------- */
  if(m.type==='PreVote'){
    // Grant only if we have NOT heard from a live leader within one minimum
    // election timeout. That is what stops a node the leader cannot reach
    // from disrupting a cluster that is otherwise perfectly healthy.
    // A leader hears a leader every tick — itself — so it never grants.
    const heardRecently = to.state==='leader' || to.lastHeard < ELECTION_MIN;
    const granted = !heardRecently
                 && p.term >= to.currentTerm+1
                 && logIsUpToDate(p.lastLogTerm, p.lastLogIndex, to);
    send(W, to.id, m.from, 'PreVoteReply', { term:to.currentTerm, granted, forTerm:p.term });
    if(!granted){
      const why = heardRecently
          ? (to.state==='leader'    ? 'it is the LEADER and still heartbeating'
           : to.leaderId!==null     ? `it still hears N${to.leaderId}`
           :                          'it heard a leader too recently')
        : p.term < to.currentTerm+1 ? `term ${p.term} is not ahead of ours ${to.currentTerm}`
        : `its log ${p.lastLogTerm}/${p.lastLogIndex} is behind ours ${lastTerm(to)}/${lastIndex(to)}`;
      emit(W, to.id, `denies pre-vote to N${p.candidateId}: ${why}`, 'deny');
    }
    return;
  }
  if(m.type==='PreVoteReply'){
    // A higher term demotes us whatever we are doing — check that first.
    if(p.term > to.currentTerm){ stepDown(W,to,p.term,'in a pre-vote reply'); return; }
    if(to.phase!=='prevote') return;
    if(p.forTerm !== to.preVoteTerm) return;   // reply from an older probe
    if(!p.granted) return;
    to.preVotes[m.from]=true;
    const pre = Object.keys(to.preVotes).length;
    if(pre >= quorum(nodeCount(W))){
      emit(W, to.id, `pre-vote carried ${pre}/${nodeCount(W)} → now safe to increment term`, 'cand');
      startElection(W,to);
    }
    return;
  }

  /* ---------------- RequestVote ---------------- */
  if(m.type==='RequestVote'){
    if(p.term < to.currentTerm){                       // stale candidate
      send(W, to.id, m.from, 'RequestVoteReply', { term:to.currentTerm, granted:false });
      emit(W, to.id, `denies N${p.candidateId}: its term ${p.term} < ours ${to.currentTerm}`, 'deny');
      return;
    }
    if(p.term > to.currentTerm) stepDown(W, to, p.term, `from N${p.candidateId}`);

    const free    = to.votedFor===null || to.votedFor===p.candidateId;
    const fresh   = logIsUpToDate(p.lastLogTerm, p.lastLogIndex, to);
    const granted = free && fresh;
    if(granted){
      to.votedFor = p.candidateId; resetTimeout(W,to);
      // We backed someone else, so abandon any pre-vote probe of our own —
      // otherwise the UI shows a "PRE-VOTE" node that has already voted.
      to.phase=null; to.preVotes={}; to.preVoteTerm=0;
    }

    const why = granted ? '' :
      !free  ? ` (already voted for N${to.votedFor} this term)` :
               ` (its log ${p.lastLogTerm}/${p.lastLogIndex} is behind ours ${lastTerm(to)}/${lastIndex(to)})`;
    emit(W, to.id, `${granted?'GRANTS':'denies'} vote to N${p.candidateId} in term ${to.currentTerm}${why}`,
         granted?'grant':'deny');
    send(W, to.id, m.from, 'RequestVoteReply', { term:to.currentTerm, granted });
    return;
  }

  if(m.type==='RequestVoteReply'){
    // ANY reply carrying a higher term demotes us, whatever we are.
    if(p.term > to.currentTerm){ stepDown(W,to,p.term,'in a vote reply'); return; }
    if(to.state!=='candidate' || p.term!==to.currentTerm) return;
    if(!p.granted) return;
    to.votes[m.from]=true;
    const got = Object.keys(to.votes).length;
    if(got >= quorum(nodeCount(W))) becomeLeader(W,to);
    return;
  }

  /* ---------------- AppendEntries ---------------- */
  if(m.type==='AppendEntries'){
    // 1. Reject anything from a stale leader.
    if(p.term < to.currentTerm){
      send(W, to.id, m.from, 'AppendEntriesReply',
           { term:to.currentTerm, success:false, conflictIndex:1, reqTerm:p.term });
      emit(W, to.id, `rejects N${p.leaderId}: stale term ${p.term} < ${to.currentTerm}`, 'deny');
      return;
    }
    if(p.term > to.currentTerm){ to.currentTerm=p.term; to.votedFor=null; }
    if(to.state!=='follower' && to.state!=='down'){
      emit(W, to.id, `recognises N${p.leaderId} as leader of term ${p.term} → FOLLOWER`, 'down');
    }
    to.state='follower'; to.phase=null; to.leaderId=p.leaderId;
    to.votes={}; to.preVotes={}; to.preVoteTerm=0;
    resetTimeout(W,to);
    to.lastHeard=0;

    // 2. Log consistency check: we must already hold prevLogIndex@prevLogTerm.
    if(p.prevLogIndex > lastIndex(to)){
      // our log is too short — tell the leader where we actually end
      send(W, to.id, m.from, 'AppendEntriesReply',
           { term:to.currentTerm, success:false, conflictIndex:lastIndex(to)+1, reqTerm:p.term });
      emit(W, to.id, `gap: leader assumed index ${p.prevLogIndex}, our log ends at ${lastIndex(to)}`, 'deny');
      return;
    }
    if(p.prevLogIndex > 0 && termAt(to,p.prevLogIndex) !== p.prevLogTerm){
      // term mismatch — skip our whole conflicting term at once (fast backtrack)
      const badTerm = termAt(to, p.prevLogIndex);
      let i = p.prevLogIndex;
      while(i > 1 && termAt(to, i-1) === badTerm) i--;
      send(W, to.id, m.from, 'AppendEntriesReply',
           { term:to.currentTerm, success:false, conflictIndex:i, reqTerm:p.term });
      emit(W, to.id, `mismatch at ${p.prevLogIndex}: ours term ${badTerm}, leader says ${p.prevLogTerm}`, 'deny');
      return;
    }

    // 3+4. Truncate conflicts, then append what is new.
    let appended=0, truncated=0;
    for(let k=0; k<p.entries.length; k++){
      const idx = p.prevLogIndex + k + 1;
      if(idx <= lastIndex(to)){
        if(termAt(to,idx) !== p.entries[k].term){
          truncated = lastIndex(to) - (idx-1);
          to.log.length = idx-1;         // delete this entry and everything after
          to.log.push({...p.entries[k]});
          appended++;
        }
      } else {
        to.log.push({...p.entries[k]});
        appended++;
      }
    }
    if(truncated) emit(W, to.id, `TRUNCATES ${truncated} conflicting entr${truncated>1?'ies':'y'}, adopts leader's`, 'trunc');
    else if(appended) emit(W, to.id, `appends ${appended} entr${appended>1?'ies':'y'} from N${p.leaderId}`, 'log');

    // 5. Follower learns what is committed.
    if(p.leaderCommit > to.commitIndex){
      to.commitIndex = Math.min(p.leaderCommit, p.prevLogIndex + p.entries.length);
    }
    send(W, to.id, m.from, 'AppendEntriesReply', {
      term:to.currentTerm, success:true,
      matchIndex: p.prevLogIndex + p.entries.length, reqTerm:p.term,
    });
    return;
  }

  if(m.type==='AppendEntriesReply'){
    if(p.term > to.currentTerm){ stepDown(W,to,p.term,'in an append reply'); return; }
    if(to.state!=='leader' || p.reqTerm!==to.currentTerm) return;  // ignore stale replies
    if(p.success){
      to.matchIndex[m.from] = Math.max(to.matchIndex[m.from]||0, p.matchIndex);
      to.nextIndex[m.from]  = to.matchIndex[m.from] + 1;
      advanceCommit(W, to);
    } else {
      const before = to.nextIndex[m.from];
      to.nextIndex[m.from] = Math.max(1, p.conflictIndex);
      if(to.nextIndex[m.from] < before){
        emit(W, to.id, `backs off nextIndex[N${m.from}] ${before} → ${to.nextIndex[m.from]}, retries`, 'retry');
      }
      sendAppendEntries(W, to, m.from);   // retry immediately with an older prefix
    }
    return;
  }
}

/* ------------------------------------------------------------------ *
 *  Message fate: partitions, drops, dead targets                      *
 * ------------------------------------------------------------------ */
function blockReason(W,m){
  const a = byId(W,m.from), b = byId(W,m.to);
  if(!b) return 'node gone';
  if(b.state==='down') return 'target crashed';
  if(a && a.state==='down') return 'sender crashed';
  if(a && a.partition !== b.partition) return 'partitioned';
  if(linkBlocks(W, m.from, m.to)) return 'link cut';
  if(m.willDrop) return 'packet lost';
  return null;
}
function dieAt(reason){
  return reason==='partitioned' ? 0.5
       : reason==='link cut' ? 0.5
       : reason==='target crashed' ? 0.9
       // 2PC only: dies at the far ring, so you see it arrive and find nobody home
       : reason==='no leader there' ? 0.9
       : reason==='sender lost leadership' ? 0.2
       : reason==='sender crashed' ? 0.25 : 0.55;
}

/* ------------------------------------------------------------------ *
 *  One simulation tick                                                *
 * ------------------------------------------------------------------ */
function tick(W, dt){
  W.clock += dt;

  for(const n of W.nodes){
    if(n.state==='down') continue;
    n.lastHeard += dt;

    if(n.state==='leader'){
      n.lastHeard = 0;        // a leader hears a leader continuously: itself
      n.hbTimer -= dt;
      if(n.hbTimer<=0){ broadcastAppendEntries(W,n); n.hbTimer = HEARTBEAT; }
    } else {
      n.timeout -= dt;
      if(n.timeout<=0){
        // A pre-vote that failed retries as another PRE-VOTE. It must never
        // fall through into a real election, or the whole mechanism is moot:
        // the second timeout would bump the term and disrupt a live leader.
        if(W.cfg.prevote) beginPreVote(W,n);
        else startElection(W,n);
      }
    }
  }

  for(const m of W.msgs){
    if(m.dead){ m.fade += dt; continue; }
    const reason = blockReason(W,m);
    if(reason && m.progress >= dieAt(reason)){
      m.dead=true; m.fade=0; m.reason=reason; continue;
    }
    m.progress += dt / m.travel;
    if(m.progress>=1){
      if(!reason) deliver(W,m);
      m.delivered=true;
    }
  }
  W.msgs = W.msgs.filter(m => !m.delivered && !(m.dead && m.fade>450));

  // deferred scenario action, e.g. "crash the leader once one exists"
  if(W.armed){
    const done = W.armed(W);
    if(done) W.armed = null;
  }
}
