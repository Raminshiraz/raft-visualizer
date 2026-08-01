/* The live narration for both views. Pure functions of a world.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
function explain(W){
  const N = W.nodes.length, q = quorum(N);
  const live = W.nodes.filter(n=>n.state!=='down');
  const leader = currentLeader(W.nodes);
  const cands  = W.nodes.filter(n=>n.state==='candidate');
  const pre    = W.nodes.filter(n=>n.phase==='prevote');
  const parts  = new Set(W.nodes.map(n=>n.partition));

  if(live.length < q){
    return { ttl:'No quorum — cluster is stuck', tone:'var(--down)',
      bd:`Only <em>${live.length}</em> of <em>${N}</em> nodes are alive, but a majority needs <em>${q}</em>. No candidate can ever collect enough votes, so no leader can exist and nothing new can commit.`,
      why:'Raft trades availability for safety: it would rather stop than risk two leaders. Revive a node to get back above the line.' };
  }

  if(parts.size > 1){
    const sizes = {};
    for(const n of W.nodes) sizes[n.partition] = (sizes[n.partition]||0)+1;
    const big = Math.max(...Object.values(sizes));
    return { ttl:'Network is partitioned', tone:'var(--candidate)',
      bd:`The cluster is split into <em>${parts.size}</em> groups of ${Object.values(sizes).join(' and ')} nodes. A quorum is <em>${q}</em>, so ${big>=q?'the larger side can still elect and commit':'no side is large enough to elect anything'}. Messages crossing the boundary die mid-flight.`,
      why:'Only one side can ever hold a majority, which is exactly why Raft can never produce two committing leaders. The minority may keep raising its term, but it can never win.' };
  }

  const cuts = Object.keys(W.links).length;
  if(cuts && !leader && !cands.length){
    return { ttl:`${cuts} link${cuts>1?'s':''} cut — connectivity is uneven`, tone:'var(--candidate)',
      bd:`The cluster is not partitioned into clean groups; individual wires are down. A node can be reachable by some peers and invisible to others, so different nodes disagree about who is even alive.`,
      why:'Raft only needs a majority that can all hear the leader. Uneven links are worse than a clean split: a node that can send but not receive keeps timing out and disrupting everyone else.' };
  }

  if(pre.length){
    return { ttl:`N${pre[0].id} is running a pre-vote`, tone:'var(--pv)',
      bd:`N${pre[0].id} timed out but has <em>not</em> incremented its term. It first asks whether the others would vote for it. Only if <em>${q}</em> agree does it start a real election.`,
      why:'Pre-vote stops a node that was isolated from returning with an inflated term and knocking a perfectly healthy leader out of office.' };
  }

  if(cands.length>1){
    return { ttl:`Split vote — ${cands.length} candidates at once`, tone:'var(--candidate)',
      bd:`${cands.map(c=>`N${c.id} (${Object.keys(c.votes).length} votes)`).join(' and ')} are both campaigning. Each node gets exactly <em>one</em> vote per term, so if neither reaches <em>${q}</em> the term ends leaderless and everyone tries again.`,
      why:'This is why election timeouts are randomized over a wide range — the next round almost always has a clear first mover.' };
  }

  if(cands.length===1){
    const c=cands[0], got=Object.keys(c.votes).length;
    return { ttl:`N${c.id} is campaigning for term ${c.currentTerm}`, tone:'var(--candidate)',
      bd:`It voted for itself and asked everyone else. It holds <em>${got}</em> of the <em>${q}</em> votes it needs. Voters only say yes if they haven't voted this term <em>and</em> the candidate's log is at least as up-to-date as their own.`,
      why:'That log check is what guarantees a new leader already holds every committed entry — so committed data can never be lost in an election.' };
  }

  if(leader){
    const behind = W.nodes.filter(n=>n.id!==leader.id && n.state!=='down'
                 && (leader.matchIndex[n.id]||0) < lastIndex(leader)).length;
    const pending = lastIndex(leader) - leader.commitIndex;
    return { ttl:`N${leader.id} leads term ${leader.currentTerm}`, tone:'var(--leader)',
      bd:`It heartbeats every ${HEARTBEAT}ms to stop followers timing out. Its log holds <em>${lastIndex(leader)}</em> entries, <em>${leader.commitIndex}</em> committed${pending?`, <em>${pending}</em> still waiting on a majority`:''}.${behind?` <em>${behind}</em> follower(s) are still catching up.`:' All followers are in sync.'}`,
      why:'An entry commits the moment it is stored on a majority — the leader then tells everyone via leaderCommit on the next heartbeat.' };
  }

  return { ttl:'Idle — waiting for a timeout', tone:'var(--follower)',
    bd:`No leader and no candidate. Every node is counting down its own randomized timeout (${ELECTION_MIN}–${ELECTION_MAX}ms — the shrinking ring around each node). The first to hit zero campaigns.`,
    why:'Different deadlines mean one node almost always moves first, which keeps elections short.' };
}

/* Same shape and same priority-cascade style as explain(), so the panel
   markup is shared. Everything interpolated is an internal constant or a
   number — nothing user-supplied reaches the HTML. */
function explainTxn(T){
  const t     = T.txn;
  const g0    = T.groups[0];
  const cl    = groupLeader(g0);
  const shards= shardsOf(T);
  const locked= txnLocked(T);
  const phase = txnPhase(T);
  const secs  = ms => (Math.max(0,ms)/1000).toFixed(1)+'s';

  if(!cl && locked.length){
    const solo = g0.W.nodes.length===1;
    return { ttl:`BLOCKED — ${locked.length} shard${locked.length>1?'s hold':' holds'} locks and nobody can decide`, tone:'var(--down)',
      bd:`${locked.map(g=>g.name).join(' and ')} ${locked.length>1?'have':'has'} voted yes and taken locks. The coordinator group has no leader, so no decision can be recorded and none can be announced. ${solo?'It is a <em>single machine</em>, so there is no second copy of anything.':`Its ${g0.W.nodes.length} nodes cannot reach a quorum.`} The shards are not allowed to guess: a yes vote is a promise that they <em>can</em> commit, never permission to.`,
      why:'This is 2PC\'s famous blocking window. The participants have promised, and only the coordinator may release them. Put the coordinator behind Raft — add nodes to it — and the promise outlives the machine that took it.' };
  }
  if(!cl) return { ttl:'COORD has no leader — 2PC is paused', tone:'var(--candidate)',
    bd:`The coordinator group is electing. Nothing is lost — no shard has promised anything yet — but no transaction can make progress until it has a leader again.`,
    why:'The coordinator is only a Raft group like any other: it stops rather than risk two coordinators deciding differently.' };

  const electing = shards.find(g=>!groupLeader(g));
  if(electing && t) return { ttl:`${electing.name} has no leader — 2PC just waits`, tone:'var(--candidate)',
    bd:`The coordinator cannot get an answer out of ${electing.name} until that group elects someone. It will keep retrying until its prepare deadline runs out, then presume abort.`,
    why:'A shard that cannot elect cannot accept anything either. Stopping is the correct behaviour; it is the coordinator\'s job to give up in bounded time.' };

  if(t){
    const stranded = shards.find(g=>{
      const L = groupLeader(g); if(!L) return false;
      const A = anywhereOn(L,t.id), C = committedOn(L,t.id);
      return A.vote && !C.vote;
    });
    if(stranded) return { ttl:`${stranded.name}'s vote is stranded in an older term`, tone:'var(--candidate)',
      bd:`Its new leader holds the vote record but cannot commit it: Raft only commits entries from the leader's <em>own</em> term. Until something from the current term commits, that vote is invisible and the shard stays silent.`,
      why:'This is Figure 8 with a transaction riding on it. Turn on Leader no-op and the new leader appends one immediately, which carries the old vote over with it.' };

    const unreachable = shards.find(g=> g.partition!==g0.partition || linkBlocks(T,0,g.id));
    if(unreachable && phase==='preparing') return { ttl:`COORD cannot reach ${unreachable.name}`, tone:'var(--down)',
      bd:`Messages to ${unreachable.name} are dying on the wire. The coordinator cannot tell a dead shard from a slow one, and it is not allowed to care — it presumes abort in ${secs(t.deadline-T.clock)}.`,
      why:'Uncertainty and failure are the same thing to a coordinator on a deadline. That is why the safe default is abort, not commit.' };

    const no = shards.find(g=>shardFate(T,g).vote==='no');
    if(no && phase==='preparing') return { ttl:`${no.name} voted NO — the whole transaction must abort`, tone:'var(--down)',
      bd:`One refusal is enough. The coordinator is recording ABORT, and every shard that already locked will release as soon as it hears.`,
      why:'A NO is Raft-committed exactly like a YES. It is a promise too, and a coordinator that crashes has to be able to re-read it.' };

    if(phase==='beginning') return { ttl:'Waiting for Raft to commit the BEGIN record', tone:'var(--follower)',
      bd:`The coordinator has appended BEGIN ${t.id} to its own log and is waiting for a majority of its group to store it. <em>No PREPARE goes out until it does.</em>`,
      why:'If it asked first and recorded second, a crash in between would leave shards holding locks for a transaction no coordinator has ever heard of.' };

    if(phase==='preparing'){
      const got = Object.keys(t.votes).length;
      return { ttl:`Phase 1 — collecting votes (${got}/${shards.length})`, tone:'var(--pv)',
        bd:`Each shard appends its own vote to its own Raft log and waits for its own majority before answering. Presumed abort in <em>${secs(t.deadline-T.clock)}</em>.`,
        why:'That is what makes a vote survivable: the promise is stored by the group, not by the one node that happened to be leader when it was made.' };
    }
    if(phase==='committing'||phase==='aborting'){
      const dec = phase==='committing'?'COMMIT':'ABORT';
      const acks = Object.keys(t.acks).length;
      return { ttl:`Decision ${dec} is durable — telling the shards (${acks}/${shards.length})`, tone:phase==='committing'?'var(--leader)':'var(--down)',
        bd:`The decision is on a majority of the coordinator group. From here it cannot be lost, only re-sent: crash the leader and the next one reads it out of the log and carries on.`,
        why:'This is the line plain 2PC cannot draw. One machine\'s disk write became a committed consensus record, and that is the whole fix.' };
    }
    if(phase==='committed') return { ttl:`${t.id} committed on every shard`, tone:'var(--leader)',
      bd:`Every shard recorded the outcome in its own log and released its locks. The decision, the votes and the outcome all survive any single machine.`,
      why:'Press Begin transaction to run another, or break something first and see how far it gets.' };
    if(phase==='aborted') return { ttl:`${t.id} aborted everywhere — no shard applied it`, tone:'var(--down)',
      bd:`Every shard recorded ABORT and released. Nothing was half-applied anywhere, which is the only promise 2PC actually makes.`,
      why:'Abort is not a failure of the protocol. Reaching the same answer everywhere is the success condition.' };
  }

  return { ttl:'No transaction — press Begin transaction', tone:'var(--follower)',
    bd:`Each bubble is a full Raft group with its own leader, term and log. The coordinator is on top; the shards below it are the participants. Crash nodes, cut the wires between groups, or resize any group first — then start a transaction and watch what it costs.`,
    why:'Every 2PC record here is an ordinary Raft log entry. That is the entire idea: replace the coordinator\'s single disk write with a committed consensus record.' };
}
