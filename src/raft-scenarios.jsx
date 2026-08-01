/* World construction, cluster membership, and the single-cluster scenarios.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ================================================================== *
 *  WORLD + SCENARIOS                                                  *
 * ================================================================== */
function makeWorld(count, cfg){
  _nid=0;
  const nodes=[];
  for(let i=0;i<count;i++) nodes.push(makeNode(0));
  nodes.forEach(n=>{ n.timeoutInit=n.timeout; });
  return { nodes, msgs:[], events:[], cfg, links:{}, cmdSeq:0, armed:null, clock:0, note:null,
           nextNid:count, nextMid:0, nextEid:0 };
}

/* Membership, as world-level operations so the 2PC view can grow and shrink a
   group without going through App's handlers. Ids come from W.nextNid, which
   never rewinds — reusing an id would resurrect a dead node's matchIndex. */
function addNodeTo(W, term){
  const n = makeNode(term, W.nextNid++);
  n.timeoutInit = n.timeout;
  n.lastHeard = 0;
  W.nodes.push(n);
  return n;
}
function removeNodeFrom(W){
  const n = W.nodes.pop();
  if(!n) return null;
  W.msgs = W.msgs.filter(m=>m.from!==n.id && m.to!==n.id);
  // Drop every trace of it, or a stale vote/matchIndex still counts toward
  // a quorum that is now smaller than it was.
  W.nodes.forEach(o=>{
    delete o.nextIndex[n.id]; delete o.matchIndex[n.id];
    delete o.votes[n.id];     delete o.preVotes[n.id];
  });
  for(const k of Object.keys(W.links)){
    const [a,b] = k.split('-').map(Number);
    if(a===n.id || b===n.id) delete W.links[k];
  }
  return n;
}

const SCENARIOS = {
  fresh: {
    label:'Cold start',
    note:'Five empty nodes. The first to burn through its randomized election timeout campaigns and normally wins outright.',
    build(cfg){ return makeWorld(5,cfg); }
  },

  killLeader: {
    label:'Kill the leader',
    note:'Waits for a leader, then crashes it. Watch the survivors time out, campaign in a higher term, and elect a replacement.',
    build(cfg){
      const W = makeWorld(5,cfg);
      W.armed = (W)=>{
        const L = currentLeader(W.nodes);
        if(!L) return false;
        L.state='down';
        emit(W, L.id, `CRASHED by scenario (was LEADER of term ${L.currentTerm})`, 'crash');
        return true;
      };
      return W;
    }
  },

  splitVote: {
    label:'Split vote',
    note:'Four nodes — an even cluster — with two timers firing together and heavy latency jitter. Votes often split 2-2, nobody reaches 3, and the term is wasted. Randomized timeouts then break the tie.',
    build(cfg){
      const W = makeWorld(4,{...cfg, jitter:0.6});
      W.nodes[0].timeout = 60;
      W.nodes[1].timeout = 60;
      W.nodes[2].timeout = 5200;
      W.nodes[3].timeout = 5600;
      W.nodes.forEach(n=>{ n.timeoutInit = Math.max(n.timeout, ELECTION_MIN); });
      return W;
    }
  },

  partition: {
    label:'Partition 3–2',
    note:'The link splits into a 3-node majority and a 2-node minority. Only the majority can elect. The minority spins its term up forever and commits nothing. Press Heal to reconnect.',
    build(cfg){
      const W = makeWorld(5,cfg);
      W.nodes[3].partition = 1;
      W.nodes[4].partition = 1;
      return W;
    }
  },

  repair: {
    label:'Stale follower repair',
    note:"N1 holds four entries from a dead term-2 leader that never committed. N0 has the authoritative log. Watch nextIndex walk backwards until the logs match, then N1's bad tail is truncated and overwritten.",
    build(cfg){
      const W = makeWorld(5,{...cfg, noop:true});
      const good = [{term:1,value:'A'},{term:1,value:'B'},{term:3,value:'C'},{term:3,value:'D'}];
      for(const n of W.nodes){
        n.currentTerm = 3;
        n.log = good.map(e=>({...e}));
        n.commitIndex = 2;
        n.timeout = 5500;
      }
      // N1 diverges from index 3 onward with entries from a stale term 2
      W.nodes[1].log = [
        {term:1,value:'A'},{term:1,value:'B'},
        {term:2,value:'X'},{term:2,value:'Y'},{term:2,value:'Z'},{term:2,value:'W'},
      ];
      W.nodes[1].commitIndex = 2;
      W.nodes[0].timeout = 80;      // N0 campaigns first and must win (term 3 log)
      W.nodes.forEach(n=>{ n.timeoutInit = Math.max(n.timeout, ELECTION_MIN); });
      return W;
    }
  },

  asymmetric: {
    label:'One-way link (disruptive node)',
    note:'N4 can still SEND to everyone, but receives nothing — every inbound link to it is cut one-way. It never hears a heartbeat, so it times out forever, campaigns in ever-higher terms, and its RequestVotes keep knocking the healthy leader down. Turn on PreVote to watch the disruption stop.',
    build(cfg){
      const W = makeWorld(5,cfg);
      // block every j -> N4 direction, leaving N4 -> j intact
      for(let j=0;j<4;j++) W.links[linkKey(j,4)] = 'lo2hi';
      return W;
    }
  },

  figure8: {
    label:'Figure 8 — old-term entry',
    note:"Every node already stores entry 2 from term 2, yet nothing is committed. The new leader will replicate it to all five and still refuse to commit it, because Raft only commits entries from its OWN term. Press \"Client command\" and both commit at once.",
    build(cfg){
      const W = makeWorld(5,{...cfg, noop:false});
      for(const n of W.nodes){
        n.currentTerm = 3;
        n.log = [{term:1,value:'A'},{term:2,value:'B'}];
        n.commitIndex = 0;
        n.timeout = 5500;
      }
      W.nodes[0].timeout = 80;
      W.nodes.forEach(n=>{ n.timeoutInit = Math.max(n.timeout, ELECTION_MIN); });
      return W;
    }
  },

  figure8Lost: {
    label:'Figure 8 — the overwrite',
    note:'Proof that "stored on a majority" is NOT enough. N0 holds X from term 2; N2 and N3 never received it; N4 alone holds Y from term 3 and is DOWN, so the one better log in the cluster votes on nothing. N0 wins term 4 against those empty logs, spreads X to four of five nodes — and still refuses to commit it. Then N0 crashes, N4 returns, and X is overwritten everywhere. Anyone who had called X committed just lost it. Now switch ON Leader no-op and load this again: X commits, and N4 can never win.',
    build(cfg){
      // Paper Figure 8(b), frozen the instant S5 crashes. cfg.noop is passed
      // through untouched — the whole point is to run this both ways.
      const W = makeWorld(5,cfg);
      for(const n of W.nodes){ n.currentTerm=3; n.commitIndex=0; n.timeout=5500; }
      W.nodes[0].log = [{term:2,value:'X'}];   // X got to N0 and N1 and stopped
      W.nodes[1].log = [{term:2,value:'X'}];
      W.nodes[4].log = [{term:3,value:'Y'}];   // Y never left N4
      W.nodes[4].state = 'down';
      W.nodes[0].timeout = 80;                 // N0 campaigns first
      W.nodes.forEach(n=>{ n.timeoutInit = Math.max(n.timeout, ELECTION_MIN); });

      // The moment X reaches a majority, crash N0 and let N4 back in. N4 has
      // to campaign twice: the others already spent their term-4 vote on N0,
      // so its first bid is refused and only the term-5 bid can win.
      W.armed = (W)=>{
        const L = W.nodes.find(n=>n.state==='leader' && n.id===0);
        if(!L) return false;
        let on = 1;
        for(const id in L.matchIndex) if(L.matchIndex[id] >= 1) on++;
        if(on < quorum(nodeCount(W))) return false;
        L.state='down';
        emit(W, L.id, `CRASHED by scenario — X sits on ${on}/${nodeCount(W)} nodes and is STILL uncommitted`, 'crash');
        for(const o of W.nodes){                 // keep the field clear for N4
          if(o.id===0 || o.id===4) continue;
          o.timeout = 11000; o.timeoutInit = 11000;
        }
        const back = byId(W,4);
        back.state='follower'; back.leaderId=null; back.lastHeard=ELECTION_MAX;
        back.timeout=300; back.timeoutInit=ELECTION_MIN;
        emit(W, back.id, `restarts holding Y from term 3 — the only node that ever saw it`, 'sys');
        return true;
      };
      return W;
    }
  },
};
