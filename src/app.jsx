/* The root component: state, the animation loop, controls and page layout.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ================================================================== *
 *  APP                                                                *
 * ================================================================== */
const DEFAULT_CFG = { dropRate:0, jitter:0.12, prevote:false, noop:false };

function App(){
  const world = useRef(null);
  if(world.current===null) world.current = SCENARIOS.fresh.build({...DEFAULT_CFG});

  /* The 2PC world is a peer of the Raft world, not a child of it: several
     live Raft groups plus the transaction driving them. Built lazily so the
     Raft view costs nothing extra until you switch. */
  const txnWorld = useRef(null);
  if(txnWorld.current===null)
    txnWorld.current = makeTxnWorld(3,[3,3],{...DEFAULT_CFG, noop:true});

  const ui = useRef({ running:true, speed:1 });
  const [running,setRunning] = useState(true);
  const [speed,setSpeed]     = useState(1);
  const [mode,setMode]       = useState('crash');   // crash | partition
  const [cfg,setCfgState]    = useState({...DEFAULT_CFG});
  const [scenario,setScenario] = useState('fresh');
  const [tip,setTip]         = useState(null);
  const [view,setView]       = useState('raft');    // raft | txn
  const [txnScenario,setTxnScenario] = useState(null);
  const [sel,setSel]         = useState(0);         // inspected group, 2PC view
  const [,force]             = useState(0);

  /* main loop — only the visible world is ticked. The other freezes where you
     left it instead of burning simulated time and flooding its event log. */
  useEffect(()=>{
    let raf, last=performance.now();
    const loop=(now)=>{
      const dt = now-last; last=now;
      if(ui.current.running){
        let sd = Math.min(dt,200)*ui.current.speed;
        while(sd>0){
          const s=Math.min(sd,MAX_STEP);
          if(ui.current.view==='txn') tickTxn(txnWorld.current,s);
          else                        tick(world.current,s);
          sd-=s;
        }
      }
      force(f=>f+1);
      raf=requestAnimationFrame(loop);
    };
    raf=requestAnimationFrame(loop);
    return ()=>cancelAnimationFrame(raf);
  },[]);

  const W    = world.current;
  const T    = txnWorld.current;
  const isTxn= view==='txn';
  ui.current.view = view;

  /* ---- controls, shared by both views ---- */
  const toggleRun=()=>{ ui.current.running=!ui.current.running; setRunning(ui.current.running); };
  const pause=()=>{ ui.current.running=false; setRunning(false); };
  const advance=(ms)=>{ if(isTxn) tickTxn(T,ms); else tick(W,ms); };
  const events=()=> isTxn ? T.events : W.events;

  const stepOnce=()=>{ pause(); advance(120); force(f=>f+1); };
  const stepEvent=()=>{
    pause();
    const before=events().length;
    let spent=0;
    while(spent<8000 && events().length===before){ advance(40); spent+=40; }
    force(f=>f+1);
  };

  /* Every group in the 2PC world holds the same cfg object by reference, so
     one patch reaches all of them. */
  const setCfg=(patch)=>{
    const c = isTxn ? T.cfg : W.cfg;
    Object.assign(c, patch);
    setCfgState({...c});
  };

  const switchView=(v)=>{
    setView(v);
    ui.current.view = v;
    setCfgState({...(v==='txn' ? T.cfg : W.cfg)});
    ui.current.running=true; setRunning(true);
  };

  const loadScenario=(key)=>{
    world.current = SCENARIOS[key].build({...W.cfg});
    world.current.note = SCENARIOS[key].note;
    setScenario(key);
    setCfgState({...world.current.cfg});
    ui.current.running=true; setRunning(true);
    emit(world.current,-1,`scenario loaded: ${SCENARIOS[key].label}`,'sys');
  };

  const clickNode=(id)=>{
    const n=byId(W,id); if(!n) return;
    if(mode==='partition'){
      const groups = Math.max(2, new Set(W.nodes.map(x=>x.partition)).size);
      n.partition = (n.partition+1) % Math.min(groups+1,3);
      emit(W,n.id,`moved to network group ${GROUP_NAMES[n.partition]}`,'sys');
    } else {
      if(n.state==='down'){
        // currentTerm / votedFor / log are persistent — they survive the crash
        n.state='follower'; n.votes={}; n.preVotes={}; n.preVoteTerm=0; n.phase=null;
        n.leaderId=null; n.lastHeard=ELECTION_MAX; resetTimeout(W,n);
        emit(W,n.id,`restarts as FOLLOWER — keeps term ${n.currentTerm}, votedFor ${n.votedFor===null?'∅':'N'+n.votedFor}, ${lastIndex(n)} log entries`,'sys');
      } else {
        const was=n.state;
        n.state='down'; n.votes={}; n.preVotes={}; n.preVoteTerm=0; n.phase=null; n.leaderId=null;
        emit(W,n.id,`CRASHED (was ${was.toUpperCase()})`,'crash');
      }
    }
  };

  const clickLink=(a,b)=>{
    const k=linkKey(a,b);
    const next=LINK_CYCLE[W.links[k]];
    if(next===undefined) delete W.links[k]; else W.links[k]=next;
    const lo=Math.min(a,b), hi=Math.max(a,b);
    emit(W,-1, next===undefined ? `link N${lo}–N${hi} restored`
      : next==='cut' ? `link N${lo}–N${hi} CUT in both directions`
      : next==='lo2hi' ? `link N${lo}→N${hi} cut one-way (N${hi}→N${lo} still works)`
      : `link N${hi}→N${lo} cut one-way (N${lo}→N${hi} still works)`, 'sys');
  };

  const healAll=()=>{
    W.nodes.forEach(n=>n.partition=0);
    W.links={};
    emit(W,-1,'network healed — all partitions merged and every link restored','sys');
  };

  const addNode=()=>{
    if(W.nodes.length>=9) return;
    const t=Math.max(0,...W.nodes.map(n=>n.currentTerm));
    const n=addNodeTo(W,t);
    emit(W,n.id,`joins the cluster — quorum is now ${quorum(W.nodes.length)}/${W.nodes.length}`,'sys');
  };
  const removeNode=()=>{
    if(W.nodes.length<=3) return;
    const n=removeNodeFrom(W);
    emit(W,n.id,`removed — quorum is now ${quorum(W.nodes.length)}/${W.nodes.length}`,'sys');
  };

  const clientCmd=()=>{
    const L=W.nodes.find(n=>n.state==='leader');
    if(!L){ emit(W,-1,'client command REJECTED — there is no leader to accept it','deny'); return; }
    const v=String.fromCharCode(65+(W.cmdSeq++%26));
    L.log.push({ term:L.currentTerm, value:v });
    emit(W,L.id,`accepts client command "${v}" at index ${lastIndex(L)} (term ${L.currentTerm}) — replicating`,'log');
    broadcastAppendEntries(W,L);
    L.hbTimer=HEARTBEAT;
  };

  /* ---- 2PC controls ---- *
   * Adding and removing nodes MUTATES the group's live world, so its Raft
   * log — and therefore its 2PC records — survive the resize. Only the
   * controls that have to rebuild a group discard the transaction, and they
   * say so in the event log rather than silently refusing.               */
  const txnClickNode=(gid,id)=>{
    const g=T.groups[gid]; if(!g) return;
    const n=byId(g.W,id);  if(!n) return;
    if(mode==='partition'){
      const parts = Math.max(2, new Set(g.W.nodes.map(x=>x.partition)).size);
      n.partition = (n.partition+1) % Math.min(parts+1,3);
      emit(g.W,n.id,`moved to network group ${GROUP_NAMES[n.partition]}`,'sys');
    } else if(n.state==='down'){
      n.state='follower'; n.votes={}; n.preVotes={}; n.preVoteTerm=0; n.phase=null;
      n.leaderId=null; n.lastHeard=ELECTION_MAX; resetTimeout(g.W,n);
      emit(g.W,n.id,`restarts as FOLLOWER — keeps term ${n.currentTerm} and ${lastIndex(n)} log entries`,'sys');
      emitT(T,gid,`N${n.id} restarts in ${g.name}`,'sys');
    } else {
      const was=n.state;
      n.state='down'; n.votes={}; n.preVotes={}; n.preVoteTerm=0; n.phase=null; n.leaderId=null;
      emit(g.W,n.id,`CRASHED (was ${was.toUpperCase()})`,'crash');
      emitT(T,gid,`N${n.id} CRASHED in ${g.name}${was==='leader'?' — it was the leader':''}`,'crash');
    }
    setSel(gid);
  };

  const txnClickLink=(a,b)=>{
    const k=linkKey(a,b);
    const next=LINK_CYCLE[T.links[k]];
    if(next===undefined) delete T.links[k]; else T.links[k]=next;
    const lo=Math.min(a,b), hi=Math.max(a,b);
    const nm=gid=>T.groups[gid]?T.groups[gid].name:'group '+gid;
    emitT(T,-1, next===undefined ? `${nm(lo)}–${nm(hi)} link restored`
      : next==='cut' ? `${nm(lo)}–${nm(hi)} CUT in both directions`
      : next==='lo2hi' ? `${nm(lo)}→${nm(hi)} cut one-way (the reverse still works)`
      : `${nm(hi)}→${nm(lo)} cut one-way (the reverse still works)`, 'sys');
  };

  const txnHeal=()=>{
    T.links={};
    T.groups.forEach(g=>{ g.partition=0; g.W.links={}; g.W.nodes.forEach(n=>n.partition=0); });
    emitT(T,-1,'network healed — every group reconnected and every internal link restored','sys');
  };

  const groupAdd=()=>{
    const g=T.groups[sel]; if(!g || g.W.nodes.length>=7) return;
    const t=Math.max(0,...g.W.nodes.map(n=>n.currentTerm));
    const n=addNodeTo(g.W,t);
    emit(g.W,n.id,`joins — quorum is now ${quorum(g.W.nodes.length)}/${g.W.nodes.length}`,'sys');
    emitT(T,sel,`${g.name} grows to ${g.W.nodes.length} nodes — quorum ${quorum(g.W.nodes.length)}`+
      (g.role==='coord'&&g.W.nodes.length===2?', and it is no longer a single point of failure':''),'sys');
  };
  const groupRemove=()=>{
    const g=T.groups[sel]; if(!g || g.W.nodes.length<=1) return;
    const n=removeNodeFrom(g.W);
    emitT(T,sel,`N${n.id} removed from ${g.name} — quorum is now ${quorum(g.W.nodes.length)}/${g.W.nodes.length}`+
      (g.role==='coord'&&g.W.nodes.length===1?', and it is a single point of failure again':''),'sys');
  };

  /* Rebuilds groups, so any live transaction goes with them. Everything that is
     not a consequence of the new topology is carried across, because silently
     undoing the user's damage is worse than refusing to: cuts between groups
     that still exist survive, and any that pointed at a group that does not
     are dropped and reported. */
  const rebuild=(coordSize, shardSizes, why)=>{
    resetTxn(T, why);
    const keep = T.groups.map(g=>g.willVote);
    const fresh = makeTxnWorld(coordSize, shardSizes, T.cfg);
    fresh.events = T.events; fresh.nextEid = T.nextEid; fresh.clock = T.clock;
    fresh.txnSeq = T.txnSeq;
    fresh.groups.forEach((g,i)=>{ if(keep[i]) g.willVote = keep[i]; });

    let dropped = 0;
    for(const k of Object.keys(T.links)){
      const [a,b] = k.split('-').map(Number);
      if(a < fresh.groups.length && b < fresh.groups.length) fresh.links[k] = T.links[k];
      else dropped++;
    }
    if(dropped) emitT(fresh,-1,`${dropped} cut link(s) went with the group(s) that were removed`,'sys');
    txnWorld.current = fresh;
    setSel(s=>Math.min(s, fresh.groups.length-1));
    setTxnScenario(null);
    emitT(fresh,-1,`topology is now 1 coordinator ×${coordSize} + ${shardSizes.length} shard(s) ×${shardSizes.join('/')}`,'sys');
  };
  const setShards=(d)=>{
    const cur = T.groups.slice(1).map(g=>g.W.nodes.length);
    const next = d>0 ? [...cur,3] : cur.slice(0,-1);
    if(next.length<1 || next.length>3) return;
    rebuild(T.groups[0].W.nodes.length, next, 'the shard count changed');
  };
  const toggleFT=()=>{
    const solo = T.groups[0].W.nodes.length===1;
    rebuild(solo?3:1, T.groups.slice(1).map(g=>g.W.nodes.length),
      solo?'the coordinator was replicated':'the coordinator was reduced to one machine');
  };

  const cycleVote=(gid)=>{
    const g=T.groups[gid]; if(!g) return;
    g.willVote = g.willVote==='yes' ? 'no' : 'yes';
    emitT(T,gid,`${g.name} will now answer ${g.willVote.toUpperCase()} when it is asked to prepare`,'sys');
  };

  const beginTransaction=()=>{ beginTxn(T); };

  const loadTxnScenario=(key)=>{
    txnWorld.current = TXN_SCENARIOS[key].build({...T.cfg});
    txnWorld.current.note = TXN_SCENARIOS[key].note;
    setTxnScenario(key);
    setSel(0);
    setCfgState({...txnWorld.current.cfg});
    ui.current.running=true; setRunning(true);
    emitT(txnWorld.current,-1,`scenario loaded: ${TXN_SCENARIOS[key].label}`,'sys');
  };

  const restart=()=>{
    if(!isTxn) return loadScenario(scenario);
    if(txnScenario) return loadTxnScenario(txnScenario);
    rebuild(T.groups[0].W.nodes.length, T.groups.slice(1).map(g=>g.W.nodes.length), 'the view was reset');
  };

  /* ---- derived ---- */
  const maxTerm = Math.max(0,...W.nodes.map(n=>n.currentTerm));
  const leader  = W.nodes.find(n=>n.state==='leader');
  const live    = W.nodes.filter(n=>n.state!=='down').length;
  const ex      = isTxn ? explainTxn(T) : explain(W);
  const q       = quorum(W.nodes.length);

  const selG    = T.groups[Math.min(sel,T.groups.length-1)];
  const selLead = selG && groupLeader(selG);
  const txnPh   = isTxn ? txnPhase(T) : null;
  const nLocks  = isTxn ? txnLocked(T).length : 0;
  const note    = isTxn ? T.note : W.note;

  /* A message tooltip must not outlive its message. Compared by identity, not
     by id: the ids are per world, so a group's message 7 and the cross-group
     message 7 would keep each other's tooltips alive. Derived rather than
     cleared through setTip, which would be a state write during render. */
  const tipLive = !tip || !tip.msg || (isTxn
    ? T.msgs.includes(tip.msg) || T.groups.some(g=>g.W.msgs.includes(tip.msg))
    : W.msgs.includes(tip.msg));

  return (
    <div className="app">
      <div className="head">
        {/* h2, not h1: the page's single <h1> is the static one in
            index.html, which is what crawlers read on the first pass.
            Still a heading, so it keeps its place in the outline. */}
        <h2>{isTxn?'Two-Phase Commit over Raft':'Raft Consensus Visualizer'}</h2>
        <span className="sub">{isTxn
          ? 'atomic commit across replicated groups · the coordinator is a Raft group too'
          : 'leader election · log replication · partitions'}</span>
        <div className="stat">
          {isTxn ? <>
            <span className="chip">groups <b>{T.groups.length}</b></span>
            <span className="chip">txn <b>{T.txn?T.txn.id:'—'}</b></span>
            <span className="chip">phase <b style={{color:PHASE_TONE[txnPh]}}>{txnPh}</b></span>
            <span className="chip">locks held <b style={{color:nLocks?'var(--down)':'var(--leader)'}}>{nLocks}</b></span>
          </> : <>
            <span className="chip">term <b>{maxTerm}</b></span>
            <span className="chip">quorum <b>{q}/{W.nodes.length}</b></span>
            <span className="chip">alive <b style={{color:live>=q?'var(--leader)':'var(--down)'}}>{live}</b></span>
            <span className="chip">leader <b style={{color:leader?'var(--leader)':'var(--down)'}}>{leader?'N'+leader.id:'none'}</b></span>
          </>}
        </div>
      </div>

      <div className="bar">
        <span className="barlab">View</span>
        <button className={'btn '+(!isTxn?'on':'')} onClick={()=>switchView('raft')}
          title="one Raft cluster: elections, replication, partitions">Raft cluster</button>
        <button className={'btn '+(isTxn?'on':'')} onClick={()=>switchView('txn')}
          title="a distributed transaction across several Raft groups">2PC over Raft</button>
        <span className="hint" style={{margin:0,flex:'1 1 260px',minWidth:180}}>
          {isTxn ? 'Every bubble is a full Raft group. The other view is paused where you left it.'
                 : 'The 2PC view runs this same engine, several clusters at a time.'}
        </span>
      </div>

      <div className="bar">
        <span className="barlab">Run</span>
        <button className="btn primary" onClick={toggleRun}>{running?'❙❙\uFE0E  Pause':'▶\uFE0E  Play'}</button>
        <button className="btn" onClick={stepOnce}>Step 120 ms</button>
        <button className="btn" onClick={stepEvent}>Next event</button>
        <div className="sl"><span>speed</span>
          <input type="range" min="0.25" max="4" step="0.25" value={speed}
            onChange={e=>{const v=parseFloat(e.target.value); ui.current.speed=v; setSpeed(v);}}/>
          <b>{speed}×</b></div>
        <div className="sep"/>
        <button className="btn warn" onClick={restart}>↺ Restart {isTxn&&!txnScenario?'view':'scenario'}</button>
      </div>

      {!isTxn &&
        <div className="bar">
          <span className="barlab">Cluster</span>
          <button className={'btn '+(mode==='crash'?'on':'')} onClick={()=>setMode('crash')}>Crash mode</button>
          <button className={'btn '+(mode==='partition'?'on':'')} onClick={()=>setMode('partition')}>Partition mode</button>
          <button className="btn" onClick={healAll}>Heal network</button>
          <div className="sep"/>
          <Stepper label="Nodes" value={W.nodes.length} onAdd={addNode} onSub={removeNode}
            addDisabled={W.nodes.length>=9} subDisabled={W.nodes.length<=3}
            addTitle="add a node — quorum grows with the cluster"/>
          <div className="sep"/>
          <button className="btn primary" onClick={clientCmd}>Client command</button>
        </div>}

      {isTxn && <>
        <div className="bar">
          <span className="barlab">Groups</span>
          {T.groups.map(g=>(
            <button key={g.id} className={'btn '+(sel===g.id?'on':'')} onClick={()=>setSel(g.id)}
              title="pick the group the panels on the right inspect">
              {g.name} ×{g.W.nodes.length}
            </button>
          ))}
          <div className="sep"/>
          <Stepper label="Nodes" value={selG?selG.W.nodes.length:0} onAdd={groupAdd} onSub={groupRemove}
            addDisabled={!selG||selG.W.nodes.length>=7} subDisabled={!selG||selG.W.nodes.length<=1}
            addTitle="grows the selected group without rebuilding it — its log survives"/>
          <div className="sep"/>
          <Stepper label="Shards" value={T.groups.length-1} onAdd={()=>setShards(1)} onSub={()=>setShards(-1)}
            addDisabled={T.groups.length>=4} subDisabled={T.groups.length<=2}
            addTitle="adding or removing a shard rebuilds it, which clears any running transaction"/>
        </div>

        <div className="bar">
          <span className="barlab">Transaction</span>
          <button className="btn primary" onClick={beginTransaction}
            disabled={!!T.txn && !txnDone(T)}>Begin transaction</button>
          <button className={'btn '+(T.groups[0].W.nodes.length>1?'on':'warn')} onClick={toggleFT}
            title="a one-node coordinator is textbook 2PC; three nodes is the fix. Rebuilds the group, so it clears any running transaction">
            {T.groups[0].W.nodes.length>1?'✓ ':''}Replicated coordinator</button>
          <button className={'btn '+(mode==='crash'?'on':'')} onClick={()=>setMode('crash')}>Crash mode</button>
          <button className={'btn '+(mode==='partition'?'on':'')} onClick={()=>setMode('partition')}>Partition mode</button>
          <button className="btn" onClick={txnHeal}>Heal network</button>
        </div>
      </>}

      <div className="bar">
        <span className="barlab">Network</span>
        <div className="sl"><span>packet loss</span>
          <input type="range" min="0" max="0.6" step="0.05" value={cfg.dropRate}
            onChange={e=>setCfg({dropRate:parseFloat(e.target.value)})}/>
          <b>{Math.round(cfg.dropRate*100)}%</b></div>
        <div className="sl"><span>latency jitter</span>
          <input type="range" min="0" max="0.8" step="0.05" value={cfg.jitter}
            onChange={e=>setCfg({jitter:parseFloat(e.target.value)})}/>
          <b>±{Math.round(cfg.jitter*100)}%</b></div>
        <div className="sep"/>
        <button className={'btn '+(cfg.prevote?'on':'')} onClick={()=>setCfg({prevote:!cfg.prevote})}
          title="Probe for votes before incrementing the term">
          {cfg.prevote?'✓ ':''}PreVote</button>
        <button className={'btn '+(cfg.noop?'on':'')} onClick={()=>setCfg({noop:!cfg.noop})}
          title="New leader appends a no-op so older-term entries can commit">
          {cfg.noop?'✓ ':''}Leader no-op</button>
      </div>

      <div className="bar">
        <span className="barlab">Scenario</span>
        {isTxn
          ? Object.keys(TXN_SCENARIOS).map(k=>(
              <button key={k} className={'btn '+(txnScenario===k?'on':'')}
                onClick={()=>loadTxnScenario(k)}>{TXN_SCENARIOS[k].label}</button>))
          : Object.keys(SCENARIOS).map(k=>(
              <button key={k} className={'btn '+(scenario===k?'on':'')}
                onClick={()=>loadScenario(k)}>{SCENARIOS[k].label}</button>))}
      </div>

      <div className="grid">
        <div>
          <div className="card">
            <h2>{isTxn?'Groups':'Cluster'}
              <span style={{marginLeft:'auto',fontWeight:600,letterSpacing:0,
                textTransform:'none',color:'var(--dim)'}}>
                {mode==='partition'?'click a node to move it between network groups'
                                   :'click a node to crash or restart it'}
              </span>
            </h2>
            <div style={{padding:6}}>
              {isTxn
                ? <TxnStage T={T} onClick={txnClickNode} mode={mode} onTip={setTip}
                    onLink={txnClickLink} onSelect={setSel} sel={sel}/>
                : <Stage W={W} onClick={clickNode} mode={mode} onTip={setTip} onLink={clickLink}/>}
            </div>
            <div className="legend">
              <L c="var(--follower)" t="Follower"/>
              <L c="var(--candidate)" t="Candidate"/>
              <L c="var(--leader)" t="Leader"/>
              <L c="var(--down)" t="Crashed"/>
              {isTxn ? <>
                <L c="var(--pv)" t="Prepare"/>
                <L c="var(--grant)" t="Prepared ✓ yes"/>
                <L c="var(--deny)" t="Prepared ✗ no"/>
                <L c="var(--leader)" t="Decide D commit"/>
                <L c="var(--down)" t="Decide A abort"/>
                <L c="var(--ae)" t="Ack / Raft traffic inside a group"/>
                <L c="#ef4444" t="✕ groups cut apart"/>
              </> : <>
                <L c="var(--pv)" t="PreVote"/>
                <L c="var(--rv)" t="RequestVote"/>
                <L c="var(--grant)" t="granted ✓"/>
                <L c="var(--deny)" t="denied ✗"/>
                <L c="var(--ae)" t="AppendEntries / ♥ heartbeat"/>
                <L c="#ef4444" t="✕ link cut both ways"/>
                <L c="#f59e0b" t="➤ link cut one way"/>
              </>}
            </div>
            <div className="hint">{isTxn
              ? <><b style={{color:'var(--muted)'}}>The row of boxes under each group is its 2PC record</b> in that group's own Raft log — solid once a majority stores it, faded dashed until then. Nothing is ever said out loud before its box goes solid, which is the whole difference between this and textbook 2PC. Click the wire between two groups to cut them apart, click a group's ring to point the panels at it, click any node to crash it, and hover anything for detail.</>
              : <><b style={{color:'var(--muted)'}}>Click a wire</b> between two nodes to cycle it: healthy → fully cut → one-way → the other one-way → healthy. A one-way cut lets a node send but never receive — that is how a node becomes unreachable to only <i>some</i> peers without being offline. Ring around each node = its election-timeout countdown on a shared scale. Hover any flying message for its RPC payload.</>}
            </div>
          </div>

          <div className="card">
            <h2>Replicated logs{isTxn?' — '+(selG?selG.name:''):''} <span style={{marginLeft:'auto',fontWeight:600,
              letterSpacing:0,textTransform:'none',color:'var(--dim)'}}>
              solid = committed · faded dashed = not yet committed · colour = term</span></h2>
            <div style={{padding:'3px 0'}}>
              {(isTxn ? (selG?selG.W.nodes:[]) : W.nodes).map(n=>
                <LogRow key={n.id} node={n} leader={isTxn?selLead:leader}/>)}
            </div>
          </div>
        </div>

        <div>
          <div className="card">
            <h2>What is happening</h2>
            <div className="explain">
              <div className="ttl" style={{color:ex.tone}}>{ex.ttl}</div>
              <div className="bd" dangerouslySetInnerHTML={{__html:ex.bd}}/>
              <div className="why">{ex.why}</div>
              {note && <div className="why" style={{color:'var(--muted)'}}>
                <b style={{color:'var(--txt)'}}>Scenario: </b>{note}</div>}
            </div>
          </div>

          {isTxn &&
            <div className="card">
              <h2>Transaction</h2>
              <TxnPanel T={T} onVote={cycleVote}/>
            </div>}

          <div className="card">
            <h2>{isTxn?'Nodes — '+(selG?selG.name:''):'Nodes'}</h2>
            <div className="insp">
              {(isTxn ? (selG?selG.W.nodes:[]) : W.nodes).map(n=>
                <NodeRow key={n.id} node={n} W={isTxn?selG.W:W} mode={mode}
                  onClick={id=>isTxn?txnClickNode(selG.id,id):clickNode(id)}/>)}
            </div>
          </div>

          <div className="card">
            <h2>{isTxn?'2PC event log':'Event log'}</h2>
            <div className="log">
              {isTxn
                ? T.events.map(e=>{
                    const g = e.nodeId>=0 ? T.groups[e.nodeId] : null;
                    return (
                      <div className="row" key={e.id}>
                        <span className="t">{e.t}s</span>
                        <span className="n" style={{color:g?(groupLeader(g)?'var(--leader)':'var(--down)'):'var(--dim)'}}>
                          {g?(g.role==='coord'?'COORD':SHARD_NAMES[g.id-1]):'—'}</span>
                        <span style={{color:EVENT_COLOR[e.kind]||'var(--txt)'}}>{e.text}</span>
                      </div>
                    );
                  })
                : W.events.map(e=>{
                    const n = e.nodeId<0?null:byId(W,e.nodeId);
                    return (
                      <div className="row" key={e.id}>
                        <span className="t">{e.t}s</span>
                        <span className="n" style={{color:n?nodeColor(n.state):'var(--dim)'}}>
                          {e.nodeId<0?'—':'N'+e.nodeId}</span>
                        <span style={{color:EVENT_COLOR[e.kind]||'var(--txt)'}}>{e.text}</span>
                      </div>
                    );
                  })}
            </div>
          </div>

          {isTxn && selG &&
            <div className="card">
              <h2>Inside {selG.name}</h2>
              <div className="log" style={{height:180}}>
                {selG.W.events.map(e=>{
                  const n = e.nodeId<0?null:byId(selG.W,e.nodeId);
                  return (
                    <div className="row" key={e.id}>
                      <span className="t">{e.t}s</span>
                      <span className="n" style={{color:n?nodeColor(n.state):'var(--dim)'}}>
                        {e.nodeId<0?'—':'N'+e.nodeId}</span>
                      <span style={{color:EVENT_COLOR[e.kind]||'var(--txt)'}}>{e.text}</span>
                    </div>
                  );
                })}
              </div>
            </div>}
        </div>
      </div>

      {tip && tipLive && <Tip tip={tip}/>}
    </div>
  );
}

const EVENT_COLOR = {
  grant:'#7ee2b8', deny:'#f8a5a5', leader:'#6ee7b7', cand:'#fcd34d',
  commit:'#34d399', trunc:'#fca5a5', retry:'#c4b5fd', crash:'#ef7d7d',
  log:'#93c5fd', down:'#cbd5e1', sys:'#8aa0d0',
  /* 2PC */ prep:'#f0abfc', decide:'#34d399', block:'#ef7d7d',
};
const L = ({c,t})=>(<span className="it"><span className="dot" style={{background:c}}/>{t}</span>);

/* One control instead of two loose buttons: the count sits between + and -,
   so what the buttons act on is named where they are, and a topology that is
   already at its limit greys out the side that cannot move. */
const Stepper = ({label,value,onAdd,onSub,addDisabled,subDisabled,addTitle})=>(
  <span className="step">
    <button className="btn" onClick={onAdd} disabled={addDisabled}
      title={addTitle||('add one — '+label.toLowerCase())} aria-label={'add one '+label}>+</button>
    <span className="stepv">{label} <b>{value}</b></span>
    <button className="btn" onClick={onSub} disabled={subDisabled}
      title={'remove one — '+label.toLowerCase()} aria-label={'remove one '+label}>−</button>
  </span>
);
