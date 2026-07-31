/* The side panels: node inspector, replicated logs, transaction state, tooltip.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ================================================================== *
 *  SIDE PANELS                                                        *
 * ================================================================== */
function NodeRow({node,W,onClick,mode}){
  const col=nodeColor(node.state);
  const frac=Math.max(0,Math.min(1,node.timeout/ELECTION_MAX));
  const leader=W.nodes.find(n=>n.state==='leader');
  const isFollowerOfLeader = leader && leader.id!==node.id && node.state!=='down';
  return (
    <div className="nrow">
      <div className="top">
        <b style={{color:col}}>N{node.id}</b>
        <span className="pill" style={{background:col,color:'#07102a'}}>
          {node.phase==='prevote'?'pre-vote':node.state}</span>
        {mode==='partition' && <span className="pill" style={{background:'#243466',color:'var(--txt)'}}>
          net {GROUP_NAMES[node.partition]}</span>}
        <button className="mini" onClick={()=>onClick(node.id)}>
          {mode==='partition'?'move net':node.state==='down'?'restart':'crash'}</button>
      </div>
      <div className="meta">
        <span>term {node.currentTerm}</span>
        <span>votedFor {node.votedFor===null?'∅':'N'+node.votedFor}</span>
        <span>log {lastIndex(node)}</span>
        <span>commit {node.commitIndex}</span>
      </div>
      {node.state==='leader' &&
        <div className="meta" style={{color:'var(--muted)'}}>
          {W.nodes.filter(o=>o.id!==node.id).map(o=>(
            <span key={o.id} title="nextIndex / matchIndex">
              N{o.id}:<b style={{color:'#c4b5fd'}}>{node.nextIndex[o.id]??'?'}</b>/
              <b style={{color:'#7ee2b8'}}>{node.matchIndex[o.id]??0}</b>
            </span>
          ))}
        </div>}
      {(node.state==='follower'||node.state==='candidate') &&
        <div className="tobar"><i style={{width:(frac*100)+'%',
          background:node.phase==='prevote'?'var(--pv)':node.state==='candidate'?'var(--candidate)':'#3d5aa8'}}/></div>}
    </div>
  );
}

const PHASE_TONE = {
  idle:'#5a6c99', beginning:'var(--follower)', preparing:'var(--pv)',
  committing:'var(--leader)', aborting:'var(--down)',
  committed:'var(--leader)', aborted:'var(--down)',
};

/* Everything here is derived from the groups' committed logs each frame —
   there is no transaction state object to fall out of sync with them. */
function TxnPanel({T,onVote}){
  const t      = T.txn;
  const phase  = txnPhase(T);
  const c      = coordFate(T);
  const shards = shardsOf(T);
  const left   = t ? Math.max(0, t.deadline - T.clock) : 0;
  const coord  = T.groups[0];

  return (
    <div className="insp">
      <div className="nrow">
        <div className="top">
          <b style={{color:'var(--txt)'}}>{t ? t.id : 'no transaction'}</b>
          <span className="pill" style={{background:PHASE_TONE[phase],color:'#08122b'}}>{phase}</span>
          <span style={{marginLeft:'auto',fontSize:11,
            color: coord.W.nodes.length===1 ? 'var(--down)' : 'var(--muted)'}}>
            {coord.W.nodes.length===1 ? 'coordinator unreplicated' : `coordinator ×${coord.W.nodes.length}`}
          </span>
        </div>
        <div className="meta">
          <span>BEGIN <b style={{color:c.begin?'var(--leader)':'var(--dim)'}}>
            {c.begin?'durable':'—'}</b></span>
          <span>decision <b style={{color:c.decision?(c.decision==='commit'?'var(--leader)':'var(--down)'):'var(--dim)'}}>
            {c.decision?c.decision.toUpperCase():'not recorded'}</b></span>
        </div>
        {t && phase==='preparing' &&
          <div className="tobar" title={`presumed abort in ${(left/1000).toFixed(1)}s`}>
            <i style={{width:(100*left/PREPARE_TIMEOUT)+'%',background:'var(--pv)'}}/></div>}
      </div>

      {shards.map(g=>{
        const f  = shardFate(T,g);
        const Ld = groupLeader(g);
        const st = f.applied ? (f.applied==='commit'?'applied ✔':'applied ✘')
                 : f.vote==='yes' ? 'PREPARED'
                 : f.vote==='no'  ? 'refused' : 'not asked';
        const tone = f.applied ? (f.applied==='commit'?'var(--leader)':'var(--down)')
                   : f.vote==='yes' ? 'var(--candidate)'
                   : f.vote==='no'  ? 'var(--down)' : '#5a6c99';
        return (
          <div className="nrow" key={g.id}>
            <div className="top">
              <b style={{color:Ld?'var(--txt)':'var(--down)'}}>{g.name}</b>
              <span className="pill" style={{background:tone,color:'#08122b'}}>{st}</span>
              {f.locked && <span style={{color:'var(--down)',fontWeight:800,fontSize:11}}>🔒 locked</span>}
              <button className="mini" style={{marginLeft:'auto'}} onClick={()=>onVote(g.id)}
                title="what this shard answers when it is asked to prepare">
                will vote {g.willVote}</button>
            </div>
            <div className="meta">
              <span>leader {Ld?'N'+Ld.id:'none'}</span>
              <span>{g.W.nodes.length} node{g.W.nodes.length>1?'s':''}</span>
              <span>quorum {quorum(g.W.nodes.length)}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function LogRow({node,leader}){
  const col=nodeColor(node.state);
  const nx = leader && leader.id!==node.id ? leader.nextIndex[node.id] : null;
  return (
    <div className="lrRow" style={{opacity:node.state==='down'?0.45:1}}>
      <div className="lrName">
        <span className="dot" style={{background:col,width:9,height:9}}/>N{node.id}
        {node.state==='leader'&&' 👑'}{node.state==='down'&&' 💀'}
      </div>
      <div className="entries">
        {node.log.length===0 && <span className="empty">empty log</span>}
        {node.log.map((e,i)=>{
          const c=termColor(e.term);
          const committed = (i+1)<=node.commitIndex;
          return (
            <div key={i} className={'ent '+(committed?'':'pending')}
              title={`index ${i+1} · term ${e.term} · ${committed?'committed':'not committed'}`}
              style={{background:c.bg,borderColor:c.br,color:c.fg}}>
              {e.value}<small>{i+1}·t{e.term}</small>
            </div>
          );
        })}
        {nx!=null && <span style={{fontSize:10.5,color:'#c4b5fd',fontFamily:'Consolas',
          marginLeft:6,whiteSpace:'nowrap'}}>next→{nx}</span>}
      </div>
    </div>
  );
}

function Tip({tip}){
  const style={ left:Math.min(tip.x+16, window.innerWidth-310), top:tip.y+16 };
  return (
    <div className="tip" style={style}>
      {tip.rows.map(([k,v],i)=>(
        <div key={i}><span className="k">{k}: </span>{String(v)}</div>
      ))}
    </div>
  );
}
