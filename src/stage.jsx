/* The single-cluster SVG stage, plus the message and wire drawing shared with the 2PC stage.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ================================================================== *
 *  STAGE                                                              *
 * ================================================================== */
const MSG_STYLE = {
  PreVote:        { c:'var(--pv)',  l:'PV'  },
  PreVoteReply:   { c:'var(--pv)',  l:'pv'  },
  RequestVote:    { c:'var(--rv)',  l:'RV'  },
  RequestVoteReply:{c:'var(--rv)',  l:'rv'  },
  AppendEntries:  { c:'var(--ae)',  l:'AE'  },
  AppendEntriesReply:{c:'var(--ae)',l:'ae'  },
};
function msgLook(m){
  const base = MSG_STYLE[m.type] || {c:'#94a3b8',l:'·'};
  if(m.type==='RequestVoteReply'||m.type==='PreVoteReply')
    return { c: m.payload.granted?'var(--grant)':'var(--deny)', l: m.payload.granted?'✓':'✗' };
  if(m.type==='AppendEntriesReply')
    return { c: m.payload.success?'var(--grant)':'var(--deny)', l: m.payload.success?'✓':'✗' };
  if(m.type==='AppendEntries')
    return { c: base.c, l: m.payload.entries.length ? String(m.payload.entries.length) : '♥' };
  return base;
}
function payloadText(m){
  const p=m.payload;
  const rows=[['type',m.type],['from','N'+m.from],['to','N'+m.to]];
  if(m.type==='PreVote'||m.type==='RequestVote')
    rows.push(['term',p.term],['candidateId','N'+p.candidateId],
              ['lastLogIndex',p.lastLogIndex],['lastLogTerm',p.lastLogTerm]);
  else if(m.type==='PreVoteReply'||m.type==='RequestVoteReply')
    rows.push(['term',p.term],['granted',String(p.granted)]);
  else if(m.type==='AppendEntries')
    rows.push(['term',p.term],['leaderId','N'+p.leaderId],
              ['prevLogIndex',p.prevLogIndex],['prevLogTerm',p.prevLogTerm],
              ['entries','['+p.entries.map(e=>`${e.value}@t${e.term}`).join(', ')+']'],
              ['leaderCommit',p.leaderCommit]);
  else if(m.type==='AppendEntriesReply')
    rows.push(['term',p.term],['success',String(p.success)],
              p.success?['matchIndex',p.matchIndex]:['conflictIndex',p.conflictIndex]);
  return rows;
}

/* Messages in flight. Shared by both stages: the 2PC view passes a scale, its
   own look-up and its own payload formatter, and gets the same trail, halo,
   glyph and drop animation for free. */
function Msgs({msgs,pos,onTip,s=1,look=msgLook,pay=payloadText}){
  return msgs.map(m=>{
    const p=pos[m.from], r=pos[m.to];
    if(!p||!r) return null;
    const t=Math.min(m.progress,1);
    const x=p.x+(r.x-p.x)*t, y=p.y+(r.y-p.y)*t;
    const lk=look(m);
    if(m.dead){
      const o=Math.max(0,1-m.fade/450);
      return (
        <g key={m.id} opacity={o}>
          <circle cx={x} cy={y} r={(11+m.fade/28)*s} fill="none" stroke="var(--down)" strokeWidth={2*s}/>
          <text x={x} y={y+4*s} textAnchor="middle" fontSize={13*s} fontWeight="900" fill="var(--down)">✕</text>
          <text x={x} y={y-16*s} textAnchor="middle" fontSize={8.5*s} fontWeight="800" fill="var(--down)">{m.reason}</text>
        </g>
      );
    }
    return (
      <g key={m.id} style={{cursor:'help'}}
         onMouseEnter={e=>onTip({rows:pay(m),x:e.clientX,y:e.clientY})}>
        <line x1={p.x} y1={p.y} x2={x} y2={y} stroke={lk.c} strokeWidth="1.3"
          opacity="0.3" strokeDasharray="3 4"/>
        <circle cx={x} cy={y} r={13*s} fill={lk.c} opacity="0.15"/>
        <circle cx={x} cy={y} r={8.5*s} fill={lk.c} filter="url(#gl)"/>
        <text x={x} y={y+3.4*s} textAnchor="middle" fontSize={9*s} fontWeight="900"
          fill="#08122b" pointerEvents="none">{lk.l}</text>
      </g>
    );
  });
}

/* One wire. Fat transparent overlay makes it easy to hit. Used between nodes
   in the Raft view and between whole groups in the 2PC view — `nm` is how it
   learns to say "SHARD A" instead of "N3". */
function Wire({a,b,pa,pb,st,onLink,onTip,nm=(id=>'N'+id)}){
  const partSplit = a.partition!==b.partition;
  const lo = Math.min(a.id,b.id), hi = Math.max(a.id,b.id);
  const mx=(pa.x+pb.x)/2, my=(pa.y+pb.y)/2;

  let stroke='#1b2650', dash='none', w=1;
  if(partSplit){ stroke='#3b1d2c'; dash='2 8'; }
  if(st==='cut'){ stroke='#ef4444'; dash='4 5'; w=1.6; }
  else if(st){ stroke='#f59e0b'; dash='7 5'; w=1.6; }

  // for a one-way cut, point the arrow along the DEAD direction
  const src = st==='lo2hi' ? lo : hi, dst = st==='lo2hi' ? hi : lo;
  const sp = src===a.id?pa:pb, dp = dst===a.id?pa:pb;
  const ang = Math.atan2(dp.y-sp.y, dp.x-sp.x)*180/Math.PI;

  const label = st==='cut' ? `${nm(lo)} ⇄ ${nm(hi)} fully cut`
              : st ? `${nm(src)} ⇢ ${nm(dst)} blocked (reverse still works)`
              : `${nm(lo)} ⇄ ${nm(hi)} healthy — click to cut`;

  return (
    <g>
      <line x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
        stroke={stroke} strokeWidth={w} strokeDasharray={dash}/>
      <line x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
        stroke="transparent" strokeWidth="14" style={{cursor:'pointer'}}
        onClick={e=>{ e.stopPropagation(); onLink(a.id,b.id); }}
        onMouseEnter={e=>onTip({rows:[['link',label],['click','cycles healthy → cut → one-way → one-way → healthy']],
                                x:e.clientX,y:e.clientY})}
        onMouseLeave={()=>onTip(null)}/>
      {st==='cut' &&
        <g pointerEvents="none">
          <circle cx={mx} cy={my} r="9" fill="#2a1420" stroke="#ef4444" strokeWidth="1.5"/>
          <text x={mx} y={my+3.5} textAnchor="middle" fontSize="10" fontWeight="900" fill="#ef4444">✕</text>
        </g>}
      {st && st!=='cut' &&
        <g pointerEvents="none" transform={`translate(${mx},${my}) rotate(${ang})`}>
          <path d="M -9 -5 L 2 0 L -9 5 Z" fill="#f59e0b"/>
          <line x1="4" y1="-7" x2="12" y2="7" stroke="#ef4444" strokeWidth="2.2"/>
          <line x1="12" y1="-7" x2="4" y2="7" stroke="#ef4444" strokeWidth="2.2"/>
        </g>}
    </g>
  );
}

function Stage({W,onClick,mode,onTip,onLink}){
  const VW=700, VH=580, NR=NODE_R;
  const nodes=W.nodes;
  const lay=computeLayout(nodes,VW,VH);
  const pos=lay.pos;
  const q=quorum(nodes.length);

  return (
    <svg className="stage" viewBox={`0 0 ${VW} ${VH}`} onMouseLeave={()=>onTip(null)}>
      <defs>
        <filter id="gl" x="-60%" y="-60%" width="220%" height="220%">
          <feGaussianBlur stdDeviation="4" result="b"/>
          <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
      </defs>

      {/* partition bubbles */}
      {lay.groups.map(g=>{
        const maj = g.size>=q;
        const col = maj ? 'var(--leader)' : 'var(--down)';
        return (
          <g key={g.key}>
            <path d={g.d} fill={maj?'#0d2b22':'#2a1420'}
              opacity="0.5" stroke={col} strokeWidth="1.5" strokeDasharray="7 6"/>
            <text x={g.lx} y={g.ly} textAnchor="middle" fontSize="11"
              fontWeight="800" fill={col} letterSpacing="1">
              NETWORK {GROUP_NAMES[g.key]} · {g.size} node{g.size>1?'s':''} · {maj?'MAJORITY':'minority — cannot elect'}
            </text>
          </g>
        );
      })}

      {/* wires — click one to cut it */}
      {nodes.map((a,i)=>nodes.slice(i+1).map(b=>(
        <Wire key={a.id+'_'+b.id} a={a} b={b} pa={pos[a.id]} pb={pos[b.id]}
          st={W.links[linkKey(a.id,b.id)]} onLink={onLink} onTip={onTip}/>
      )))}

      {/* messages */}
      <Msgs msgs={W.msgs} pos={pos} onTip={onTip}/>

      {/* nodes */}
      {nodes.map(n=>{
        const {x,y}=pos[n.id];
        const col=nodeColor(n.state);
        const down=n.state==='down';
        const showRing = n.state==='follower'||n.state==='candidate';
        const frac = Math.max(0,Math.min(1,n.timeout/ELECTION_MAX));
        const C = 2*Math.PI*(NR+7);
        const votes = n.state==='candidate' ? Object.keys(n.votes).length
                    : n.phase==='prevote'   ? Object.keys(n.preVotes).length : 0;
        return (
          <g key={n.id} style={{cursor:'pointer'}} onClick={()=>onClick(n.id)}>
            {showRing && <circle cx={x} cy={y} r={NR+7} fill="none" stroke="#22305c" strokeWidth="4"/>}
            {showRing && <circle cx={x} cy={y} r={NR+7} fill="none"
              stroke={n.phase==='prevote'?'var(--pv)':n.state==='candidate'?'var(--candidate)':'#3d5aa8'}
              strokeWidth="4" strokeLinecap="round" strokeDasharray={C}
              strokeDashoffset={C*(1-frac)} transform={`rotate(-90 ${x} ${y})`}/>}
            {n.state==='leader' &&
              <circle cx={x} cy={y} r={NR+8} fill="none" stroke="var(--leader)" strokeWidth="2">
                <animate attributeName="r" values={`${NR+6};${NR+17};${NR+6}`} dur="1.5s" repeatCount="indefinite"/>
                <animate attributeName="opacity" values="0.6;0;0.6" dur="1.5s" repeatCount="indefinite"/>
              </circle>}

            <circle cx={x} cy={y} r={NR} fill={down?'#2a1420':'#0f1a3a'} stroke={col}
              strokeWidth={n.state==='leader'?4:2.5} strokeDasharray={down?'5 4':'none'}
              filter={n.state==='leader'?'url(#gl)':'none'}/>
            <text x={x} y={y-5} textAnchor="middle" fontSize="15" fontWeight="900" fill={col}>N{n.id}</text>
            <text x={x} y={y+9} textAnchor="middle" fontSize="9" fontWeight="700" fill="var(--muted)">term {n.currentTerm}</text>
            <text x={x} y={y+20} textAnchor="middle" fontSize="8" fontWeight="700" fill="var(--dim)">
              log {lastIndex(n)}·c{n.commitIndex}</text>
            <text x={x} y={y+NR+15} textAnchor="middle" fontSize="10" fontWeight="900" fill={col}>
              {n.phase==='prevote'?'PRE-VOTE':n.state.toUpperCase()}</text>

            {votes>0 &&
              <g><rect x={x+NR-8} y={y-NR-8} width="34" height="19" rx="9.5"
                   fill={n.phase==='prevote'?'var(--pv)':'var(--candidate)'}/>
                 <text x={x+NR+9} y={y-NR+5.5} textAnchor="middle" fontSize="10"
                   fontWeight="900" fill="#2a1c00">{votes}/{q}</text></g>}
            {n.state==='leader' && <text x={x} y={y-NR-10} textAnchor="middle" fontSize="15">👑</text>}
            {down && <text x={x} y={y-NR-10} textAnchor="middle" fontSize="14">💀</text>}
            {mode==='partition' &&
              <text x={x-NR-4} y={y-NR+2} textAnchor="middle" fontSize="11" fontWeight="900"
                fill="var(--muted)">{GROUP_NAMES[n.partition]}</text>}
          </g>
        );
      })}
    </svg>
  );
}
