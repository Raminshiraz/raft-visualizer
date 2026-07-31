/* The 2PC SVG stage: group layout, small node glyphs and cross-group RPCs.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ================================================================== *
 *  2PC STAGE                                                          *
 * ================================================================== */

/* Coordinator on top, shards in a row beneath — the way every textbook
   draws 2PC, and it keeps the inter-group wires from crossing. Ring and
   node radii shrink with the member count so that no combination of
   1..3 shards by 1..7 nodes can overlap or run off the canvas. */
function txnLayout(T){
  const VW=700, VH=580;
  const S = Math.max(1, T.groups.length-1);
  const SLOT = Math.min(300, 640/S);
  const out = { VW, VH, groups:[], centres:{} };

  for(const g of T.groups){
    const shard = g.role==='shard';
    const cx = shard ? VW/2 + ((g.id-1)-(S-1)/2)*SLOT : VW/2;
    const cy = shard ? 415 : 140;
    const half = shard ? Math.min(150, SLOT/2-8) : 150;
    const k  = g.W.nodes.length;
    const nr = k===1?26 : k<=3?21 : k<=5?17 : 14;
    const gr = k===1?0  : Math.min(half-nr-6, 22+k*7);
    const pos = {};
    g.W.nodes.forEach((n,j)=>{
      const a = -Math.PI/2 + j*2*Math.PI/k;
      pos[n.id] = { x: cx+gr*Math.cos(a), y: cy+gr*Math.sin(a) };
    });
    out.groups.push({ g, cx, cy, nr, gr, r:gr+nr+7, s:nr/NODE_R, pos, slot:shard?SLOT:VW });
    out.centres[g.id] = { x:cx, y:cy };
  }
  return out;
}

/* Rough advance width of the group label at 11px / weight 800 / letterSpacing 1.
   Only ever used to decide whether the line has to wrap, and erring high is the
   safe direction — it wraps a shade early rather than letting three shards'
   labels run into one another. SVG has no wrapping and measuring properly means
   getComputedTextLength, which needs the node to already be in the document. */
const labelWidth = s => s.length * 7.4;

/* The 2PC records in a group's log, newest `max`, read off its leader (or,
   if it has none, off whichever node knows the most). */
function txnRecords(g, max){
  const src = groupLeader(g)
           || g.W.nodes.slice().sort((a,b)=>b.log.length-a.log.length)[0];
  if(!src) return { src:null, recs:[] };
  const recs = [];
  src.log.forEach((e,i)=>{ if(e.txn) recs.push({ e, index:i+1, committed:(i+1)<=src.commitIndex }); });
  return { src, recs: recs.slice(-max) };
}

const TXN_STYLE = {
  Prepare: { c:'var(--pv)',     l:'P' },
  Prepared:{ c:'var(--grant)',  l:'✓' },
  Decide:  { c:'var(--leader)', l:'D' },
  Ack:     { c:'var(--ae)',     l:'a' },
};
function txnLook(m){
  if(m.type==='Prepared')
    return { c: m.payload.vote==='yes'?'var(--grant)':'var(--deny)', l: m.payload.vote==='yes'?'✓':'✗' };
  if(m.type==='Decide')
    return { c: m.payload.dec==='commit'?'var(--leader)':'var(--down)', l: m.payload.dec==='commit'?'D':'A' };
  return TXN_STYLE[m.type] || { c:'#94a3b8', l:'·' };
}
/* Endpoints are groups, not nodes, so this cannot share payloadText. */
const txnPayload = T => (m)=>{
  const p = m.payload;
  const nm = gid => T.groups[gid] ? T.groups[gid].name : 'group '+gid;
  const ep = (gid,node) => nm(gid) + (node!==null && node!==undefined ? ' · leader N'+node : '');
  const rows = [['type',m.type],['from',ep(m.from,m.fromNode)],['to',ep(m.to,m.toNode)],['txn',p.txn]];
  if(m.type==='Prepared') rows.push(['vote',p.vote],['from log index',p.index]);
  if(m.type==='Decide')   rows.push(['decision',p.dec]);
  if(m.type==='Ack')      rows.push(['applied',p.dec],['at log index',p.index]);
  return rows;
};

/* A node drawn small. The full Stage glyph stacks five rows of text that
   simply do not fit at r=14, so the details move to the hover tooltip. */
function NodeMini({n,x,y,r,onClick,onTip}){
  const col  = nodeColor(n.state);
  const down = n.state==='down';
  const ring = n.state==='follower'||n.state==='candidate';
  const frac = Math.max(0,Math.min(1,n.timeout/ELECTION_MAX));
  const C = 2*Math.PI*(r+4);
  return (
    <g style={{cursor:'pointer'}} onClick={e=>{ e.stopPropagation(); onClick(n.id); }}
       onMouseEnter={e=>onTip({rows:[
         ['node','N'+n.id],
         ['state', n.phase==='prevote'?'pre-vote':n.state],
         ['currentTerm',n.currentTerm],
         ['votedFor', n.votedFor===null?'∅':'N'+n.votedFor],
         ['log length',lastIndex(n)],
         ['commitIndex',n.commitIndex],
       ],x:e.clientX,y:e.clientY})}
       onMouseLeave={()=>onTip(null)}>
      {ring && <circle cx={x} cy={y} r={r+4} fill="none" stroke="#22305c" strokeWidth="2.5"/>}
      {ring && <circle cx={x} cy={y} r={r+4} fill="none"
        stroke={n.phase==='prevote'?'var(--pv)':n.state==='candidate'?'var(--candidate)':'#3d5aa8'}
        strokeWidth="2.5" strokeLinecap="round" strokeDasharray={C}
        strokeDashoffset={C*(1-frac)} transform={`rotate(-90 ${x} ${y})`}/>}
      <circle cx={x} cy={y} r={r} fill={down?'#2a1420':'#0f1a3a'} stroke={col}
        strokeWidth={n.state==='leader'?3:2} strokeDasharray={down?'4 3':'none'}
        filter={n.state==='leader'?'url(#gl)':'none'}/>
      <text x={x} y={y+r*0.3} textAnchor="middle" fontSize={r*0.8} fontWeight="900"
        fill={col} pointerEvents="none">{n.id}</text>
      {n.state==='leader' && <text x={x} y={y-r-3} textAnchor="middle" fontSize={r*0.7} pointerEvents="none">👑</text>}
      {down && <text x={x} y={y-r-3} textAnchor="middle" fontSize={r*0.65} pointerEvents="none">💀</text>}
    </g>
  );
}

function TxnStage({T,onClick,mode,onTip,onLink,sel}){
  const lay = txnLayout(T);
  const ctr = lay.centres;
  const nm  = gid => T.groups[gid] ? T.groups[gid].name : 'group '+gid;

  return (
    <svg className="stage" viewBox={`0 0 ${lay.VW} ${lay.VH}`} onMouseLeave={()=>onTip(null)}>
      <defs>
        <filter id="gl" x="-60%" y="-60%" width="220%" height="220%">
          <feGaussianBlur stdDeviation="4" result="b"/>
          <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
      </defs>

      {/* coordinator <-> shard wires. Click one to cut the groups apart. */}
      {T.groups.slice(1).map(g=>(
        <Wire key={'w'+g.id} a={T.groups[0]} b={g} pa={ctr[0]} pb={ctr[g.id]}
          st={T.links[linkKey(0,g.id)]} onLink={onLink} onTip={onTip} nm={nm}/>
      ))}

      {lay.groups.map(gl=>{
        const g = gl.g;
        const Ld = groupLeader(g);
        const col = Ld ? 'var(--leader)' : 'var(--down)';
        const alive = g.W.nodes.filter(n=>n.state!=='down').length;
        const term  = Math.max(0,...g.W.nodes.map(n=>n.currentTerm));
        const fate  = g.role==='shard' ? shardFate(T,g) : null;
        const { recs } = txnRecords(g, 6);
        const sy = gl.cy + gl.r + 6;
        /* Three shards leave 213px between centres, and "· NO LEADER" or the
           lock alone pushes the label past that — the states most worth reading
           were the ones that collided. Wrap onto a second line instead. */
        const head = `${g.name}${fate&&fate.locked?' 🔒':''} · term ${term}`;
        const tail = `${alive}/${g.W.nodes.length} up${Ld?'':' · NO LEADER'}`;
        const wrap = labelWidth(`${head} · ${tail}`) > gl.slot-12;
        const ly   = gl.cy - gl.r - 7;
        return (
          <g key={g.id}>
            <circle cx={gl.cx} cy={gl.cy} r={gl.r} fill={sel===g.id?'#111c40':'#0c1430'}
              opacity="0.6" stroke={col} strokeWidth={sel===g.id?2.2:1.2} strokeDasharray="7 6"/>
            <text x={gl.cx} y={wrap?ly-12:ly} textAnchor="middle" fontSize="11"
              fontWeight="800" fill={col} letterSpacing="1">
              {wrap ? head : `${head} · ${tail}`}
            </text>
            {wrap &&
              <text x={gl.cx} y={ly} textAnchor="middle" fontSize="11"
                fontWeight="800" fill={col} letterSpacing="1">{tail}</text>}

            {/* Raft traffic inside the group, at the group's own scale */}
            <Msgs msgs={g.W.msgs} pos={gl.pos} onTip={onTip} s={gl.s}/>

            {g.W.nodes.map(n=>(
              <NodeMini key={n.id} n={n} x={gl.pos[n.id].x} y={gl.pos[n.id].y} r={gl.nr}
                onClick={id=>onClick(g.id,id)} onTip={onTip}/>
            ))}
            {mode==='partition' && g.W.nodes.map(n=>(
              <text key={'p'+n.id} x={gl.pos[n.id].x-gl.nr-3} y={gl.pos[n.id].y-gl.nr+2}
                textAnchor="middle" fontSize="10" fontWeight="900" fill="var(--muted)"
                pointerEvents="none">{GROUP_NAMES[n.partition]}</text>
            ))}

            {/* this group's 2PC records: solid once Raft has committed them */}
            {recs.map((r,i)=>{
              const bw=17, x0 = gl.cx - (recs.length*bw)/2 + i*bw;
              const c = termColor(r.e.term);
              return (
                <g key={r.index} style={{cursor:'help'}}
                   onMouseEnter={e=>onTip({rows:[
                     ['group',g.name],['record',r.e.rec],['txn',r.e.txn],
                     ['log index',r.index],['term',r.e.term],
                     ['state', r.committed?'committed — durable':'appended, NOT yet committed'],
                   ],x:e.clientX,y:e.clientY})}>
                  <rect x={x0} y={sy} width="15" height="15" rx="3" fill={c.bg} stroke={c.br}
                    strokeWidth="1.2" strokeDasharray={r.committed?'none':'2 2'}
                    opacity={r.committed?1:0.45}/>
                  <text x={x0+7.5} y={sy+11.2} textAnchor="middle" fontSize="9" fontWeight="900"
                    fill={c.fg} opacity={r.committed?1:0.6} pointerEvents="none">{r.e.value}</text>
                </g>
              );
            })}
          </g>
        );
      })}

      {/* 2PC RPCs, drawn centre to centre so the endpoint does not jump
          when a group re-elects mid-flight */}
      <Msgs msgs={T.msgs} pos={ctr} onTip={onTip} look={txnLook} pay={txnPayload(T)}/>
    </svg>
  );
}
