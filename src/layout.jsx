/* Ring layout for a single cluster, arc-grouped by network partition.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ================================================================== *
 *  LAYOUT — nodes arc-grouped by network partition                    *
 * ================================================================== */
function computeLayout(nodes, W, H){
  const cx=W/2, cy=H/2;
  const R = Math.min(215, 148 + nodes.length*6);
  const pos = {};

  const groups = {};
  for(const n of nodes) (groups[n.partition] = groups[n.partition] || []).push(n);
  const keys = Object.keys(groups).map(Number).sort((a,b)=>a-b);

  if(keys.length<=1){
    nodes.forEach((n,i)=>{
      const a = -Math.PI/2 + i*2*Math.PI/nodes.length;
      pos[n.id] = { x:cx+R*Math.cos(a), y:cy+R*Math.sin(a) };
    });
    return { pos, cx, cy, R, groups:[], split:false };
  }

  // One arc of the ring per group, separated by visible gaps. The band drawn
  // around a group is an annular SECTOR hugging its arc, not a bounding
  // circle: a 3-of-5 group covers ~160° of the ring, so a circle enclosing it
  // is very nearly the whole ring. Two of those overlap, swamp the canvas and
  // push their labels off the top edge.
  const GAP = 0.85;                    // empty radians between neighbouring arcs
  const pad = (NODE_R+8)/R;            // angular half-width of a band's end cap
  const ri  = R - NODE_R - 12;         // band inner radius
  const ro  = R + NODE_R + 12;         // band outer radius
  const usable = 2*Math.PI - keys.length*GAP;
  let angle = -Math.PI/2 - Math.PI/keys.length;
  const shapes = [];
  const at = (rad,ang)=> `${(cx+rad*Math.cos(ang)).toFixed(1)} ${(cy+rad*Math.sin(ang)).toFixed(1)}`;
  for(const k of keys){
    const members = groups[k];
    const span = usable * (members.length/nodes.length);
    let first=0, last=0;
    members.forEach((n,i)=>{
      const a = members.length===1 ? angle+span/2
              : angle + span*(i/(members.length-1));
      if(i===0) first=a;
      last = a;
      pos[n.id] = { x:cx+R*Math.cos(a), y:cy+R*Math.sin(a) };
    });
    const a0=first-pad, a1=last+pad, mid=(a0+a1)/2;
    const big = (a1-a0) > Math.PI ? 1 : 0;
    shapes.push({
      key:k, size:members.length,
      d: `M ${at(ro,a0)} A ${ro} ${ro} 0 ${big} 1 ${at(ro,a1)}`
       + ` L ${at(ri,a1)} A ${ri} ${ri} 0 ${big} 0 ${at(ri,a0)} Z`,
      // label rides just outside the band, clamped so it cannot leave the frame
      lx: Math.max(142, Math.min(W-142, cx+(ro+18)*Math.cos(mid))),
      ly: Math.max(18,  Math.min(H-10,  cy+(ro+18)*Math.sin(mid))),
    });
    angle += span + GAP;
  }
  return { pos, cx, cy, R, groups:shapes, split:true };
}

/* ================================================================== *
 *  EXPLAIN PANEL — narrates whatever the cluster is doing right now   *
 * ================================================================== */
