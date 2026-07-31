/* Simulation constants, the Raft node model and the shared colour scales.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
const {useState,useRef,useEffect,useCallback} = React;

/* ================================================================== *
 *  CONSTANTS  (all times are simulated milliseconds)                  *
 * ================================================================== */
const ELECTION_MIN = 3000;   // randomized election timeout, low end
const ELECTION_MAX = 6000;   // ...high end. Spread prevents split votes.
const HEARTBEAT    = 1000;   // leader -> followers, must be << ELECTION_MIN
const TRAVEL       = 700;    // base one-way network latency
const MAX_STEP     = 100;    // largest dt fed to step() in one go
const NODE_R       = 33;     // drawn radius of a node, needed by the layout

const rand  = (a,b)=> a + Math.random()*(b-a);
const quorum = n => Math.floor(n/2)+1;
const GROUP_NAMES = ['A','B','C','D'];

/* --- per-link connectivity -----------------------------------------
   W.links maps "lo-hi" (node ids, lo<hi) to one of:
     'cut'  both directions dead
     'lo2hi' only lo→hi dead   (hi can still be heard by lo)
     'hi2lo' only hi→lo dead
   Missing key = healthy link. One-way states model the nasty real-world
   case where a node can send but never receives.                     */
const linkKey = (a,b)=> a<b ? a+'-'+b : b+'-'+a;
const LINK_CYCLE = { undefined:'cut', cut:'lo2hi', lo2hi:'hi2lo', hi2lo:undefined };

function linkBlocks(W, from, to){
  const st = W.links[linkKey(from,to)];
  if(!st) return false;
  if(st==='cut') return true;
  const fromIsLo = from < to;
  return st==='lo2hi' ? fromIsLo : !fromIsLo;
}

/* ================================================================== *
 *  NODE MODEL                                                         *
 *  Mirrors Raft paper Figure 2 state exactly.                         *
 * ================================================================== */
/* Node ids are allocated per world (W.nextNid), not globally, because the 2PC
   view keeps several worlds alive at once — one Raft group each. Message and
   event ids live on the world for the same reason: they are React keys, and
   two worlds minting id 3 in the same frame is a duplicate-key bug. */
let _nid=0;

function makeNode(term, id){
  return {
    id: id===undefined ? _nid++ : id,

    /* --- PERSISTENT state (survives a crash, on stable storage) --- */
    currentTerm: term|0,
    votedFor: null,
    log: [],              // 1-indexed logically: log[i-1] is index i

    /* --- VOLATILE state on all servers --- */
    state: 'follower',    // follower | candidate | leader | down
    commitIndex: 0,
    leaderId: null,

    /* --- VOLATILE state on leaders (reset on election) --- */
    nextIndex: {},        // follower id -> next log index to send
    matchIndex: {},       // follower id -> highest index known replicated

    /* --- simulation-only bookkeeping --- */
    votes: {},            // id -> true, for the current election
    preVotes: {},         // id -> true, for the pre-vote round
    preVoteTerm: 0,       // term this pre-vote round is probing for
    phase: null,          // 'prevote' while running a pre-vote round
    timeout: rand(ELECTION_MIN, ELECTION_MAX),
    timeoutInit: 0,
    hbTimer: 0,
    lastHeard: 0,         // ms since a valid AppendEntries arrived
    partition: 0,         // network group; equal ids can talk
  };
}

/* log helpers — everything below speaks 1-based log indices */
const lastIndex = n => n.log.length;
const termAt    = (n,i) => i<=0 ? 0 : (n.log[i-1] ? n.log[i-1].term : 0);
const lastTerm  = n => termAt(n, n.log.length);

/* Raft §5.4.1 — is `cand` at least as up-to-date as `voter`? */
function logIsUpToDate(candLastTerm, candLastIndex, voter){
  const vt = lastTerm(voter);
  if(candLastTerm !== vt) return candLastTerm > vt;
  return candLastIndex >= lastIndex(voter);
}

/* distinct colour per term so divergent logs are visually obvious */
function termColor(t){
  const h = (t*137.5+200) % 360;
  return { bg:`hsl(${h} 62% 26%)`, br:`hsl(${h} 70% 52%)`, fg:`hsl(${h} 90% 84%)` };
}
function nodeColor(s){
  return s==='leader'?'var(--leader)':s==='candidate'?'var(--candidate)'
       : s==='down'?'var(--down)':'var(--follower)';
}
