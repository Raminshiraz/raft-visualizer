/* The 2PC scenarios.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
/* ------------------------------------------------------------------ *
 *  2PC scenarios                                                       *
 *                                                                      *
 *  Kept in their own object rather than in SCENARIOS, because the       *
 *  safety suite iterates SCENARIOS expecting every entry to build a     *
 *  single Raft world. These build a transaction world instead.          *
 * ------------------------------------------------------------------ */

/* A scripted scenario is a list of [predicate, action] stages, run in
   order, one at a time, polled from tickTxn exactly like W.armed. */
function script(stages){
  let i = 0;
  return (T)=>{
    while(i < stages.length){
      const [when, act] = stages[i];
      if(!when(T)) return false;
      act(T); i++;
    }
    return true;
  };
}
const hasCoordLeader = T => !!groupLeader(T.groups[0]);
const votedYes  = gid => T => shardFate(T, T.groups[gid]).vote === 'yes';
const allVoted  = T => shardsOf(T).every(g=>shardFate(T,g).vote);
const asked     = gid => T => T.msgs.some(m=>m.type==='Prepare' && m.to===gid);
const crashLeaderOf = g => { const L = groupLeader(g); if(L) L.state = 'down'; return L; };

const TXN_SCENARIOS = {
  happy: {
    label:'Commit path',
    note:'Nothing broken. Watch the ORDER, because it is the whole difference between this and textbook 2PC: the coordinator appends BEGIN and waits for a majority of its own group before a single PREPARE leaves the building, and each shard appends its YES and waits for its own majority before answering. Every arrow you see is preceded by a commit.',
    build(cfg){
      const T = makeTxnWorld(3,[3,3],cfg);
      T.armed = script([[hasCoordLeader, T=>beginTxn(T)]]);
      return T;
    }
  },

  voteNo: {
    label:'A shard votes NO',
    note:'Shard B is set to answer NO. Its refusal is Raft-appended and committed exactly like a yes would be — a NO is a promise too, and a coordinator that crashes must be able to re-read it. One refusal is enough: the coordinator records ABORT, and Shard A, which had already voted yes and taken locks, releases them.',
    build(cfg){
      const T = makeTxnWorld(3,[3,3],cfg);
      T.groups[2].willVote = 'no';
      T.armed = script([[hasCoordLeader, T=>beginTxn(T)]]);
      return T;
    }
  },

  shardLeaderDies: {
    label:'Shard leader dies after PREPARED',
    note:'Shard A\'s leader is crashed the instant its YES becomes durable. It was the only node that ever spoke to the coordinator, and it is gone. Watch the survivors elect a replacement, and watch that replacement answer PREPARED for a transaction it has never heard of, straight out of the replicated log. This is the entire reason to put a participant behind Raft. Now turn OFF Leader no-op and load it again: the new leader holds the vote but cannot commit an entry from the old term, so it goes quiet and the transaction times out into an abort. Figure 8, with a transaction riding on it.',
    build(cfg){
      const T = makeTxnWorld(3,[3,3],cfg);
      T.armed = script([
        [hasCoordLeader, T=>beginTxn(T)],
        [votedYes(1), T=>{
          const L = crashLeaderOf(T.groups[1]);
          if(L) emitT(T, 1, `leader N${L.id} CRASHED by scenario — it is the only node that has spoken to the coordinator`, 'block');
        }],
      ]);
      return T;
    }
  },

  coordDies: {
    label:'Coordinator dies — one machine',
    note:'The coordinator is a single machine, the way textbook 2PC draws it. It crashes after both shards have voted yes and before it records a decision. Both shards are now PREPARED, both hold locks, and neither is allowed to guess — a yes vote is a promise to be able to commit, not permission to. Nothing times out, and asking each other would not help: neither shard knows any more than the other. This is the blocking problem, and no amount of retrying fixes it. It lasts exactly as long as this one machine is down — click it to restart it, and watch it find BEGIN in its log with no decision, ask again, and finish. Then load Coordinator dies — replicated: the same script, and nobody has to wait for one particular machine.',
    build(cfg){
      const T = makeTxnWorld(1,[3,3],cfg);
      T.armed = script([
        [hasCoordLeader, T=>beginTxn(T)],
        [allVoted, T=>{
          T.groups[0].W.nodes[0].state = 'down';
          emitT(T, 0, `the coordinator CRASHED holding both votes and no decision — there is no second copy`, 'block');
        }],
      ]);
      return T;
    }
  },

  coordDiesFT: {
    label:'Coordinator dies — replicated',
    note:'The same script, except the coordinator is a three-node Raft group and the crash takes only its leader. The survivors elect a new one. The in-memory vote tally is gone with the old leader — watch it re-send PREPARE and collect the votes again — but the BEGIN record is still on a majority, and the shards answer from their own committed logs. The transaction finishes. The machine that started it never came back.',
    build(cfg){
      const T = makeTxnWorld(3,[3,3],cfg);
      T.armed = script([
        [hasCoordLeader, T=>beginTxn(T)],
        [allVoted, T=>{
          const L = crashLeaderOf(T.groups[0]);
          if(L) emitT(T, 0, `COORD leader N${L.id} CRASHED holding both votes and no decision — but it was never the only copy`, 'block');
        }],
      ]);
      return T;
    }
  },

  cutShard: {
    label:'Coordinator cut off from a shard',
    note:'The wire between the coordinator and Shard B is cut while PREPARE is still in flight. Shard A votes yes and locks. Shard B\'s answer dies in mid-air. The coordinator cannot tell a dead shard from a slow one and is not allowed to care: when the prepare deadline runs out it records ABORT, and A releases. Press Heal network afterwards and watch B learn the decision late.',
    build(cfg){
      const T = makeTxnWorld(3,[3,3],cfg);
      T.armed = script([
        [hasCoordLeader, T=>beginTxn(T)],
        [asked(2), T=>{
          T.links[linkKey(0,2)] = 'cut';
          emitT(T, 0, `the wire to SHARD B is CUT — its answer, whatever it was, is not coming`, 'block');
        }],
      ]);
      return T;
    }
  },

  shardNoQuorum: {
    label:'Shard loses quorum',
    note:'Two of Shard B\'s three nodes are already down, so it can never elect a leader and can never append anything. PREPARE arrives at a group with nobody home and dies on the ring. Raft is behaving correctly here — it stops rather than risk split brain — and 2PC on top of it does the only safe thing left: presumed abort. Restart a node in Shard B before the deadline and the transaction lives.',
    build(cfg){
      const T = makeTxnWorld(3,[3,3],cfg);
      T.groups[2].W.nodes[0].state = 'down';
      T.groups[2].W.nodes[1].state = 'down';
      T.armed = script([[hasCoordLeader, T=>beginTxn(T)]]);
      return T;
    }
  },

  slowShard: {
    label:'Prepare timeout — presumed abort',
    note:'Nothing is crashed and nothing is cut. Every group is just sitting behind 45% packet loss. The votes may or may not arrive before the deadline — run it a few times. The outcome is indistinguishable from the crash and from the partition, which is exactly the point: presumed abort does not care why the answer was late, because it cannot find out.',
    build(cfg){
      const T = makeTxnWorld(3,[3,3],{...cfg, dropRate:0.45});
      T.armed = script([[hasCoordLeader, T=>beginTxn(T)]]);
      return T;
    }
  },
};
