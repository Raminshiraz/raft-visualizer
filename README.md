# Raft Consensus Visualizer

An interactive, in-browser visualization of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) — leader election, log replication, and the failure modes that make Raft interesting.

**[▶ Live demo - Use it here!](https://raminshiraz.github.io/raft-visualizer/)**

![A five-node Raft cluster mid-replication: N0 wears the crown as leader of term 3, the four followers sit in sync, and AppendEntries acknowledgements travel back along the wires](demo.png)

No build step, no bundler, no CDN. Open `index.html` and it runs.

---

## Why another Raft visualizer?

There are two well-known ones already, and both are excellent:

- **[raftscope](https://raft.github.io/)** — by Diego Ongaro, Raft's author
- **[The Secret Lives of Data](http://thesecretlivesofdata.com/raft/)** — a guided narrative walkthrough

This one exists because both stop short of the parts that are hardest to understand. Specifically:

| | raftscope | Secret Lives | this |
|---|---|---|---|
| Leader election, heartbeats | ✅ | ✅ | ✅ |
| Crash / restart nodes | ✅ | — | ✅ |
| Clean network partitions | ✅ | ✅ | ✅ |
| **Per-link cuts, including one-way** | — | — | ✅ |
| **Log repair (`nextIndex` backtracking, truncation)** | partial | — | ✅ |
| **Figure 8 — the previous-term commit rule** | — | — | ✅ |
| **PreVote** | — | — | ✅ |
| **Safety invariants machine-checked** | — | — | ✅ |

The point of difference is the **pathological cases**. A clean 3–2 partition is easy to reason about. A node that can send but not receive is not, and it's the one that actually breaks naive implementations.

---

## What you can do

**Break the network in three different ways**

- **Crash a node** — click it. Its `currentTerm`, `votedFor` and `log[]` survive the restart, because in Raft those are persistent state.
- **Partition the cluster** — partition mode splits nodes into network groups, drawn as bubbles labelled `MAJORITY` or `minority — cannot elect`.
- **Cut a single wire** — click the line between any two nodes. Each click cycles it: healthy → fully cut → one-way → the other one-way → healthy.

That third one is the interesting one. A one-way cut models a node that is fully alive and still sending, but invisible to some peers — asymmetric connectivity, which is far more common in practice than a clean split and far nastier.

**Tune the network** — packet loss up to 60%, latency jitter up to ±80%. Dropped messages die visibly mid-flight with the reason attached.

**Toggle real-world extensions** — PreVote and the leader no-op append, so you can watch what breaks without them.

**Step through it** — pause, step 120ms, or jump to the next event. Hover any in-flight message to inspect its full RPC payload.

**Read what's happening** — a live panel narrates the current phase and the reasoning behind it, not just the mechanics.

---

## The scenarios

Seven presets, roughly in order of subtlety:

| Scenario | What it demonstrates |
|---|---|
| **Cold start** | Randomized election timeouts; first to fire usually wins outright |
| **Kill the leader** | Detects the leader, crashes it, survivors elect a replacement in a higher term |
| **Split vote** | Even-sized cluster, simultaneous candidates. Nobody reaches quorum; the term is wasted |
| **Partition 3–2** | Majority keeps working; minority spins its term up and commits nothing |
| **One-way link** | A node that can send but never receive times out forever and repeatedly disrupts a healthy leader. **Turn on PreVote and watch it stop.** |
| **Stale follower repair** | A follower holds four entries from a dead term-2 leader. Watch `nextIndex` walk backwards until the logs match, then the bad tail is truncated |
| **Figure 8** | Every node stores an entry from an old term, replicated to all five — and the leader still refuses to commit it |

Figure 8 is the one worth sitting with. Raft only commits entries from the leader's **own** term; an old entry replicated to every single node still stays uncommitted. Press *Client command* and both commit at once. This rule is the difference between a correct Raft and a subtly broken one.

For the repair scenario, drop the speed to 0.5× and use *Next event* — the backtracking is only a few hundred milliseconds otherwise.

---

## Correctness

The simulation is not animation over a hand-waved state machine. It implements Figure 2 of the paper: `prevLogIndex`/`prevLogTerm` consistency checks, conflicting-entry truncation, `nextIndex[]` backtracking with fast term-skip, `matchIndex[]`, and majority commit gated on the leader's current term.

To keep it honest, `npm test` loads the **same `app.js` the browser runs** and asserts the paper's four safety properties after every simulated 40ms tick, under randomized chaos:

```
$ npm test
Raft safety verification — engine loaded from app.js

PASS  healthy cluster + client writes      leader=y  maxCommit=22
PASS  random crash / restart               leader=y  maxCommit=20
PASS  flapping network partitions          leader=y  maxCommit=24
PASS  35% packet loss                      leader=y  maxCommit=12
PASS  random link cuts (incl. one-way)     leader=y  maxCommit=8
PASS  links + partitions + crashes + loss  leader=n  maxCommit=0
PASS  one-way isolated node cannot win     leader=y  maxCommit=0
PASS  scenario: fresh                      leader=y  maxCommit=7
PASS  scenario: killLeader                 leader=y  maxCommit=6
PASS  scenario: splitVote                  leader=y  maxCommit=8
PASS  scenario: partition                  leader=y  maxCommit=8
PASS  scenario: repair                     leader=y  maxCommit=13
PASS  scenario: asymmetric                 leader=y  maxCommit=3
PASS  scenario: figure8                    leader=y  maxCommit=10
PASS  liveness: 40 cold starts             slow(>15s)=0

ALL INVARIANTS HELD
```

Checked continuously:

- **Election Safety** — at most one leader per term
- **Log Matching** — identical index + term implies identical prefix
- **State Machine Safety** — a committed index never changes value
- **Leader Completeness** — a leader holds every previously committed entry

One row deserves reading carefully. `links + partitions + crashes + loss` reports `leader=n maxCommit=0`: under that much simultaneous damage the cluster never elected anyone and committed nothing for the entire run. That is correct behaviour, not a failure — Raft stops rather than risk split brain. **The suite asserts safety, not liveness**, and makes no claim that progress is possible under arbitrary faults. Liveness is checked separately, and only for a healthy cluster.

---

## Known simplifications

Honest list of where this departs from a production Raft:

- **No log compaction / snapshots.** Logs grow forever.
- **No cluster membership changes.** Adding a node takes effect immediately, with no joint consensus. Do not read the add/remove buttons as a model of reconfiguration.
- **No persistence layer.** "Persistent" state survives a simulated crash because it lives in the same object; there is no fsync to model.
- **`AppendEntries` sends the whole tail** from `nextIndex` onward rather than a bounded batch. Correct, just not what you would ship.
- **Simulated time.** One virtual clock, no real concurrency, so there are no genuine races — message interleaving is driven by latency jitter instead.

---

## Running it

Just open `index.html` — it works straight off the filesystem.

To serve it over HTTP:

```bash
npm run serve      # http://localhost:8080
```

To run the safety suite (no dependencies needed):

```bash
npm test
```

To edit the simulation, change `app.jsx` and rebuild:

```bash
npm install
npm run build      # app.jsx -> app.js
```

`app.js` is committed so a bare checkout and GitHub Pages both work with no build step. React and ReactDOM are vendored in `vendor/` (~140 KB total) so the page has no network dependency at all.

### Layout

```
index.html        page shell, styles, library-load diagnostics
app.jsx           source: Raft engine + React UI
app.js            compiled output (committed)
build.mjs         app.jsx -> app.js
test/verify.mjs   headless safety-invariant suite
vendor/           React 18 UMD builds
```

The Raft engine is plain functions over a mutable world object and has no React dependency — that is what lets the test suite drive it headlessly.

---

## Credit

Based on **[In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf)** by Diego Ongaro and John Ousterhout, USENIX ATC 2014. PreVote and the leader no-op follow Ongaro's PhD thesis, *Consensus: Bridging Theory and Practice* (2014), §9.6 and §6.4.

Any deviation from the paper is a bug — please open an issue.

## License

MIT — see [LICENSE](LICENSE).
