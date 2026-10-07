"""Block dissemination delay, inferred from tipscan's GetTip observations.

Every observation is (peer, time t, the peer's tip). For each main-chain
block B created at slot time s_B, a peer whose tip is on the main chain
*has* B at time t iff B is an ancestor-or-self of its tip, i.e.
height(B) <= height(tip). Pooling every (observation, block) pair with
age = t - s_B gives the fraction of peers holding a block as a function of
its age: the network-wide dissemination CDF. Delays are measured to the
point where the peer has *applied* the block (it is on its canonical chain),
from the block's slot start, so they include the proposer's own build time.

Peers not on the main chain at an observation are excluded (stale forks
such as branch B never receive main blocks). "Live" additionally drops
observations whose tip is more than --stuck-after seconds old: those peers
are stalled rather than slow.

Usage: dissemination.py <tipscan.sqlite> [--out dissemination.png]
       [--max-age 120] [--stuck-after 300] [--genesis-ms 1790758800000]
       [--mac-until-scan 57]
"""
import argparse
import sqlite3

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ap = argparse.ArgumentParser()
ap.add_argument('db')
ap.add_argument('--out', default='dissemination.png')
ap.add_argument('--max-age', type=int, default=120)
ap.add_argument('--stuck-after', type=int, default=300)
ap.add_argument('--genesis-ms', type=int, default=1790758800000)
ap.add_argument('--mac-until-scan', type=int, default=57,
                help='scans up to this id were taken from the Mac (clock ~0.28 s slow)')
a = ap.parse_args()

con = sqlite3.connect(a.db)
blocks = pd.read_sql('SELECT id, parent, slot, height FROM blocks', con).set_index('id')
obs = pd.read_sql('SELECT scan, peer, t, tip, slot, height FROM obs WHERE tip IS NOT NULL', con)

# Main chain = ancestors of the highest block.
parent = blocks.parent.to_dict()
best = blocks.height.idxmax()
main = set()
c = best
while c in parent:
    main.add(c)
    c = parent[c]
main_blocks = blocks[blocks.index.isin(main)].sort_values('height')
mb_h = main_blocks.height.to_numpy()
mb_ms = a.genesis_ms + main_blocks.slot.to_numpy() * 1000

obs = obs[obs.tip.isin(main)].copy()
obs['tip_h'] = obs.tip.map(blocks.height)
obs['tip_age_s'] = (obs.t - (a.genesis_ms + obs.slot * 1000)) / 1000
obs['source'] = np.where(obs.scan <= a.mac_until_scan, 'mac', 'german')


def curve(o, bin_ms=1000, max_age=None):
    """Fraction of (observation, block) pairs where the peer has the block,
    by block age in bins of `bin_ms`."""
    max_age = max_age or a.max_age
    nb = max_age * 1000 // bin_ms + 1
    has = np.zeros(nb)
    tot = np.zeros(nb)
    t = o.t.to_numpy()
    th = o.tip_h.to_numpy()
    # Blocks created in [t - max_age, t] for each observation.
    lo = np.searchsorted(mb_ms, t - max_age * 1000, side='left')
    hi = np.searchsorted(mb_ms, t, side='right')
    for i in range(len(t)):
        if hi[i] <= lo[i]:
            continue
        ages = ((t[i] - mb_ms[lo[i]:hi[i]]) // bin_ms).astype(int)
        h = mb_h[lo[i]:hi[i]] <= th[i]
        np.add.at(tot, ages, 1)
        np.add.at(has, ages, h)
    with np.errstate(invalid='ignore'):
        return has / tot, tot


FINE_MS, FINE_MAX = 250, 20


def reach(frac, p, bin_s=1.0):
    """Age (s, upper edge of the first bin) where the fraction reaches p."""
    idx = np.where(frac >= p)[0]
    return round((idx[0] + 1) * bin_s, 2) if len(idx) else None


def summarize(name, o):
    frac, tot = curve(o)
    fine, ftot = curve(o, FINE_MS, FINE_MAX)
    peers = o.peer.nunique()
    r = {p: reach(fine, p, FINE_MS / 1000) for p in (0.5, 0.75, 0.9, 0.95, 0.99)}
    print(f"{name:26} {len(o):6d} obs  {peers:4d} peers  {int(tot.sum()):8d} pairs  "
          + '  '.join(f"t{int(p*100)}={'%5.2fs' % v if v is not None else '   —  '}" for p, v in r.items())
          + f"  at {a.max_age}s: {np.nanmean(frac[-10:]):.4f}")
    return frac, tot, fine, ftot


live = obs[obs.tip_age_s <= a.stuck_after]
print(f"main chain: {len(main)} blocks; observations on main: {len(obs)} "
      f"(live: {len(live)}, dropped stuck >{a.stuck_after}s: {len(obs) - len(live)})\n")
res = {
    'all main-chain peers': summarize('all main-chain peers', obs),
    'live peers': summarize('live peers', live),
    'live, german scans': summarize('live, german scans', live[live.source == 'german']),
    'live, mac scans': summarize('live, mac scans', live[live.source == 'mac']),
}

fig, (ax, ax2) = plt.subplots(1, 2, figsize=(15, 5.5), gridspec_kw={'width_ratios': [3, 2]})
series = [('live peers', dict(color='#2a78d6', lw=2.4)),
          ('all main-chain peers', dict(color='#9a9aa0', lw=1.5, ls='--')),
          ('live, german scans', dict(color='#1f9e5a', lw=1.2)),
          ('live, mac scans', dict(color='#e8810c', lw=1.2))]
fine_ages = (np.arange(FINE_MAX * 1000 // FINE_MS + 1) + 1) * FINE_MS / 1000
for name, style in series:
    _, _, fine, ftot = res[name]
    ok = ftot >= 30
    ax.step(fine_ages[ok], 100 * fine[ok], where='pre', label=name, **style)
live_fine = res['live peers'][2]
for p in (50, 90, 99):
    ax.axhline(p, color='gray', lw=.6, ls=':')
    v = reach(live_fine, p / 100, FINE_MS / 1000)
    if v is not None:
        ax.annotate(f'{p}% by {v:g}s', (v, p), xytext=(8, -13), textcoords='offset points', fontsize=9, color='#2a78d6')
ax.set_xlim(0, FINE_MAX)
ax.set_ylim(0, 101)
ax.set_xlabel(f'block age (s since its slot), {FINE_MS} ms bins')
ax.set_ylabel('% of peers with the block on their chain')
ax.set_title('Block dissemination: first 20 s', loc='left', fontsize=11)
ax.grid(alpha=.3)
ax.legend(loc='lower right', fontsize=9)

ages = np.arange(a.max_age + 1)
for name, style in series[:2]:
    frac, tot = res[name][:2]
    ok = tot >= 50
    ax2.plot(ages[ok], 100 * frac[ok], label=name, **style)
ax2.set_xlim(0, a.max_age)
ax2.set_ylim(95, 100.2)
ax2.set_xlabel('block age (s), 1 s bins')
ax2.set_title('Tail: share still missing the block (y from 95%)', loc='left', fontsize=11)
ax2.grid(alpha=.3)
ax2.legend(loc='lower right', fontsize=9)
plt.tight_layout()
plt.savefig(a.out, dpi=130)
print(f'\nSaved {a.out}')
