"""Shared bisect-based MachineTimeline.

One timeline instance per physical machine. Stores committed intervals sorted
by start time; supports:
  - latest_feasible_start: ALAP placement <= a target, non-overlap & changeover
  - commit: insert an interval (keeps sorted)

Times are float MINUTES on a common epoch axis (see ingest.to_minutes_epoch).
All operations are deterministic - pure bisect, no scan ambiguity, no clock.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Interval:
    start: float
    end: float
    lot_id: str
    sku: str

    def __lt__(self, other: "Interval") -> bool:  # for bisect on start
        return self.start < other.start


@dataclass
class MachineTimeline:
    machine_id: str
    changeover_min: float = 15.0
    intervals: List[Interval] = field(default_factory=list)
    _start_cache: List[float] = field(default_factory=list)  # parallel sorted starts
    _load: float = 0.0                                        # running busy minutes

    # -- helpers -----------------------------------------------------------
    def _starts(self) -> List[float]:
        # Kept in sync with `intervals` by commit(); O(1).
        if len(self._start_cache) != len(self.intervals):
            self._start_cache = [iv.start for iv in self.intervals]
        return self._start_cache

    def load_minutes(self) -> float:
        return self._load

    def _changeover_between(self, sku_a: str, sku_b: str) -> float:
        """Changeover reserved only when consecutive lots differ in SKU."""
        if sku_a is None or sku_b is None:
            return 0.0
        return 0.0 if sku_a == sku_b else self.changeover_min

    # -- core query --------------------------------------------------------
    def latest_feasible_start(
        self, target_start: float, proc: float, sku: str,
        earliest: float = float("-inf"),
    ) -> Optional[float]:
        """Latest start S <= target_start such that [S, S+proc] fits without
        overlap and honours changeover gaps to neighbours. Returns None if no
        feasible slot >= earliest exists.

        Bisect-driven ALAP: intervals are kept sorted by start, so from the
        candidate S we only need to look LEFT through intervals that could
        overlap [S, S+proc]+changeover. Each conflict strictly pushes S below a
        distinct interval start, so the walk is O(k log n) with k = conflicts
        (deterministic; no full scan, no clock)."""
        if proc <= 0:
            proc = 0.0
        S = target_start
        if S < earliest:
            return None
        n = len(self.intervals)
        if n == 0:
            return S
        starts = self._starts()
        co = self.changeover_min

        guard = n + 2
        while guard >= 0:
            guard -= 1
            end = S + proc
            # Candidate window for overlap: any interval whose start < end+co and
            # whose end+co > S. Intervals are sorted by start; the right edge of
            # the relevant window is the last interval with start < end + co.
            hi = bisect.bisect_left(starts, end + co)   # exclusive upper index
            conflict = None
            # walk left from hi-1; stop once an interval ends well before S
            i = hi - 1
            while i >= 0:
                iv = self.intervals[i]
                co_before = 0.0 if (iv.sku == sku or iv.sku is None or sku is None) else co
                co_after = co_before
                # new lot [S,end]; iv [iv.start, iv.end]
                if end + co_after <= iv.start or iv.end + co_before <= S:
                    # No conflict with THIS iv. SAFE EARLY-STOP (BUG-N1 fix):
                    # committed intervals are pairwise NON-overlapping, so for every
                    # interval further left iv_j.end <= iv.start <= iv.end (end is
                    # monotone leftward). The termination bound must therefore use
                    # the machine-wide changeover `co`, NOT the per-iv `co_before`:
                    # once iv.end + co <= S, no further-left interval - same- OR
                    # different-SKU - can come within a changeover of S. The old
                    # bound `iv.end + co_before <= S` (co_before=0 for a same-SKU
                    # iv) could stop one interval too early and skip a different-SKU
                    # neighbour ending in (S-co, S] that needs the full 15-min gap,
                    # silently under-reserving the changeover (a C4 breach). Using
                    # `co` only walks further left (strictly safer; the per-iv
                    # conflict test above still uses co_before, which is correct).
                    if iv.end + co <= S:
                        break
                    i -= 1
                    continue
                conflict = iv
                break
            if conflict is None:
                return S if S >= earliest else None
            co_after = 0.0 if (conflict.sku == sku or conflict.sku is None
                               or sku is None) else co
            new_end = conflict.start - co_after
            S = new_end - proc
            if S < earliest:
                return None
        return None

    def earliest_feasible_start(
        self, target_start: float, proc: float, sku: str,
        latest: float = float("inf"),
    ) -> Optional[float]:
        """Earliest start S >= target_start such that [S, S+proc] fits without
        overlap and honours changeover gaps to neighbours, with S <= latest.
        Returns None if no feasible slot in [target_start, latest] exists.

        Dual of latest_feasible_start (FIX-A build level-loader): from the
        candidate S we look RIGHT through intervals that could overlap
        [S, S+proc]+changeover; each conflict strictly pushes S above a distinct
        interval end, so the walk is O(k log n) with k = conflicts. Deterministic
        (pure bisect, no clock, no RNG)."""
        if proc <= 0:
            proc = 0.0
        S = target_start
        if S > latest:
            return None
        n = len(self.intervals)
        if n == 0:
            return S if S <= latest else None
        starts = self._starts()
        co = self.changeover_min

        guard = n + 2
        while guard >= 0:
            guard -= 1
            end = S + proc
            # Any interval whose start < end+co and whose end+co > S overlaps.
            # Intervals are sorted by start; relevant ones have start < end + co.
            hi = bisect.bisect_left(starts, end + co)
            conflict = None
            # walk left from hi-1 to find the LAST (right-most) conflicting iv, so
            # the new S is pushed to just AFTER it (earliest forward feasible).
            i = hi - 1
            best_conf = None
            while i >= 0:
                iv = self.intervals[i]
                co_gap = 0.0 if (iv.sku == sku or iv.sku is None or sku is None) else co
                # conflict iff the two intervals (plus changeover) overlap
                if end + co_gap <= iv.start or iv.end + co_gap <= S:
                    # SAFE EARLY-STOP (BUG-N1 fix, dual of latest_feasible_start):
                    # non-overlap => end monotone leftward, so terminate on the
                    # machine-wide `co`, not the per-iv `co_gap` (which is 0 for a
                    # same-SKU iv and could skip a closer different-SKU neighbour).
                    if iv.end + co <= S:
                        # this iv (and all further left) end >co before S -> no more
                        break
                    i -= 1
                    continue
                # overlapping iv: track the one with the largest end (push past it)
                if best_conf is None or (iv.end + co_gap) > (best_conf[0]):
                    best_conf = (iv.end + co_gap, iv)
                i -= 1
            if best_conf is None:
                return S if S <= latest else None
            S = best_conf[0]                       # just after the binding iv (+co)
            if S > latest:
                return None
        return None

    def commit(self, start: float, proc: float, lot_id: str, sku: str) -> Interval:
        # INVARIANT (BUG-N2): `commit` and dispatch._remove_interval are the ONLY
        # mutators of `intervals`, and BOTH must keep `_start_cache` index-aligned
        # with `intervals` (parallel sorted-by-start lists). `_starts()` only
        # rebuilds on a length mismatch, so any future mutator that edits
        # `intervals` without the matching `_start_cache` edit would leave a stale
        # cache and corrupt bisect order. Do not add a third mutator without
        # preserving this invariant.
        iv = Interval(start=start, end=start + proc, lot_id=lot_id, sku=sku)
        idx = bisect.bisect_left(self._starts(), start)
        self.intervals.insert(idx, iv)
        self._start_cache.insert(idx, start)   # keep parallel cache in sync
        self._load += (iv.end - iv.start)
        return iv

    def changeover_at(self, start: float, proc: float, sku: str) -> float:
        """Estimated changeover incurred if [start, start+proc] for `sku` is
        placed here: > 0 when the immediate left/right neighbour is a different
        SKU. Used only as a machine-choice PREFERENCE (placement feasibility and
        the actual C/O reservation are handled in latest_feasible_start)."""
        if not self.intervals:
            return 0.0
        starts = self._starts()
        idx = bisect.bisect_left(starts, start)
        # L16: charge the changeover ONCE for the machine-choice preference. A
        # single placed lot incurs at most one SKU changeover reservation on this
        # machine (the gap to whichever differing-SKU neighbour binds), so adding
        # BOTH the left and right neighbour double-counted the preference cost and
        # could mis-rank an otherwise-equal machine. The actual C/O reservation is
        # still resolved per-gap in latest_feasible_start; this is preference only.
        diff_neighbour = False
        if idx < len(self.intervals):
            nb = self.intervals[idx]
            if nb.sku is not None and sku is not None and nb.sku != sku:
                diff_neighbour = True
        if not diff_neighbour and idx - 1 >= 0:
            nb = self.intervals[idx - 1]
            if nb.sku is not None and sku is not None and nb.sku != sku:
                diff_neighbour = True
        return self.changeover_min if diff_neighbour else 0.0

    def overlaps_any(self) -> List[tuple]:
        """Return list of overlapping (lot_a, lot_b) pairs - used by validate."""
        bad = []
        s = sorted(self.intervals, key=lambda iv: (iv.start, iv.lot_id))
        for i in range(1, len(s)):
            if s[i].start < s[i - 1].end - 1e-6:
                bad.append((s[i - 1].lot_id, s[i].lot_id))
        return bad
