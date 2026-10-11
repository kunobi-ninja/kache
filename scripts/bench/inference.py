"""Exact order-randomization tests; no third-party numerical dependency."""

import itertools
import math
import statistics

ALPHA = 0.05


def coefficients(effects, orders, kind):
    """Slot differences under the observed assignment, and all allowed assignments."""
    n = len(effects)
    slots = [effect * sign for effect, sign in zip(effects, orders, strict=True)]
    if kind == "balanced":
        assignments = (
            tuple(1 if i in positives else -1 for i in range(n))
            for positives in itertools.combinations(range(n), n // 2)
        )
    elif kind == "independent":
        assignments = itertools.product((-1, 1), repeat=n)
    else:
        raise ValueError("unknown randomization kind")
    return [
        (statistics.fmean(q * slot for q, slot in zip(signs, slots, strict=True)),
         statistics.fmean(q * observed for q, observed in zip(signs, orders, strict=True)))
        for signs in assignments
    ]


def probabilities(coeffs, observed):
    tolerance = 1e-12 * max(1, abs(observed), max(abs(a) for a, _ in coeffs))
    total = len(coeffs)
    return (
        sum(a >= observed - tolerance for a, _ in coeffs) / total,
        sum(abs(a) >= abs(observed) - tolerance for a, _ in coeffs) / total,
    )


def interval(coeffs, observed, alpha):
    """Invert the two-sided constant-effect test in O(m log m).

    Each assignment contributes a closed interval (or half-line) where
    |a - b*effect| >= |observed - effect|. Sweep those intervals and return
    the outer hull of effects whose exact p-value exceeds alpha. Endpoints
    include ties. None denotes an unbounded endpoint, never zero uncertainty.
    """
    events = {}
    active = 0

    def event(point, entering):
        entry = events.setdefault(point, [0, 0])
        entry[0 if entering else 1] += 1

    for a, b in coeffs:
        if b == 1:
            if a == observed:
                active += 1
            elif a > observed:
                active += 1
                event((a + observed) / 2, False)
            else:
                event((a + observed) / 2, True)
        elif b == -1:
            if a == -observed:
                active += 1
            elif a + observed > 0:
                event((observed - a) / 2, True)
            else:
                active += 1
                event((observed - a) / 2, False)
        else:
            endpoints = sorted(((observed - a) / (1 - b),
                                (a + observed) / (1 + b)))
            event(endpoints[0], True)
            event(endpoints[1], False)
    required = math.floor(alpha * len(coeffs)) + 1
    accepted = []
    previous = None
    for point, (enter, leave) in sorted(events.items()):
        if active >= required:
            accepted.append((previous, point))
        if active + enter >= required:
            accepted.append((point, point))
        active += enter - leave
        previous = point
    if active >= required:
        accepted.append((previous, None))
    if not accepted:
        raise ValueError("empty randomization confidence set")
    return [accepted[0][0], accepted[-1][1]]


def analyze(effects, design):
    """Inference is conditional on this run's slots and its recorded assignment."""
    orders = [1 if block["order"][0] == "base" else -1
              for block in design["blocks"]]
    if len(effects) != len(orders) or not all(math.isfinite(x) for x in effects):
        raise ValueError("incomplete or invalid seed-block effects")
    coeffs = coefficients(effects, orders, design["kind"])
    observed = statistics.fmean(effects)
    p_slow, p_change = probabilities(coeffs, observed)
    family = design["family_size"]
    adjusted = min(1.0, family * p_change)
    bounds = interval(coeffs, observed, ALPHA / family)
    outcome = "inconclusive"
    if adjusted <= ALPHA:
        if observed > 0:
            outcome = "regression"
        elif observed < 0:
            outcome = "improvement"
    return {
        "method": "exact_order_randomization",
        "randomization_kind": design["kind"],
        "blocks": len(effects),
        "assignments": len(coeffs),
        "family_size": family,
        "confidence": 1 - ALPHA / family,
        "effect": observed,
        "interval": bounds,
        "p_slowdown": p_slow,
        "p_value": p_change,
        "p_adjusted": adjusted,
        "outcome": outcome,
        "scope": "conditional on this run; constant effect model for interval",
    }
