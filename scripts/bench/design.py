"""Predeclared base/head randomization, at the cold-cache seed level."""

import random


def plan(samples, cold_every, seed, family_size):
    for name, value in (("samples", samples), ("cold_every", cold_every),
                        ("family_size", family_size)):
        if type(value) is not int or value < 1:
            raise ValueError(f"randomization: invalid {name}")
    if type(seed) is not int:
        raise ValueError("randomization: invalid seed")
    blocks = (samples + cold_every - 1) // cold_every
    if blocks > 20:
        raise ValueError("randomization: at most 20 seed blocks supported")
    rng = random.Random(seed)
    if blocks % 2 == 0:
        signs = [1] * (blocks // 2) + [-1] * (blocks // 2)
        rng.shuffle(signs)
        kind = "balanced"
    else:
        signs = [rng.choice((-1, 1)) for _ in range(blocks)]
        kind = "independent"
    return {
        "version": 1,
        "unit": "cold_seed",
        "kind": kind,
        "seed": seed,
        "samples": samples,
        "cold_every": cold_every,
        "family_size": family_size,
        "blocks": [
            {"samples": list(range(start, min(start + cold_every, samples))),
             "order": ["base", "head"] if signs[i] == 1 else ["head", "base"]}
            for i, start in enumerate(range(0, samples, cold_every))
        ],
    }


def checked(design):
    if not isinstance(design, dict):
        raise ValueError("randomization: invalid design")
    try:
        expected = plan(design["samples"], design["cold_every"], design["seed"], design["family_size"])
    except KeyError as error:
        raise ValueError("randomization: missing design metadata") from error
    if design != expected:
        raise ValueError("randomization: schedule does not match its declared design")
    return design


def ordered(sample, arms, design):
    """Keep context tools after the adjacent randomized base/head pair."""
    names = {arm[0] for arm in arms}
    if design is None or not {"base", "head"} <= names:
        return arms
    order = design["blocks"][sample // design["cold_every"]]["order"]
    by_name = {arm[0]: arm for arm in arms}
    return [by_name[name] for name in order] + [
        arm for arm in arms if arm[0] not in order
    ]


def verify_pairs(base, head, design, phase, allow_incomplete=False):
    """Check observations against the plan before any significance calculation."""
    if design is None:
        return None
    checked(design)
    expected = set(range(design["samples"]))
    if phase == "cold":
        expected = {block["samples"][0] for block in design["blocks"]}
    if not set(base) <= expected or not set(head) <= expected:
        raise ValueError("randomization: unexpected sample")
    for sample in base:
        order = design["blocks"][sample // design["cold_every"]]["order"]
        for arm, records in (("base", base), ("head", head)):
            if records[sample].get("position") != order.index(arm):
                raise ValueError("randomization: observed order differs from schedule")
    if set(base) != expected or set(head) != expected:
        if allow_incomplete:
            return None
        raise ValueError("incomplete planned randomization blocks")
    return None if allow_incomplete else design
