#!/usr/bin/env python3
"""Set preferred CPU list by specifying two NUMA nodes."""
import argparse
from pathlib import Path
from typing import List

CPU_AFFINITY = Path(__file__).resolve().parent / "compute_node" / "handler" / "cpu_affinity.hpp"
NODE_LAYOUT = {
    0: {"phys": list(range(0, 8)), "ht": list(range(32, 40))},
    1: {"phys": list(range(8, 16)), "ht": list(range(40, 48))},
    2: {"phys": list(range(16, 24)), "ht": list(range(48, 56))},
    3: {"phys": list(range(24, 32)), "ht": list(range(56, 64))},
    # disable_smt = prefer NUMA1 physical cores first, then NUMA0/2/3
    "disable_smt": {
        "phys": [*range(8, 16), *range(0, 8), *range(16, 24), *range(24, 32)],
        "ht": [],
    },
}


def build_cpu_order(primary, secondary, include_rest: bool) -> List[int]:
    order: List[int] = []
    seen = []

    def add_node(node_key, use_ht=True):
        if node_key not in NODE_LAYOUT or node_key in seen:
            return
        seen.append(node_key)
        layout = NODE_LAYOUT[node_key]
        order.extend(layout["phys"])
        if use_ht:
            order.extend(layout["ht"])

    for node in (primary, secondary):
        add_node(node, use_ht=True)

    if include_rest:
        for node in [1, 0, 2, 3]:
            add_node(node, use_ht=True)
        remaining_phys = []
        for node in [0, 2, 3]:
            remaining_phys.extend(NODE_LAYOUT[node]["phys"])
        order.extend(remaining_phys)
    return order


def format_array(cpus: List[int]) -> str:
    rows = []
    for i in range(0, len(cpus), 8):
        rows.append("        " + ", ".join(str(c) for c in cpus[i : i + 8]) + ",")
    return "\n".join(rows)


def update_affinity(cpus: List[int]) -> None:
    contents = CPU_AFFINITY.read_text(encoding="utf-8")
    replacement = format_array(cpus)
    needle = "static constexpr int kPreferredCpuList[]"
    start = contents.find(needle)
    if start == -1:
        needle = "static const int preferred_cpus[]"
        start = contents.find(needle)
    if start == -1:
        raise RuntimeError("preferred CPU list not found")

    start_brace = contents.find("{", start)
    end_brace = contents.find("};", start_brace)
    if start_brace == -1 or end_brace == -1:
        raise RuntimeError("preferred CPU list braces not found")

    new_contents = (
        contents[: start_brace + 1]
        + "\n"
        + replacement
        + "\n"
        + contents[end_brace:]
    )
    CPU_AFFINITY.write_text(new_contents, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Update cpu_affinity using two NUMA nodes")
    parser.add_argument("primary", type=str,
                        help="Primary NUMA node id (0-3) or 'disable_smt'")
    parser.add_argument("secondary", type=str,
                        help="Secondary NUMA node id (0-3) or 'disable_smt'")
    parser.add_argument(
        "--no-rest",
        action="store_true",
        help="Do not append the remaining NUMA nodes after the two specified ones",
    )
    args = parser.parse_args()

    def normalize(value: str):
        if value == "disable_smt":
            return value
        try:
            node = int(value)
        except ValueError:
            raise SystemExit("invalid node '%s', expected 0-3 or disable_smt" % value)
        if node not in NODE_LAYOUT:
            raise SystemExit("invalid node '%s', expected 0-3 or disable_smt" % value)
        return node

    primary = normalize(args.primary)
    secondary = normalize(args.secondary)

    cpus = build_cpu_order(primary, secondary, include_rest=not args.no_rest)
    update_affinity(cpus)
    print(f"Updated {CPU_AFFINITY} with order: {cpus}")


if __name__ == "__main__":
    main()