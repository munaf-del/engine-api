# RUNNER REGISTRY
# Maps calculator types + versions to runner files

RUNNER_REGISTRY = {
    "MULTI_PILE_FOUNDATION": {
        "v6": "multi_lp_runner_v6.py",
        "v6_3": "multi_lp_runner_v6_3.py",
        "v6_3_v2_1": "multi_lp_runner_v6_3_v2_1.py",
        "v6_3_v2_1_layout": "multi_lp_runner_v6_3_v2_1_layout.py",
        "v7": "multi_lp_runner_v7.py",
    }
}


def resolve_runner(calculator_key: str, version: str):
    if calculator_key not in RUNNER_REGISTRY:
        raise ValueError(f"Unknown calculator: {calculator_key}")

    versions = RUNNER_REGISTRY[calculator_key]

    if version not in versions:
        raise ValueError(f"Unsupported version '{version}' for {calculator_key}")

    return versions[version]