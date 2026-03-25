"""
DEMO MASTER RUNNER
Runs all 6 demo steps in sequence with timing.

Usage:
    python demo_run_all.py           # run all 6 steps
    python demo_run_all.py 1 2 4     # run specific steps
"""
import asyncio
import importlib
import sys
import time


STEPS = {
    1: ("demo_step1_decision_history", "The Week Standard — Full decision history + integrity"),
    2: ("demo_step2_concurrency",       "Concurrency Under Pressure — Double-decision OCC"),
    3: ("demo_step3_temporal",          "Temporal Compliance Query — as_of time-travel"),
    4: ("demo_step4_upcasting",         "Upcasting & Immutability — v1→v2, raw DB unchanged"),
    5: ("demo_step5_gas_town",          "Gas Town Recovery — Crash + reconstruct context"),
    6: ("demo_step6_whatif",            "What-If Counterfactual — HIGH vs MEDIUM risk"),
}

SEP2 = "█" * 70


async def run_step(step_num: int) -> tuple[bool, float]:
    module_name, title = STEPS[step_num]
    print(f"\n{SEP2}")
    print(f"  STEP {step_num}  —  {title}")
    print(f"{SEP2}")

    t0 = time.monotonic()
    try:
        if step_num == 1:
            from demo_step1_decision_history import main
            await main("APEX-NARR05")
        elif step_num == 2:
            from demo_step2_concurrency import main
            await main()
        elif step_num == 3:
            from demo_step3_temporal import main
            await main()
        elif step_num == 4:
            from demo_step4_upcasting import main
            await main()
        elif step_num == 5:
            from demo_step5_gas_town import main
            await main()
        elif step_num == 6:
            from demo_step6_whatif import main
            await main()
        elapsed = time.monotonic() - t0
        print(f"  ✓  Step {step_num} completed in {elapsed:.1f}s")
        return True, elapsed
    except Exception as e:
        elapsed = time.monotonic() - t0
        print(f"  ✗  Step {step_num} FAILED after {elapsed:.1f}s: {e}")
        import traceback
        traceback.print_exc()
        return False, elapsed


async def main():
    if len(sys.argv) > 1:
        steps = [int(x) for x in sys.argv[1:] if x.isdigit()]
    else:
        steps = list(STEPS.keys())

    print(f"\n{'█'*70}")
    print(f"  APEX FINANCIAL SERVICES — LEDGER DEMO")
    print(f"  Running steps: {steps}")
    print(f"{'█'*70}")

    results = {}
    for step in steps:
        ok, elapsed = await run_step(step)
        results[step] = (ok, elapsed)

    print(f"\n{'█'*70}")
    print(f"  SUMMARY")
    print(f"{'█'*70}")
    total = 0.0
    for step, (ok, elapsed) in results.items():
        _, title = STEPS[step]
        status = "✓ PASS" if ok else "✗ FAIL"
        print(f"  Step {step}  {status}  {elapsed:.1f}s   {title}")
        total += elapsed
    print(f"  {'─'*66}")
    print(f"  Total: {total:.1f}s")
    all_ok = all(ok for ok, _ in results.values())
    print(f"  Result: {'✓ ALL PASSED' if all_ok else '✗ SOME FAILED'}")
    print(f"{'█'*70}\n")


if __name__ == "__main__":
    asyncio.run(main())
