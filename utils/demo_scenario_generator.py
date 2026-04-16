"""
Demo Scenario Generator — SIM Card Fraud Ring, Karnataka
=========================================================
Generates a purpose-built, story-driven CDR dataset for the CrimeNet demo.

The Scenario
------------
A coordinated SIM card fraud ring operates across Bengaluru over 30 days.

Cast:
  - 3 Ringleaders  : High betweenness centrality — coordinate all operations
  - 8 Intermediaries: Execute fraud calls, relay instructions from ringleaders
  - 4 Victims       : Receive calls only, never initiate contact
  - 2 Burner phones : Activated in Week 2, IMEI swapped in Week 3 (evasion tactic)

Timeline:
  Week 1  — Ringleaders set up network, brief intermediaries
  Week 2  — Burner phones introduced; pre-operation coordination intensifies
  Week 3  — Fraud Wave 1: co-location meeting at Koramangala, then victim calls
  Week 4  — Fraud Wave 2: co-location meeting at Whitefield, then victim calls
  Day 28+ — Ringleaders go silent (evasion); burners deactivate

Usage:
    python utils/demo_scenario_generator.py
    python utils/demo_scenario_generator.py --output data/sample/demo_fraud_ring.csv
"""

import csv
import random
import argparse
from datetime import datetime, timedelta


# ── Fixed cast of characters ──────────────────────────────────────────────────

RINGLEADERS = ["9900001111", "9900002222", "9900003333"]
INTERMEDIARIES = [
    "8800001111", "8800002222", "8800003333", "8800004444",
    "8800005555", "8800006666", "8800007777", "8800008888",
]
VICTIMS = ["7700001111", "7700002222", "7700003333", "7700004444"]
BURNER_1 = "6600001111"   # active weeks 2–3, IMEI swap in week 3
BURNER_2 = "6600002222"   # active weeks 2–4, IMEI swap in week 3

# Delhi masterminds — control the Karnataka ring from north India
DELHI_MASTERMINDS = ["9911000001", "9911000002"]

ALL_NUMBERS = RINGLEADERS + INTERMEDIARIES + VICTIMS + [BURNER_1, BURNER_2] + DELHI_MASTERMINDS

# Towers — Karnataka (operational) + Delhi/NCR (mastermind base)
TOWERS = {
    # Bengaluru cluster (meeting points — kept fixed for demo narrative)
    "KA-BLR-KORA": ("Bengaluru Koramangala",     12.9352, 77.6245),
    "KA-BLR-WHIT": ("Bengaluru Whitefield",       12.9698, 77.7499),
    "KA-BLR-BAND": ("Bengaluru Banaswadi",        13.0143, 77.6561),
    "KA-BLR-ELEC": ("Bengaluru Electronics City", 12.8418, 77.6604),
    "KA-BLR-YESH": ("Bengaluru Yeshwanthpur",     13.0291, 77.5478),
    # Diverse Karnataka locations — ringleader bases, roaming, escape routes
    "KA-MYS-CHMD": ("Mysuru Chamundipuram",       12.3051, 76.6552),
    "KA-HUB-TOWN": ("Hubballi Old Town",          15.3647, 75.1240),
    "KA-MNG-HMPK": ("Mangaluru Hampankatta",      12.8698, 74.8431),
    "KA-BLG-TILK": ("Belagavi Tilakwadi",         15.8497, 74.4977),
    "KA-SHV-KUVM": ("Shivamogga Kuvempu Nagar",   13.9299, 75.5681),
    "KA-TMK-GNDH": ("Tumakuru Gandhi Nagar",      13.3409, 77.1010),
    "KA-KLB-ALND": ("Kalaburagi Aland Road",      17.3297, 76.8200),
    "KA-UDU-MITR": ("Udupi MIT Road",             13.3409, 74.7421),
    "KA-BAL-GNDH": ("Ballari Gandhi Nagar",       15.1394, 76.9214),
    # Delhi/NCR — mastermind base of operations
    "DL-NDL-CPNP": ("New Delhi Connaught Place",  28.6315, 77.2167),
    "DL-NDL-ROHK": ("Delhi Rohini",               28.7041, 77.1025),
    "DL-GGN-SC29": ("Gurugram Sector 29",         28.4595, 77.0266),
    "DL-NDA-SC18": ("Noida Sector 18",            28.5700, 77.3219),
}
TOWER_LIST = list(TOWERS.items())

OPERATORS = ["Jio", "Airtel", "Vi", "BSNL"]

# Fixed IMEI per number (before swap)
IMEI_MAP = {num: _imei for num, _imei in
            zip(ALL_NUMBERS, [str(random.Random(i).randint(10**14, 10**15 - 1))
                               for i in range(len(ALL_NUMBERS))])}

# Post-swap IMEIs for burners (different device, same SIM)
BURNER_1_NEW_IMEI = str(random.Random(99).randint(10**14, 10**15 - 1))
BURNER_2_NEW_IMEI = str(random.Random(100).randint(10**14, 10**15 - 1))

IMSI_MAP = {num: "404" + str(random.Random(ord(num[0])).randint(10**11, 10**12 - 1))
            for num in ALL_NUMBERS}
OPERATOR_MAP = {num: random.Random(int(num[-4:])).choice(OPERATORS) for num in ALL_NUMBERS}

START_DATE = datetime(2024, 3, 1)
END_DATE   = datetime(2024, 3, 30, 23, 59, 59)

# Crime wave dates (co-location meetings + victim calls)
WAVE_1_DATE = datetime(2024, 3, 15)   # Week 3
WAVE_2_DATE = datetime(2024, 3, 22)   # Week 4
BURNER_ACTIVATE = datetime(2024, 3, 8)
BURNER_IMEI_SWAP = datetime(2024, 3, 15)
RINGLEADER_SILENCE = datetime(2024, 3, 27)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rand_dt(start: datetime, end: datetime, rng: random.Random) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, delta))


def _rand_tower(rng: random.Random, prefer: str = None):
    if prefer and rng.random() < 0.75:
        return prefer, TOWERS[prefer]
    key, val = rng.choice(TOWER_LIST)
    return key, val


def _imei(number: str, dt: datetime) -> str:
    if number == BURNER_1 and dt >= BURNER_IMEI_SWAP:
        return BURNER_1_NEW_IMEI
    if number == BURNER_2 and dt >= BURNER_IMEI_SWAP:
        return BURNER_2_NEW_IMEI
    return IMEI_MAP[number]


def _make_record(sr, caller, callee, dt, duration, call_type, tower_id, tower_info):
    return {
        "SR":               sr,
        "Calling_Number":   caller,
        "Called_Number":    callee,
        "Date":             dt.strftime("%Y-%m-%d"),
        "Time":             dt.strftime("%H:%M:%S"),
        "Duration_sec":     duration,
        "Call_Type":        call_type,
        "Caller_IMEI":      _imei(caller, dt),
        "Caller_IMSI":      IMSI_MAP[caller],
        "Caller_Operator":  OPERATOR_MAP[caller],
        "Tower_ID":         tower_id,
        "Tower_Location":   tower_info[0],
        "Tower_Latitude":   tower_info[1],
        "Tower_Longitude":  tower_info[2],
    }


# ── Scene builders ────────────────────────────────────────────────────────────

def _week1_setup(rng: random.Random) -> list:
    """Ringleaders brief all 8 intermediaries — network formation."""
    records = []
    w1_start = START_DATE
    w1_end   = START_DATE + timedelta(days=6, hours=23, minutes=59)

    # Ringleaders operate from different home bases across the state
    rl_home = {
        RINGLEADERS[0]: "KA-MNG-HMPK",   # Ringleader 1 — Mangaluru coast
        RINGLEADERS[1]: "KA-BLG-TILK",   # Ringleader 2 — Belagavi northwest
        RINGLEADERS[2]: "KA-BLR-YESH",   # Ringleader 3 — Bengaluru
    }
    for _ in range(60):
        rl = rng.choice(RINGLEADERS)
        im = rng.choice(INTERMEDIARIES)
        dt = _rand_dt(w1_start, w1_end, rng)
        tid, tinfo = _rand_tower(rng, rl_home[rl])
        records.append(_make_record(0, rl, im, dt, rng.randint(60, 480), "Voice", tid, tinfo))

    # Intermediaries also talk among themselves
    for _ in range(25):
        a, b = rng.sample(INTERMEDIARIES, 2)
        dt = _rand_dt(w1_start, w1_end, rng)
        tid, tinfo = _rand_tower(rng)
        records.append(_make_record(0, a, b, dt, rng.randint(30, 200), "Voice", tid, tinfo))

    # Some SMS coordination
    for _ in range(15):
        rl = rng.choice(RINGLEADERS)
        im = rng.choice(INTERMEDIARIES)
        dt = _rand_dt(w1_start, w1_end, rng)
        tid, tinfo = _rand_tower(rng)
        records.append(_make_record(0, rl, im, dt, 0, "SMS", tid, tinfo))

    return records


def _week2_burner_intro(rng: random.Random) -> list:
    """Burner phones activated. Ringleaders communicate through burners."""
    records = []
    w2_start = START_DATE + timedelta(days=7)
    w2_end   = START_DATE + timedelta(days=13, hours=23, minutes=59)

    # Burner 1 originates from coastal Udupi (evasion — far from Bengaluru)
    for _ in range(20):
        dt = _rand_dt(BURNER_ACTIVATE, w2_end, rng)
        target = rng.choice(RINGLEADERS)
        tid, tinfo = _rand_tower(rng, "KA-UDU-MITR")
        records.append(_make_record(0, BURNER_1, target, dt, rng.randint(120, 600), "Voice", tid, tinfo))

    # Burner 2 originates from Shivamogga (central — different region)
    for _ in range(18):
        dt = _rand_dt(BURNER_ACTIVATE, w2_end, rng)
        target = rng.choice(INTERMEDIARIES[:4])
        tid, tinfo = _rand_tower(rng, "KA-SHV-KUVM")
        records.append(_make_record(0, BURNER_2, target, dt, rng.randint(60, 400), "Voice", tid, tinfo))

    # Ringleaders travel toward Bengaluru — late-night coordination
    for _ in range(40):
        rl = rng.choice(RINGLEADERS)
        im = rng.choice(INTERMEDIARIES)
        dt = _rand_dt(w2_start, w2_end, rng)
        dt = dt.replace(hour=rng.choice([21, 22, 23, 0, 1, 2]))
        # Mix of home base + Tumakuru corridor (en route to Bengaluru)
        prefer = rng.choice(["KA-TMK-GNDH", "KA-BLR-YESH", "KA-SHV-KUVM"])
        tid, tinfo = _rand_tower(rng, prefer)
        records.append(_make_record(0, rl, im, dt, rng.randint(60, 360), "Voice", tid, tinfo))

    return records


def _wave_colocation_meeting(wave_dt: datetime, tower_id: str, rng: random.Random) -> list:
    """
    All 3 ringleaders + 2 key intermediaries show up at the same tower
    within a 25-minute window — a physical planning meeting.
    """
    records = []
    meeting_numbers = RINGLEADERS + INTERMEDIARIES[:2]

    for num in meeting_numbers:
        # Each person arrives slightly offset (0–20 min spread)
        offset_min = rng.randint(0, 20)
        dt = wave_dt + timedelta(minutes=offset_min)
        # Short check-in calls between themselves during meeting
        others = [n for n in meeting_numbers if n != num]
        peer = rng.choice(others)
        tinfo = TOWERS[tower_id]
        records.append(_make_record(0, num, peer, dt, rng.randint(15, 90), "Voice", tower_id, tinfo))

    return records


def _wave_victim_calls(wave_dt: datetime, rng: random.Random) -> list:
    """
    Fraud wave: intermediaries make rapid short calls to victims
    immediately after the meeting (coded vishing pattern).
    """
    records = []
    wave_end = wave_dt + timedelta(hours=6)

    for _ in range(35):
        im = rng.choice(INTERMEDIARIES)
        victim = rng.choice(VICTIMS)
        dt = _rand_dt(wave_dt + timedelta(hours=1), wave_end, rng)
        tid, tinfo = _rand_tower(rng, "KA-BLR-ELEC")
        # Short duration — vishing / coded calls
        records.append(_make_record(0, im, victim, dt, rng.randint(5, 45), "Voice", tid, tinfo))

    # Victims occasionally call back — confused/panicked responses
    for _ in range(8):
        victim = rng.choice(VICTIMS)
        im = rng.choice(INTERMEDIARIES)
        dt = _rand_dt(wave_dt + timedelta(hours=2), wave_end, rng)
        tid, tinfo = _rand_tower(rng)
        records.append(_make_record(0, victim, im, dt, rng.randint(30, 180), "Voice", tid, tinfo))

    return records


def _week3_wave1(rng: random.Random) -> list:
    """Wave 1 — Koramangala meeting + victim fraud calls. IMEI swap happens."""
    records = []

    # IMEI swap context: burners now using new device
    # Pre-meeting calls (night before)
    for _ in range(12):
        rl = rng.choice(RINGLEADERS)
        im = rng.choice(INTERMEDIARIES)
        dt = WAVE_1_DATE - timedelta(hours=rng.randint(1, 12))
        dt = dt.replace(hour=rng.choice([22, 23, 0, 1]))
        tid, tinfo = _rand_tower(rng)
        records.append(_make_record(0, rl, im, dt, rng.randint(60, 300), "Voice", tid, tinfo))

    # The meeting itself
    records += _wave_colocation_meeting(
        wave_dt=WAVE_1_DATE.replace(hour=10, minute=0),
        tower_id="KA-BLR-KORA",
        rng=rng,
    )

    # Fraud calls
    records += _wave_victim_calls(WAVE_1_DATE.replace(hour=10), rng)

    # Post-wave: burners go quiet for a few days
    return records


def _week4_wave2(rng: random.Random) -> list:
    """Wave 2 — Whitefield meeting + second round of victim calls."""
    records = []

    # Pre-meeting coordination
    for _ in range(10):
        rl = rng.choice(RINGLEADERS)
        im = rng.choice(INTERMEDIARIES[4:])   # second batch of intermediaries
        dt = WAVE_2_DATE - timedelta(hours=rng.randint(2, 18))
        tid, tinfo = _rand_tower(rng)
        records.append(_make_record(0, rl, im, dt, rng.randint(30, 240), "Voice", tid, tinfo))

    # Meeting at Whitefield
    records += _wave_colocation_meeting(
        wave_dt=WAVE_2_DATE.replace(hour=9, minute=30),
        tower_id="KA-BLR-WHIT",
        rng=rng,
    )

    # Fraud calls — more aggressive volume this time
    records += _wave_victim_calls(WAVE_2_DATE.replace(hour=9), rng)

    # BURNER_2 moves north to Kalaburagi after wave (escape route)
    for _ in range(8):
        dt = _rand_dt(WAVE_2_DATE, WAVE_2_DATE + timedelta(days=4), rng)
        tid, tinfo = _rand_tower(rng, "KA-KLB-ALND")
        records.append(_make_record(0, BURNER_2, RINGLEADERS[1], dt, rng.randint(90, 500), "Voice", tid, tinfo))

    return records


def _ringleader_silence(rng: random.Random) -> list:
    """After day 27, ringleaders go dark — communication silence (evasion)."""
    records = []
    # Intermediaries scatter — Ballari and Kalaburagi (northeast/flight pattern)
    end = END_DATE
    for _ in range(20):
        a, b = rng.sample(INTERMEDIARIES, 2)
        dt = _rand_dt(RINGLEADER_SILENCE, end, rng)
        prefer = rng.choice(["KA-BAL-GNDH", "KA-KLB-ALND", "KA-HUB-TOWN"])
        tid, tinfo = _rand_tower(rng, prefer)
        records.append(_make_record(0, a, b, dt, rng.randint(10, 120), "Voice", tid, tinfo))

    # Some panicked victim outbound calls (trying to report/call back)
    for _ in range(6):
        victim = rng.choice(VICTIMS)
        im = rng.choice(INTERMEDIARIES)
        dt = _rand_dt(RINGLEADER_SILENCE, end, rng)
        tid, tinfo = _rand_tower(rng)
        records.append(_make_record(0, victim, im, dt, rng.randint(5, 60), "Voice", tid, tinfo))

    return records


def _delhi_mastermind_calls(rng: random.Random) -> list:
    """
    Delhi masterminds give high-level orders to Karnataka ringleaders.
    They call only the 3 ringleaders — never intermediaries or victims.
    Operate exclusively from Delhi/NCR towers — clear geographic separation.
    """
    records = []
    delhi_towers = ["DL-NDL-CPNP", "DL-NDL-ROHK", "DL-GGN-SC29", "DL-NDA-SC18"]

    # Strategic calls before each fraud wave and at key moments
    call_windows = [
        (START_DATE,                           START_DATE + timedelta(days=3)),   # initial briefing
        (WAVE_1_DATE - timedelta(days=3),      WAVE_1_DATE - timedelta(days=1)),  # pre-wave 1 order
        (WAVE_2_DATE - timedelta(days=3),      WAVE_2_DATE - timedelta(days=1)),  # pre-wave 2 order
        (RINGLEADER_SILENCE,                   END_DATE),                          # shutdown signal
    ]

    for w_start, w_end in call_windows:
        for _ in range(rng.randint(4, 8)):
            mastermind = rng.choice(DELHI_MASTERMINDS)
            rl = rng.choice(RINGLEADERS)
            dt = _rand_dt(w_start, w_end, rng)
            # Always calls from Delhi tower
            tid = rng.choice(delhi_towers)
            tinfo = TOWERS[tid]
            records.append(_make_record(0, mastermind, rl, dt, rng.randint(120, 900), "Voice", tid, tinfo))

    # Occasional SMS — terse coded instructions
    for _ in range(10):
        mastermind = rng.choice(DELHI_MASTERMINDS)
        rl = rng.choice(RINGLEADERS)
        dt = _rand_dt(START_DATE, END_DATE, rng)
        tid = rng.choice(delhi_towers)
        tinfo = TOWERS[tid]
        records.append(_make_record(0, mastermind, rl, dt, 0, "SMS", tid, tinfo))

    return records


# ── Main generator ────────────────────────────────────────────────────────────

def generate_demo_scenario(output_file: str = "data/sample/demo_fraud_ring.csv", seed: int = 42):
    rng = random.Random(seed)

    all_records = []
    all_records += _week1_setup(rng)
    all_records += _week2_burner_intro(rng)
    all_records += _week3_wave1(rng)
    all_records += _week4_wave2(rng)
    all_records += _ringleader_silence(rng)
    all_records += _delhi_mastermind_calls(rng)

    # Sort chronologically and assign SR
    all_records.sort(key=lambda r: r["Date"] + r["Time"])
    for i, r in enumerate(all_records):
        r["SR"] = i + 1

    fieldnames = list(all_records[0].keys())
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)

    # Print scenario summary
    print("\n" + "=" * 60)
    print("  CrimeNet — Demo Scenario Generated")
    print("=" * 60)
    print(f"  Output file       : {output_file}")
    print(f"  Total records     : {len(all_records)}")
    print(f"  Date range        : {all_records[0]['Date']} → {all_records[-1]['Date']}")
    print(f"\n  Cast of characters:")
    print(f"    Delhi Masterminds: {DELHI_MASTERMINDS}")
    print(f"    Ringleaders      : {RINGLEADERS}")
    print(f"    Intermediaries   : {INTERMEDIARIES}")
    print(f"    Victims          : {VICTIMS}")
    print(f"    Burner Phone 1  : {BURNER_1}  (IMEI swap on {BURNER_IMEI_SWAP.date()})")
    print(f"    Burner Phone 2  : {BURNER_2}  (IMEI swap on {BURNER_IMEI_SWAP.date()})")
    print(f"\n  Key events:")
    print(f"    {BURNER_ACTIVATE.date()} — Burner phones activated")
    print(f"    {BURNER_IMEI_SWAP.date()} — IMEI swap on both burners")
    print(f"    {WAVE_1_DATE.date()} — Fraud Wave 1 (co-location: Koramangala)")
    print(f"    {WAVE_2_DATE.date()} — Fraud Wave 2 (co-location: Whitefield)")
    print(f"    {RINGLEADER_SILENCE.date()} — Ringleaders go silent")
    print("=" * 60)
    print("\n  What the analysis will reveal:")
    print("   Network  → 9900001111/2222/3333 top betweenness centrality")
    print("   Geo map  → Meeting cluster at Koramangala + Whitefield")
    print("   Anomaly  → 6600001111/2222 flagged as burners (IMEI swap)")
    print("   Temporal → Late-night coordination spikes in week 2")
    print("   Dismantle→ Removing 3 ringleaders collapses 80%+ of network")
    print()

    return all_records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate CrimeNet demo fraud ring scenario.")
    parser.add_argument("--output", default="data/sample/demo_fraud_ring.csv", help="Output CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()
    generate_demo_scenario(output_file=args.output, seed=args.seed)
