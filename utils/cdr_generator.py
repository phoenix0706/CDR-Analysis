"""
Fake CDR (Call Detail Record) Dataset Generator
================================================
Generates synthetic CDR data for digital forensics practice.

Usage:
    python generate_cdr.py                        # default: 100 records, random scenario
    python generate_cdr.py --records 500          # 500 records
    python generate_cdr.py --scenario gang        # gang scenario
    python generate_cdr.py --scenario fraud --records 200 --state MH
    python generate_cdr.py --help                 # show all options

Scenarios:
    random  - General random activity
    gang    - Organized group with hub nodes
    fraud   - 1-2 numbers dominate outbound calls
    burner  - Ghost numbers active only briefly
"""

import csv
import random
import argparse
from datetime import datetime, timedelta


# ──────────────────────────────────────────────
# DATA POOLS
# ──────────────────────────────────────────────

TOWERS = {
    "UP": [
        ("UP-LKO-01", "Lucknow Hazratganj",        26.8467, 80.9462),
        ("UP-LKO-02", "Lucknow Aliganj",            26.8827, 80.9371),
        ("UP-KNP-01", "Kanpur Swaroop Nagar",       26.4499, 80.3319),
        ("UP-KNP-02", "Kanpur Kidwai Nagar",        26.4674, 80.3496),
        ("UP-VNS-01", "Varanasi Sigra",              25.3176, 82.9739),
        ("UP-AGR-01", "Agra Sanjay Place",           27.1767, 78.0081),
        ("UP-MRT-01", "Meerut Shastri Nagar",        28.9845, 77.7064),
        ("UP-GZB-01", "Ghaziabad Indirapuram",       28.6412, 77.3688),
        ("UP-PYG-01", "Prayagraj Civil Lines",       25.4358, 81.8463),
        ("UP-ALG-01", "Aligarh Ramghat Road",        27.8974, 78.0880),
    ],
    "MH": [
        ("MH-MUM-01", "Mumbai Bandra",               19.0596, 72.8295),
        ("MH-MUM-02", "Mumbai Andheri",              19.1136, 72.8697),
        ("MH-MUM-03", "Mumbai Thane",                19.2183, 72.9781),
        ("MH-PUN-01", "Pune Koregaon Park",          18.5362, 73.8944),
        ("MH-NGP-01", "Nagpur Civil Lines",          21.1458, 79.0882),
        ("MH-NSK-01", "Nashik CBS",                  19.9975, 73.7898),
        ("MH-AUR-01", "Aurangabad Cantonment",       19.8762, 75.3433),
        ("MH-KLH-01", "Kolhapur Rajaram Road",       16.7050, 74.2433),
    ],
    "DL": [
        ("DL-NDL-01", "New Delhi Connaught Place",   28.6315, 77.2167),
        ("DL-NDL-02", "Delhi Rohini",                28.7041, 77.1025),
        ("DL-NDL-03", "Delhi Dwarka",                28.5921, 77.0460),
        ("DL-NDL-04", "Delhi Laxmi Nagar",           28.6317, 77.2767),
        ("DL-GGN-01", "Gurugram Sector 29",          28.4595, 77.0266),
        ("DL-NDA-01", "Noida Sector 18",             28.5700, 77.3219),
        ("DL-FBD-01", "Faridabad Sector 15",         28.4089, 77.3178),
        ("DL-GZB-01", "Ghaziabad Raj Nagar",         28.6692, 77.4538),
    ],
    "RJ": [
        ("RJ-JPR-01", "Jaipur Malviya Nagar",        26.8505, 75.8000),
        ("RJ-JPR-02", "Jaipur Vaishali Nagar",       26.9124, 75.7318),
        ("RJ-JDH-01", "Jodhpur Shastri Nagar",       26.2389, 73.0243),
        ("RJ-UDR-01", "Udaipur Hiran Magri",         24.5854, 73.7125),
        ("RJ-AJM-01", "Ajmer Vaishali Nagar",        26.4499, 74.6399),
        ("RJ-KOT-01", "Kota Industrial Area",        25.1802, 75.8389),
        ("RJ-BKN-01", "Bikaner Ganga Shahar",        28.0229, 73.3119),
        ("RJ-ALW-01", "Alwar Model Town",            27.5534, 76.6346),
    ],
    "PB": [
        ("PB-LDH-01", "Ludhiana Model Town",         30.9010, 75.8573),
        ("PB-LDH-02", "Ludhiana Sarabha Nagar",      30.8801, 75.8245),
        ("PB-AMR-01", "Amritsar Hall Bazar",         31.6340, 74.8723),
        ("PB-JLN-01", "Jalandhar Civil Lines",       31.3260, 75.5762),
        ("PB-PTL-01", "Patiala Leela Bhawan",        30.3398, 76.3869),
        ("PB-BHT-01", "Bathinda Thermal Plant Road", 30.2110, 74.9455),
    ],
    "KA": [
        ("KA-BLR-01", "Bengaluru Koramangala",     12.9352, 77.6245),
        ("KA-BLR-02", "Bengaluru Whitefield",      12.9698, 77.7499),
        ("KA-BLR-03", "Bengaluru Hebbal",          13.0450, 77.5950),
        ("KA-MYS-01", "Mysuru Chamundipuram",      12.3051, 76.6552),
        ("KA-HUB-01", "Hubballi Old Town",         15.3647, 75.1240),
        ("KA-MNG-01", "Mangaluru Hampankatta",     12.8698, 74.8431),
        ("KA-BLG-01", "Belagavi Tilakwadi",        15.8497, 74.4977),
        ("KA-SHV-01", "Shivamogga Kuvempu Nagar", 13.9299, 75.5681),
        ("KA-TMK-01", "Tumakuru Gandhi Nagar",     13.3409, 77.1010),
        ("KA-KLB-01", "Kalaburagi Aland Road",     17.3297, 76.8200),
        ("KA-UDU-01", "Udupi MIT Road",            13.3409, 74.7421),
        ("KA-BAL-01", "Ballari Gandhi Nagar",      15.1394, 76.9214),
    ],
    "TN": [
        ("TN-CHN-01", "Chennai T. Nagar",          13.0418, 80.2341),
        ("TN-CHN-02", "Chennai Anna Nagar",        13.0850, 80.2101),
        ("TN-CHN-03", "Chennai Velachery",         12.9815, 80.2180),
        ("TN-CBE-01", "Coimbatore RS Puram",       11.0064, 76.9620),
        ("TN-MDU-01", "Madurai Goripalayam",        9.9252, 78.1198),
        ("TN-TRV-01", "Tiruchirappalli Cantonment",10.8050, 78.6856),
        ("TN-SLM-01", "Salem Fairlands",           11.6643, 78.1460),
        ("TN-VEL-01", "Vellore Katpadi",           12.9165, 79.1325),
    ],
}

OPERATORS = ["Jio", "Airtel", "Vi", "BSNL"]
CALL_TYPES = ["Voice", "Voice", "Voice", "SMS", "SMS", "Data"]   # weighted


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def gen_phone():
    prefix = random.choice(["6", "7", "8", "9"])
    return prefix + "".join([str(random.randint(0, 9)) for _ in range(9)])

def gen_imei():
    return "".join([str(random.randint(0, 9)) for _ in range(15)])

def gen_imsi():
    # India MCC=404/405, MNC varies
    mcc_mnc = random.choice(["40410", "40420", "40430", "40450", "40470"])
    return mcc_mnc + "".join([str(random.randint(0, 9)) for _ in range(10)])

def rand_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def duration_for_type(call_type: str) -> int:
    if call_type == "SMS":
        return 0
    elif call_type == "Data":
        return random.randint(60, 3600)
    else:
        return random.randint(5, 900)


# ──────────────────────────────────────────────
# SCENARIO LOGIC
# ──────────────────────────────────────────────

def pick_caller_callee(phones: list, scenario: str, num_nodes: int) -> tuple:
    """Return (caller, callee) based on scenario."""
    if scenario == "gang":
        # A core group of ~half the nodes initiates most calls
        core = phones[:max(2, num_nodes // 2)]
        caller = random.choice(core)
        callee = random.choice(phones)

    elif scenario == "fraud":
        # 1-2 numbers make almost all outbound calls (call center fraud)
        caller = random.choice(phones[:2])
        callee = random.choice(phones)

    elif scenario == "burner":
        # Last phone is a burner — calls only a few targets
        if random.random() < 0.4:
            caller = phones[-1]                       # burner initiates
            callee = random.choice(phones[:num_nodes])
        else:
            caller = random.choice(phones[:num_nodes])
            callee = random.choice(phones)

    else:  # random
        caller = random.choice(phones)
        callee = random.choice(phones)

    # Avoid self-calls
    attempts = 0
    while caller == callee and attempts < 10:
        callee = random.choice(phones)
        attempts += 1

    return caller, callee


# ──────────────────────────────────────────────
# MAIN GENERATOR
# ──────────────────────────────────────────────

def generate_cdr(
    num_records: int = 100,
    num_nodes: int = 8,
    start_date: str = "2024-01-01",
    end_date: str = "2024-03-31",
    scenario: str = "random",
    state: str = "UP",
    output_file: str = "fake_cdr_data.csv",
    seed: int = None,
) -> list:
    """
    Generate synthetic CDR records and save to CSV.

    Parameters
    ----------
    num_records : int     – Number of CDR rows to generate
    num_nodes   : int     – Number of unique phone numbers (suspects)
    start_date  : str     – ISO date string YYYY-MM-DD
    end_date    : str     – ISO date string YYYY-MM-DD
    scenario    : str     – One of: random, gang, fraud, burner
    state       : str     – One of: UP, MH, DL, KA, TN
    output_file : str     – Output CSV filename
    seed        : int     – Random seed for reproducibility (optional)

    Returns
    -------
    list of dicts representing the generated records
    """
    if seed is not None:
        random.seed(seed)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")
    # ALL = pan-India dataset pulling from every state's towers
    if state == "ALL":
        towers = [t for state_towers in TOWERS.values() for t in state_towers]
    else:
        towers = TOWERS.get(state, TOWERS["UP"])

    # Build phone pool
    phones = [gen_phone() for _ in range(num_nodes)]

    # Add burner phones (short-lived numbers)
    burner_phones = []
    if scenario == "burner":
        burner_phones = [gen_phone() for _ in range(3)]
        phones.extend(burner_phones)

    # Map phone → IMEI, IMSI, operator (consistent per number)
    phone_meta = {
        p: {
            "imei":     gen_imei(),
            "imsi":     gen_imsi(),
            "operator": random.choice(OPERATORS),
        }
        for p in phones
    }

    # Assign each phone a primary home tower — 65% of calls from home, 35% roaming
    # This ensures different numbers cluster around different locations on the map
    phone_home_tower = {p: random.choice(towers) for p in phones}

    records = []
    for _ in range(num_records):
        caller, callee = pick_caller_callee(phones, scenario, num_nodes)
        dt        = rand_datetime(start_dt, end_dt)
        call_type = random.choice(CALL_TYPES)
        duration  = duration_for_type(call_type)
        # 65% chance: use caller's home tower; 35% chance: random roaming tower
        if random.random() < 0.65:
            tower = phone_home_tower[caller]
        else:
            tower = random.choice(towers)

        records.append({
            "SR":              len(records) + 1,
            "Calling_Number":  caller,
            "Called_Number":   callee,
            "Date":            dt.strftime("%Y-%m-%d"),
            "Time":            dt.strftime("%H:%M:%S"),
            "Duration_sec":    duration,
            "Call_Type":       call_type,
            "Caller_IMEI":     phone_meta[caller]["imei"],
            "Caller_IMSI":     phone_meta[caller]["imsi"],
            "Caller_Operator": phone_meta[caller]["operator"],
            "Tower_ID":        tower[0],
            "Tower_Location":  tower[1],
            "Tower_Latitude":  tower[2],
            "Tower_Longitude": tower[3],
        })

    # Sort chronologically
    records.sort(key=lambda r: r["Date"] + r["Time"])
    for i, r in enumerate(records):
        r["SR"] = i + 1

    # Write CSV
    fieldnames = list(records[0].keys())
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    # Print summary
    unique_nums    = len(set([r["Calling_Number"] for r in records] + [r["Called_Number"] for r in records]))
    unique_towers  = len(set(r["Tower_ID"] for r in records))
    voice_count    = sum(1 for r in records if r["Call_Type"] == "Voice")
    sms_count      = sum(1 for r in records if r["Call_Type"] == "SMS")
    data_count     = sum(1 for r in records if r["Call_Type"] == "Data")

    print("\n" + "="*50)
    print("  CDR Dataset Generated Successfully")
    print("="*50)
    print(f"  Output file   : {output_file}")
    print(f"  Total records : {len(records)}")
    print(f"  Unique numbers: {unique_nums}")
    print(f"  Cell towers   : {unique_towers}")
    print(f"  Scenario      : {scenario}")
    print(f"  State/Region  : {state}")
    print(f"  Date range    : {records[0]['Date']} to {records[-1]['Date']}")
    print(f"  Call types    : Voice={voice_count}, SMS={sms_count}, Data={data_count}")
    if scenario == "burner":
        print(f"  Burner phones : {burner_phones}")
    print("="*50 + "\n")

    return records


# ──────────────────────────────────────────────
# CLI ENTRY POINT
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic CDR data for digital forensics practice.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_cdr.py
  python generate_cdr.py --records 500 --scenario gang --state DL
  python generate_cdr.py --records 1000 --scenario fraud --nodes 15 --output fraud_cdr.csv
  python generate_cdr.py --scenario burner --seed 42
        """
    )
    parser.add_argument("--records",  type=int,   default=100,              help="Number of CDR records (default: 100)")
    parser.add_argument("--nodes",    type=int,   default=8,                help="Number of unique phone numbers (default: 8)")
    parser.add_argument("--start",    type=str,   default="2024-01-01",     help="Start date YYYY-MM-DD (default: 2024-01-01)")
    parser.add_argument("--end",      type=str,   default="2024-03-31",     help="End date YYYY-MM-DD (default: 2024-03-31)")
    parser.add_argument("--scenario", type=str,   default="random",         choices=["random", "gang", "fraud", "burner"], help="Investigation scenario")
    parser.add_argument("--state",    type=str,   default="UP",             choices=["UP", "MH", "DL", "KA", "TN", "RJ", "PB", "ALL"], help="Indian state for cell towers (ALL = pan-India)")
    parser.add_argument("--output",   type=str,   default="fake_cdr_data.csv", help="Output CSV filename")
    parser.add_argument("--seed",     type=int,   default=None,             help="Random seed for reproducibility")

    args = parser.parse_args()

    generate_cdr(
        num_records = args.records,
        num_nodes   = args.nodes,
        start_date  = args.start,
        end_date    = args.end,
        scenario    = args.scenario,
        state       = args.state,
        output_file = args.output,
        seed        = args.seed,
    )


if __name__ == "__main__":
    main()