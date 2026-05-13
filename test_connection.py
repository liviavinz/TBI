"""Throwaway smoke test for the refactored connection_tbi + schema."""
from connection_tbi import TBIConnection
from schema import ENTITIES, filter_entities, PATIENTS, ADMISSIONS, GCS_SCORE

conn = TBIConnection()

# 1. Can we connect at all?
print("\n[1] Testing connection...")
c = conn.get_connection()
assert c is not None, "Connection failed — check .env and VPN"
c.close()
print("    ✓ Connection works")

# 2. Do the filters run and return PatientIDs?
print("\n[2] Testing filters...")
for f in filter_entities():
    df = conn.get_data(f.source_query)
    if df is None:
        print(f"    ✗ {f.name}: query failed")
    else:
        print(f"    ✓ {f.name}: {len(df)} patients")

# 3. Does the cohort intersection work?
print("\n[3] Testing cohort intersection...")
cohort = conn.get_cohort()
print(f"    ✓ Final cohort size: {len(cohort)}")

# 4. Can we fetch transactional entities restricted to the cohort?
print("\n[4] Testing fetch() with cohort...")
for entity in [PATIENTS, ADMISSIONS, GCS_SCORE]:
    df = conn.fetch(entity, patient_ids=cohort)
    if df is None:
        print(f"    ✗ {entity.name}: fetch failed")
    else:
        print(f"    ✓ {entity.name}: {len(df)} rows, columns = {list(df.columns)}")

print("\nDone.")