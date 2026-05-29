# Databricks notebook source

# DBTITLE 1, Ironclad HR — Demo Orchestrator
# Runs the full demo end-to-end without manual intervention.
#
# For each period (1 to num_periods):
#   1. Generates HR data with controlled changes
#   2. Triggers the SDP pipeline (Bronze → Silver → Gold)
#   3. Waits for pipeline completion
#   4. Logs results and moves to the next period
#
# This produces genuine SCD Type 2 history because each period's
# snapshot is processed sequentially — exactly what AUTO CDC FROM
# SNAPSHOT requires to detect diffs.
#
# Parameters:
#   pipeline_id            — the SDP pipeline ID (find in Pipelines UI)
#   num_periods            — how many months to simulate (default 6)
#   start_year/start_month — first period to generate
#   num_employees          — base population size
#   inject_bad_records_period — which period gets bad records (0 = none)
#   schema_evolution_month — when compensation_category appears
#   catalog                — Unity Catalog catalog name

# COMMAND ----------

import time
import random
import json
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("pipeline_id", "", "SDP Pipeline ID (from Pipelines UI)")
dbutils.widgets.text("num_periods", "6")
dbutils.widgets.text("start_year", "2025")
dbutils.widgets.text("start_month", "1")
dbutils.widgets.text("num_employees", "500")
dbutils.widgets.text("catalog", "ironclad_hr")
dbutils.widgets.text("inject_bad_records_period", "2")
dbutils.widgets.text("schema_evolution_month", "3")

pipeline_id           = dbutils.widgets.get("pipeline_id")
num_periods           = int(dbutils.widgets.get("num_periods"))
start_year            = int(dbutils.widgets.get("start_year"))
start_month           = int(dbutils.widgets.get("start_month"))
num_employees         = int(dbutils.widgets.get("num_employees"))
catalog               = dbutils.widgets.get("catalog")
inject_bad_period     = int(dbutils.widgets.get("inject_bad_records_period"))
schema_evo_month      = int(dbutils.widgets.get("schema_evolution_month"))

if not pipeline_id:
    raise ValueError(
        "pipeline_id is required. Find it in Pipelines UI → your pipeline → Pipeline ID."
    )

print(f"Pipeline ID:  {pipeline_id}")
print(f"Periods:      {num_periods} (starting {start_year}-{start_month:02d})")
print(f"Employees:    {num_employees}")
print(f"Catalog:      {catalog}")
print(f"Bad records:  period {inject_bad_period} (0 = none)")
print(f"Schema evo:   period {schema_evo_month}")

# COMMAND ----------

# DBTITLE 1, Pipeline trigger + wait helpers

def trigger_pipeline_update(pipeline_id):
    """Trigger an incremental pipeline update and return the update_id."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    response = w.pipelines.start_update(pipeline_id=pipeline_id)
    return response.update_id


def wait_for_pipeline(pipeline_id, update_id, poll_interval_secs=20, timeout_secs=1800):
    """
    Poll the pipeline update until it reaches a terminal state.
    Returns the final state string.
    Raises on failure or timeout.
    """
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    elapsed = 0

    while elapsed < timeout_secs:
        update = w.pipelines.get_update(
            pipeline_id=pipeline_id,
            update_id=update_id
        ).update
        state = update.state.value if hasattr(update.state, "value") else str(update.state)

        if state in ("COMPLETED",):
            return state
        elif state in ("FAILED", "CANCELED", "STOPPING"):
            raise RuntimeError(
                f"Pipeline update {update_id} ended with state: {state}. "
                f"Check the Pipelines UI for details."
            )

        print(f"    [{elapsed}s] Pipeline state: {state} — waiting {poll_interval_secs}s...")
        time.sleep(poll_interval_secs)
        elapsed += poll_interval_secs

    raise TimeoutError(
        f"Pipeline update {update_id} did not complete within {timeout_secs}s."
    )

# COMMAND ----------

# DBTITLE 1, Data generation constants (shared with generation notebooks)

DEPT_IDS = ["D001","D002","D003","D004","D005","D006","D007","D008"]
ROLE_IDS = [
    "R001","R002","R003","R004","R005",
    "R006","R007","R008","R009","R010","R011","R012","R013"
]
GENDERS = ["Male","Female","Non-binary"]
GENDER_WEIGHTS = [0.45, 0.45, 0.10]
NATIONALITIES = ["British","American","French","German","Indian","Australian"]
ROLE_PROMOTIONS = {
    "R001":"R002","R002":"R003","R004":"R005","R006":"R007","R009":"R010"
}
FIRST_NAMES = [
    "James","Emma","Oliver","Sophia","William","Isabella","Henry","Charlotte",
    "George","Amelia","Thomas","Mia","Alexander","Grace","Michael","Lucy",
    "Daniel","Alice","Edward","Hannah","Liam","Chloe","Noah","Emily",
    "Ethan","Sarah","Aiden","Jessica","Olivia","Victoria","Harper","Avery",
    "Kai","Riley","Jordan","Morgan","Alex","Cameron","Sam","Taylor"
]
LAST_NAMES = [
    "Smith","Jones","Williams","Brown","Taylor","Davies","Evans","Wilson",
    "Thomas","Roberts","Johnson","Walker","Wright","Robinson","Thompson","White",
    "Hughes","Edwards","Green","Hall","Lewis","Harris","Clarke","Patel",
    "Jackson","Young","Scott","King","Turner","Baker","Mitchell","Phillips",
    "Campbell","Carter","Collins","Morgan","Murray","Peterson","Moore","Anderson"
]

dept_data = [
    ("D001","Engineering","CC-ENG",None),("D002","Product","CC-PRD",None),
    ("D003","Finance","CC-FIN",None),("D004","HR","CC-HR",None),
    ("D005","Sales","CC-SAL",None),("D006","Marketing","CC-MKT",None),
    ("D007","Operations","CC-OPS",None),("D008","Legal","CC-LEG",None),
]
roles_data = [
    ("R001","Software Engineer","IC3","Engineering","Technology"),
    ("R002","Senior Software Engineer","IC4","Engineering","Technology"),
    ("R003","Staff Engineer","IC5","Engineering","Technology"),
    ("R004","Product Manager","IC4","Product","Product"),
    ("R005","Senior Product Manager","IC5","Product","Product"),
    ("R006","Financial Analyst","IC3","Finance","Finance"),
    ("R007","Senior Financial Analyst","IC4","Finance","Finance"),
    ("R008","HR Business Partner","IC3","HR","People"),
    ("R009","Account Executive","IC3","Sales","Revenue"),
    ("R010","Senior Account Executive","IC4","Sales","Revenue"),
    ("R011","Marketing Manager","IC4","Marketing","Growth"),
    ("R012","Operations Analyst","IC3","Operations","Operations"),
    ("R013","Legal Counsel","IC4","Legal","Legal"),
]

def weighted_choice(rng, choices, weights):
    r = rng.random()
    cumulative = 0.0
    for c, w in zip(choices, weights):
        cumulative += w
        if r <= cumulative:
            return c
    return choices[-1]

def generate_base_population(n, seed=42):
    rng = random.Random(seed)
    population = {}
    hire_start = date(2018, 1, 1)
    hire_range = (date(2024, 12, 31) - hire_start).days
    for i in range(1, n + 1):
        emp_id = f"EMP{i:05d}"
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        hire_date = hire_start + timedelta(days=rng.randint(0, hire_range))
        manager_num = rng.randint(1, max(1, i-1)) if i > 10 else rng.randint(1, n)
        population[emp_id] = {
            "employee_id": emp_id,
            "full_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i:03d}@ironcladhr.com",
            "gender": weighted_choice(rng, GENDERS, GENDER_WEIGHTS),
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "termination_date": None,
            "department_id": rng.choice(DEPT_IDS),
            "role_id": rng.choice(ROLE_IDS),
            "manager_id": f"EMP{manager_num:05d}",
            "nationality": rng.choice(NATIONALITIES),
        }
    return population

def apply_period_changes(population, period_num, period_date_str, next_id):
    rng = random.Random(42 + period_num * 7919)
    active = [e for e in population.values() if e["termination_date"] is None]
    n_active = len(active)
    promoted = set()

    for emp in rng.sample(active, max(1, int(n_active * 0.05))):
        population[emp["employee_id"]]["department_id"] = rng.choice(
            [d for d in DEPT_IDS if d != emp["department_id"]])

    active = [e for e in population.values() if e["termination_date"] is None]
    promotable = [e for e in active if e["role_id"] in ROLE_PROMOTIONS]
    for emp in rng.sample(promotable, min(max(1, int(n_active * 0.02)), len(promotable))):
        population[emp["employee_id"]]["role_id"] = ROLE_PROMOTIONS[emp["role_id"]]
        promoted.add(emp["employee_id"])

    active = [e for e in population.values() if e["termination_date"] is None]
    terminatable = [e for e in active if e["employee_id"] not in promoted]
    n_term = min(max(1, int(n_active * 0.01)), len(terminatable))
    terminated = rng.sample(terminatable, n_term)
    for emp in terminated:
        population[emp["employee_id"]]["termination_date"] = period_date_str

    p_date = date.fromisoformat(period_date_str)
    for _ in range(len(terminated)):
        emp_id = f"EMP{next_id:05d}"
        next_id += 1
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        hire_date = p_date - timedelta(days=rng.randint(0, 28))
        population[emp_id] = {
            "employee_id": emp_id,
            "full_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{next_id:03d}@ironcladhr.com",
            "gender": weighted_choice(rng, GENDERS, GENDER_WEIGHTS),
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "termination_date": None,
            "department_id": rng.choice(DEPT_IDS),
            "role_id": rng.choice(ROLE_IDS),
            "manager_id": f"EMP{rng.randint(1, 500):05d}",
            "nationality": rng.choice(NATIONALITIES),
        }
    return population, next_id

# COMMAND ----------

# DBTITLE 1, Write one period's data to the landing volume

def write_period(population, period_num, period, landing_path, inject_bad, include_comp_cat):
    emp_rows = list(population.values())
    active_count = sum(1 for e in emp_rows if e["termination_date"] is None)

    dept_schema = StructType([
        StructField("department_id",StringType(),True),StructField("name",StringType(),True),
        StructField("cost_centre",StringType(),True),StructField("parent_department_id",StringType(),True),
    ])
    spark.createDataFrame(dept_data, schema=dept_schema)\
         .write.mode("overwrite").json(f"{landing_path}/departments/")

    roles_schema = StructType([
        StructField("role_id",StringType(),True),StructField("title",StringType(),True),
        StructField("band",StringType(),True),StructField("function",StringType(),True),
        StructField("job_family",StringType(),True),
    ])
    spark.createDataFrame(roles_data, schema=roles_schema)\
         .write.mode("overwrite").json(f"{landing_path}/job_roles/")

    emp_schema = StructType([
        StructField("employee_id",StringType(),True),StructField("full_name",StringType(),True),
        StructField("email",StringType(),True),StructField("gender",StringType(),True),
        StructField("hire_date",StringType(),True),StructField("termination_date",StringType(),True),
        StructField("department_id",StringType(),True),StructField("role_id",StringType(),True),
        StructField("manager_id",StringType(),True),StructField("nationality",StringType(),True),
    ])
    emp_df = spark.createDataFrame(
        [[r["employee_id"],r["full_name"],r["email"],r["gender"],
          r["hire_date"],r["termination_date"],r["department_id"],
          r["role_id"],r["manager_id"],r["nationality"]] for r in emp_rows],
        schema=emp_schema
    )
    if inject_bad:
        bad_df = spark.createDataFrame([
            [None,"Bad Employee","bad@ironcladhr.com","Male","2020-01-01",None,"D001","R001","EMP00001","British"],
            ["EMP99998","Early Exit","early@ironcladhr.com","Female","2023-06-01","2022-01-01","D002","R002","EMP00002","American"],
        ], schema=emp_schema)
        emp_df = emp_df.union(bad_df)
        print(f"    Bad records injected (1 CRITICAL null employee_id, 1 ERROR invalid termination_date)")

    emp_df.write.mode("overwrite").json(f"{landing_path}/employees/")

    rng_c = random.Random(42 + period_num * 31337)
    contract_types = ["Permanent","Contractor","Fixed-Term"]
    ct_weights = [0.70, 0.20, 0.10]
    contract_rows = []
    for emp in emp_rows:
        if emp["termination_date"] is None:
            rng_stable = random.Random(hash(emp["employee_id"]) & 0xFFFFFF)
            ct = weighted_choice(rng_stable, contract_types, ct_weights)
            contract_rows.append([
                f"CON{hash(emp['employee_id'])&0xFFFFF:05d}",
                emp["employee_id"], ct, emp["hire_date"], None
            ])
    contract_schema = StructType([
        StructField("contract_id",StringType(),True),StructField("employee_id",StringType(),True),
        StructField("contract_type",StringType(),True),StructField("start_date",StringType(),True),
        StructField("end_date",StringType(),True),
    ])
    spark.createDataFrame(contract_rows, schema=contract_schema)\
         .write.mode("overwrite").json(f"{landing_path}/contracts/")

    rng_comp = random.Random(42 + period_num * 54321)
    comp_rows = []
    for emp in emp_rows:
        if emp["termination_date"] is None:
            salary = rng_comp.randrange(30000, 200001, 1000)
            row = [emp["employee_id"], salary, "GBP", emp["hire_date"]]
            if include_comp_cat:
                cat = "Band 1" if salary < 50000 else "Band 2" if salary < 80000 else "Band 3" if salary < 120000 else "Band 4"
                row.append(cat)
            comp_rows.append(row)
    if include_comp_cat:
        comp_schema = StructType([
            StructField("employee_id",StringType(),True),StructField("salary",LongType(),True),
            StructField("currency",StringType(),True),StructField("effective_date",StringType(),True),
            StructField("compensation_category",StringType(),True),
        ])
    else:
        comp_schema = StructType([
            StructField("employee_id",StringType(),True),StructField("salary",LongType(),True),
            StructField("currency",StringType(),True),StructField("effective_date",StringType(),True),
        ])
    spark.createDataFrame(comp_rows, schema=comp_schema)\
         .write.mode("overwrite").json(f"{landing_path}/compensation/")

    rng_pay = random.Random(42 + period_num * 11111)
    payroll_rows = []
    for emp in emp_rows:
        if emp["termination_date"] is None:
            gross = rng_pay.randrange(2500, 16668, 100)
            deductions = rng_pay.randrange(500, 3001, 50)
            payroll_rows.append([emp["employee_id"], period, gross, deductions, gross - deductions])
    payroll_schema = StructType([
        StructField("employee_id",StringType(),True),StructField("period",StringType(),True),
        StructField("gross_pay",LongType(),True),StructField("deductions",LongType(),True),
        StructField("net_pay",LongType(),True),
    ])
    spark.createDataFrame(payroll_rows, schema=payroll_schema)\
         .write.mode("overwrite").json(f"{landing_path}/payroll_events/")

    return active_count

# COMMAND ----------

# DBTITLE 1, Main loop — generate → trigger → wait → repeat

population = generate_base_population(num_employees)
next_id = num_employees + 1
results = []

for period_num in range(1, num_periods + 1):
    month_offset = (start_month - 1) + (period_num - 1)
    year  = start_year + (month_offset // 12)
    month = (month_offset % 12) + 1
    period = f"{year}-{month:02d}"
    period_date_str = f"{year}-{month:02d}-01"
    landing_path = f"/Volumes/{catalog}/bronze/landing/{period}"

    inject_bad    = (period_num == inject_bad_period)
    include_comp  = (period_num >= schema_evo_month)

    print(f"\n{'='*60}")
    print(f"PERIOD {period_num}/{num_periods}: {period}")
    print(f"{'='*60}")

    # Apply changes for this period (except period 1 — base population)
    if period_num > 1:
        population, next_id = apply_period_changes(
            population, period_num, period_date_str, next_id
        )

    # Write data to landing volume
    print(f"  Generating data → {landing_path}")
    active_count = write_period(
        population, period_num, period, landing_path,
        inject_bad=inject_bad,
        include_comp_cat=include_comp
    )
    print(f"  Written: {active_count} active employees"
          f"{' + schema evolution (compensation_category)' if include_comp else ''}")

    # Trigger pipeline
    print(f"  Triggering pipeline {pipeline_id}...")
    update_id = trigger_pipeline_update(pipeline_id)
    print(f"  Update ID: {update_id}")

    # Wait for completion
    print(f"  Waiting for pipeline to complete...")
    t0 = time.time()
    final_state = wait_for_pipeline(pipeline_id, update_id)
    elapsed = int(time.time() - t0)
    print(f"  Pipeline completed: {final_state} ({elapsed}s)")

    results.append({
        "period_num": period_num,
        "period": period,
        "active_employees": active_count,
        "pipeline_state": final_state,
        "duration_secs": elapsed,
        "inject_bad_records": inject_bad,
        "schema_evolution": include_comp,
    })

# COMMAND ----------

# DBTITLE 1, Summary

print(f"\n{'='*60}")
print("DEMO ORCHESTRATION COMPLETE")
print(f"{'='*60}")
print(f"{'Period':<12} {'Active':>8} {'State':<12} {'Duration':>10}  {'Notes'}")
print(f"{'-'*60}")
for r in results:
    notes = []
    if r["inject_bad_records"]:
        notes.append("bad records injected")
    if r["schema_evolution"]:
        notes.append("compensation_category added")
    print(
        f"{r['period']:<12} {r['active_employees']:>8} "
        f"{r['pipeline_state']:<12} {r['duration_secs']:>8}s  "
        f"{', '.join(notes)}"
    )

print(f"\nSCD Type 2 history should now exist across {num_periods} periods.")
print(f"Check: SELECT COUNT(*) FROM {catalog}.silver.dim_employee WHERE __END_AT IS NOT NULL")
