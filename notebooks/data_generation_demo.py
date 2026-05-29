# Databricks notebook source

# DBTITLE 1, Ironclad HR — Demo Data Generation
# Generates a stable deterministic HR population across multiple periods.
# Each period applies controlled changes: 5% department transfers, 2% role
# promotions, 1% terminations with replacement hires.
#
# This produces genuine SCD Type 2 history when the pipeline is run
# once per period. Run demo mode by triggering this notebook for each
# period in sequence (period 1, then 2, etc.) before each pipeline run.
#
# Bad records are injected at inject_bad_records_period.
# compensation_category column appears from schema_evolution_month onwards.

# COMMAND ----------

import random
import copy
import json
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("period_num", "1",
    "Which period number to generate (1=first, 2=second, etc.)")
dbutils.widgets.text("start_year", "2025")
dbutils.widgets.text("start_month", "1")
dbutils.widgets.text("num_employees", "500")
dbutils.widgets.text("catalog", "ironclad_hr")
dbutils.widgets.text("inject_bad_records_period", "2")
dbutils.widgets.text("schema_evolution_month", "3")

period_num = int(dbutils.widgets.get("period_num"))
start_year = int(dbutils.widgets.get("start_year"))
start_month = int(dbutils.widgets.get("start_month"))
num_employees = int(dbutils.widgets.get("num_employees"))
catalog = dbutils.widgets.get("catalog")
inject_bad_records_period = int(dbutils.widgets.get("inject_bad_records_period"))
schema_evolution_month = int(dbutils.widgets.get("schema_evolution_month"))

# Compute year/month for this period
month_offset = (start_month - 1) + (period_num - 1)
year = start_year + (month_offset // 12)
month = (month_offset % 12) + 1
period = f"{year}-{month:02d}"
period_date_str = f"{year}-{month:02d}-01"

inject_bad = (period_num == inject_bad_records_period)
include_comp_cat = (period_num >= schema_evolution_month)

landing_path = f"/Volumes/{catalog}/bronze/landing/{period}"

print(f"Period {period_num}: {period}")
print(f"Landing path: {landing_path}")
print(f"Bad records: {'YES' if inject_bad else 'no'}")
print(f"compensation_category: {'included' if include_comp_cat else 'absent'}")

# COMMAND ----------

# DBTITLE 1, Reference data

DEPT_IDS = ["D001","D002","D003","D004","D005","D006","D007","D008"]
ROLE_IDS = [
    "R001","R002","R003","R004","R005",
    "R006","R007","R008","R009","R010","R011","R012","R013"
]
GENDERS = ["Male","Female","Non-binary"]
GENDER_WEIGHTS = [0.45, 0.45, 0.10]
NATIONALITIES = ["British","American","French","German","Indian","Australian"]
ROLE_PROMOTIONS = {
    "R001": "R002",  # SE -> SSE
    "R002": "R003",  # SSE -> Staff
    "R004": "R005",  # PM -> Senior PM
    "R006": "R007",  # FA -> Senior FA
    "R009": "R010",  # AE -> Senior AE
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
    ("D001","Engineering","CC-ENG",None),
    ("D002","Product","CC-PRD",None),
    ("D003","Finance","CC-FIN",None),
    ("D004","HR","CC-HR",None),
    ("D005","Sales","CC-SAL",None),
    ("D006","Marketing","CC-MKT",None),
    ("D007","Operations","CC-OPS",None),
    ("D008","Legal","CC-LEG",None),
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

# COMMAND ----------

# DBTITLE 1, Population generation functions

def weighted_choice(rng, choices, weights):
    r = rng.random()
    cumulative = 0.0
    for choice, weight in zip(choices, weights):
        cumulative += weight
        if r <= cumulative:
            return choice
    return choices[-1]


def generate_base_population(n, seed=42):
    """
    Generates a deterministic base HR population of n employees.
    Same seed always produces identical output — essential for
    reproducibility across pipeline runs and environments.
    """
    rng = random.Random(seed)
    population = {}
    hire_start = date(2018, 1, 1)
    hire_end = date(2024, 12, 31)
    hire_range = (hire_end - hire_start).days

    for i in range(1, n + 1):
        emp_id = f"EMP{i:05d}"
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        hire_date = hire_start + timedelta(days=rng.randint(0, hire_range))
        manager_num = rng.randint(1, max(1, i - 1)) if i > 10 else rng.randint(1, n)
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


def apply_all_periods_up_to(n, target_period_num, period_date_str):
    """
    Generates base population then applies all period changes cumulatively
    up to and including target_period_num.
    Returns (population, next_emp_id_counter).
    """
    population = generate_base_population(n)
    next_id = n + 1

    for p in range(1, target_period_num + 1):
        month_off = (start_month - 1) + (p - 1)
        y = start_year + (month_off // 12)
        m = (month_off % 12) + 1
        p_date = f"{y}-{m:02d}-01"
        population, _, next_id = _apply_changes(population, p, next_id, p_date)

    return population, next_id


def _apply_changes(population, p_num, next_id_counter, p_date_str):
    """Apply controlled changes for period p_num. Mutates population in place."""
    rng = random.Random(42 + p_num * 7919)

    active = [e for e in population.values() if e["termination_date"] is None]
    n_active = len(active)

    promoted_ids = set()

    # 5% department transfers
    n_transfers = max(1, int(n_active * 0.05))
    for emp in rng.sample(active, n_transfers):
        new_dept = rng.choice([d for d in DEPT_IDS if d != emp["department_id"]])
        population[emp["employee_id"]]["department_id"] = new_dept

    # 2% promotions (role-ladder only)
    active = [e for e in population.values() if e["termination_date"] is None]
    promotable = [e for e in active if e["role_id"] in ROLE_PROMOTIONS]
    n_promotions = min(max(1, int(n_active * 0.02)), len(promotable))
    for emp in rng.sample(promotable, n_promotions):
        population[emp["employee_id"]]["role_id"] = ROLE_PROMOTIONS[emp["role_id"]]
        promoted_ids.add(emp["employee_id"])

    # 1% terminations (not recently promoted)
    active = [e for e in population.values() if e["termination_date"] is None]
    terminatable = [e for e in active if e["employee_id"] not in promoted_ids]
    n_terminations = min(max(1, int(n_active * 0.01)), len(terminatable))
    terminated = rng.sample(terminatable, n_terminations)
    for emp in terminated:
        population[emp["employee_id"]]["termination_date"] = p_date_str

    # New hires to replace terminations
    p_date = date.fromisoformat(p_date_str)
    for _ in range(len(terminated)):
        emp_id = f"EMP{next_id_counter:05d}"
        next_id_counter += 1
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        hire_date = p_date - timedelta(days=rng.randint(0, 28))
        population[emp_id] = {
            "employee_id": emp_id,
            "full_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{next_id_counter:03d}@ironcladhr.com",
            "gender": weighted_choice(rng, GENDERS, GENDER_WEIGHTS),
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "termination_date": None,
            "department_id": rng.choice(DEPT_IDS),
            "role_id": rng.choice(ROLE_IDS),
            "manager_id": f"EMP{rng.randint(1, 500):05d}",
            "nationality": rng.choice(NATIONALITIES),
        }

    return population, len(terminated), next_id_counter

# COMMAND ----------

# DBTITLE 1, Build population for this period

# Build population state as of this period (cumulative changes applied)
population, next_emp_id = apply_all_periods_up_to(
    num_employees, period_num, period_date_str
)

active_count = sum(1 for e in population.values() if e["termination_date"] is None)
print(f"Population state for period {period_num}: {len(population)} total, {active_count} active")

# COMMAND ----------

# DBTITLE 1, Write static reference data (departments and job roles)

dept_schema = StructType([
    StructField("department_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("cost_centre", StringType(), True),
    StructField("parent_department_id", StringType(), True),
])
dept_df = spark.createDataFrame(dept_data, schema=dept_schema)
dept_df.write.mode("overwrite").json(f"{landing_path}/departments/")
print(f"Departments written: {dept_df.count()} rows")

roles_schema = StructType([
    StructField("role_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("band", StringType(), True),
    StructField("function", StringType(), True),
    StructField("job_family", StringType(), True),
])
roles_df = spark.createDataFrame(roles_data, schema=roles_schema)
roles_df.write.mode("overwrite").json(f"{landing_path}/job_roles/")
print(f"Job roles written: {roles_df.count()} rows")

# COMMAND ----------

# DBTITLE 1, Write employee snapshot for this period

emp_rows = list(population.values())
emp_schema = StructType([
    StructField("employee_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("hire_date", StringType(), True),
    StructField("termination_date", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("role_id", StringType(), True),
    StructField("manager_id", StringType(), True),
    StructField("nationality", StringType(), True),
])
emp_df = spark.createDataFrame(
    [[r["employee_id"], r["full_name"], r["email"], r["gender"],
      r["hire_date"], r["termination_date"], r["department_id"],
      r["role_id"], r["manager_id"], r["nationality"]] for r in emp_rows],
    schema=emp_schema
)

# Inject bad records on configured period
if inject_bad:
    bad_rows = [
        # CRITICAL: null employee_id
        [None, "Bad Employee", "bad@ironcladhr.com", "Male",
         "2020-01-01", None, "D001", "R001", "EMP00001", "British"],
        # ERROR: termination_date before hire_date
        ["EMP99998", "Early Exit", "early@ironcladhr.com", "Female",
         "2023-06-01", "2022-01-01", "D002", "R002", "EMP00002", "American"],
    ]
    bad_df = spark.createDataFrame(bad_rows, schema=emp_schema)
    emp_df = emp_df.union(bad_df)
    print(f"  Injected 2 bad employee records (1 CRITICAL, 1 ERROR)")

emp_df.write.mode("overwrite").json(f"{landing_path}/employees/")
print(f"Employees written: {emp_df.count()} rows (active: {active_count})")

# COMMAND ----------

# DBTITLE 1, Write contracts (one per active employee)

rng_c = random.Random(42 + period_num * 31337)
contract_types = ["Permanent","Contractor","Fixed-Term"]
ct_weights = [0.70, 0.20, 0.10]

contract_rows = []
for emp in emp_rows:
    if emp["termination_date"] is None:
        ct = weighted_choice(rng_c, contract_types, ct_weights)
        # Use hire_date as contract start — stable per employee
        rng_stable = random.Random(hash(emp["employee_id"]) & 0xFFFFFF)
        ct_stable = weighted_choice(rng_stable, contract_types, ct_weights)
        contract_rows.append([
            f"CON{hash(emp['employee_id']) & 0xFFFFF:05d}",
            emp["employee_id"],
            ct_stable,
            emp["hire_date"],
            None,  # end_date null for active contracts
        ])

contract_schema = StructType([
    StructField("contract_id", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
])
contract_df = spark.createDataFrame(contract_rows, schema=contract_schema)
contract_df.write.mode("overwrite").json(f"{landing_path}/contracts/")
print(f"Contracts written: {contract_df.count()} rows")

# COMMAND ----------

# DBTITLE 1, Write compensation (one per active employee)

rng_comp = random.Random(42 + period_num * 54321)
comp_rows = []
for emp in emp_rows:
    if emp["termination_date"] is None:
        salary = rng_comp.randrange(30000, 200001, 1000)
        row = [
            emp["employee_id"],
            salary,
            "GBP",
            emp["hire_date"],
        ]
        if include_comp_cat:
            if salary < 50000:
                cat = "Band 1"
            elif salary < 80000:
                cat = "Band 2"
            elif salary < 120000:
                cat = "Band 3"
            else:
                cat = "Band 4"
            row.append(cat)
        comp_rows.append(row)

if include_comp_cat:
    comp_schema = StructType([
        StructField("employee_id", StringType(), True),
        StructField("salary", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("effective_date", StringType(), True),
        StructField("compensation_category", StringType(), True),
    ])
else:
    comp_schema = StructType([
        StructField("employee_id", StringType(), True),
        StructField("salary", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("effective_date", StringType(), True),
    ])

comp_df = spark.createDataFrame(comp_rows, schema=comp_schema)
comp_df.write.mode("overwrite").json(f"{landing_path}/compensation/")
print(f"Compensation written: {comp_df.count()} rows "
      f"({'with' if include_comp_cat else 'without'} compensation_category)")

# COMMAND ----------

# DBTITLE 1, Write payroll events (one per active employee)

rng_pay = random.Random(42 + period_num * 11111)
payroll_rows = []
for emp in emp_rows:
    if emp["termination_date"] is None:
        gross = rng_pay.randrange(2500, 16668, 100)
        deductions = rng_pay.randrange(500, 3001, 50)
        net = gross - deductions
        payroll_rows.append([
            emp["employee_id"],
            period,
            gross,
            deductions,
            net,
        ])

payroll_schema = StructType([
    StructField("employee_id", StringType(), True),
    StructField("period", StringType(), True),
    StructField("gross_pay", LongType(), True),
    StructField("deductions", LongType(), True),
    StructField("net_pay", LongType(), True),
])
payroll_df = spark.createDataFrame(payroll_rows, schema=payroll_schema)
payroll_df.write.mode("overwrite").json(f"{landing_path}/payroll_events/")
print(f"Payroll events written: {payroll_df.count()} rows")

# COMMAND ----------

print(f"\nPeriod {period_num} ({period}) generation complete.")
print(f"Landing path: {landing_path}")
print(f"Active employees: {active_count}")
print(f"Run the Bronze + Silver + Gold pipelines for this period, then run period {period_num + 1}.")
