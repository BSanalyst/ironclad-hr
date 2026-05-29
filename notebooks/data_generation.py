# Databricks notebook source

# DBTITLE 1, Ironclad HR — Ongoing Data Generation
# Generates HR data for a single period using the stable deterministic
# population model. This is the production monthly schedule notebook.
# Each month, it generates the current period's snapshot with all
# cumulative changes applied up to this period.

# COMMAND ----------

import random
import copy
import json
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("year", "2025")
dbutils.widgets.text("month", "1")
dbutils.widgets.text("num_employees", "500")
dbutils.widgets.text("catalog", "ironclad_hr")
dbutils.widgets.text("base_year", "2025")
dbutils.widgets.text("base_month", "1")

year = int(dbutils.widgets.get("year"))
month = int(dbutils.widgets.get("month"))
num_employees = int(dbutils.widgets.get("num_employees"))
catalog = dbutils.widgets.get("catalog")
base_year = int(dbutils.widgets.get("base_year"))
base_month = int(dbutils.widgets.get("base_month"))

period = f"{year}-{month:02d}"
period_date_str = f"{year}-{month:02d}-01"
landing_path = f"/Volumes/{catalog}/bronze/landing/{period}"

# Compute which period number this is relative to the base
month_offset = (year - base_year) * 12 + (month - base_month)
period_num = month_offset + 1

include_comp_cat = (period_num >= 3)  # schema evolves at period 3

print(f"Generating period {period_num}: {period}")
print(f"Landing path: {landing_path}")
print(f"compensation_category: {'included' if include_comp_cat else 'absent'}")

# COMMAND ----------

# DBTITLE 1, Shared constants and helpers (same as demo notebook)

DEPT_IDS = ["D001","D002","D003","D004","D005","D006","D007","D008"]
ROLE_IDS = [
    "R001","R002","R003","R004","R005",
    "R006","R007","R008","R009","R010","R011","R012","R013"
]
GENDERS = ["Male","Female","Non-binary"]
GENDER_WEIGHTS = [0.45, 0.45, 0.10]
NATIONALITIES = ["British","American","French","German","Indian","Australian"]
ROLE_PROMOTIONS = {
    "R001": "R002", "R002": "R003", "R004": "R005",
    "R006": "R007", "R009": "R010",
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
    ("D001","Engineering","CC-ENG",None), ("D002","Product","CC-PRD",None),
    ("D003","Finance","CC-FIN",None), ("D004","HR","CC-HR",None),
    ("D005","Sales","CC-SAL",None), ("D006","Marketing","CC-MKT",None),
    ("D007","Operations","CC-OPS",None), ("D008","Legal","CC-LEG",None),
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
    for choice, weight in zip(choices, weights):
        cumulative += weight
        if r <= cumulative:
            return choice
    return choices[-1]

def generate_base_population(n, seed=42):
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

def apply_all_periods_up_to(n, target_period_num):
    population = generate_base_population(n)
    next_id = n + 1
    for p in range(1, target_period_num + 1):
        month_off = (base_month - 1) + (p - 1)
        y = base_year + (month_off // 12)
        m = (month_off % 12) + 1
        p_date = f"{y}-{m:02d}-01"
        rng = random.Random(42 + p * 7919)
        active = [e for e in population.values() if e["termination_date"] is None]
        n_active = len(active)
        promoted = set()
        # Transfers
        for emp in rng.sample(active, max(1, int(n_active * 0.05))):
            population[emp["employee_id"]]["department_id"] = rng.choice(
                [d for d in DEPT_IDS if d != emp["department_id"]])
        # Promotions
        active = [e for e in population.values() if e["termination_date"] is None]
        promotable = [e for e in active if e["role_id"] in ROLE_PROMOTIONS]
        for emp in rng.sample(promotable, min(max(1, int(n_active * 0.02)), len(promotable))):
            population[emp["employee_id"]]["role_id"] = ROLE_PROMOTIONS[emp["role_id"]]
            promoted.add(emp["employee_id"])
        # Terminations
        active = [e for e in population.values() if e["termination_date"] is None]
        terminatable = [e for e in active if e["employee_id"] not in promoted]
        n_term = min(max(1, int(n_active * 0.01)), len(terminatable))
        terminated = rng.sample(terminatable, n_term)
        for emp in terminated:
            population[emp["employee_id"]]["termination_date"] = p_date
        # New hires
        p_date_obj = date.fromisoformat(p_date)
        for _ in range(len(terminated)):
            emp_id = f"EMP{next_id:05d}"
            next_id += 1
            first = rng.choice(FIRST_NAMES)
            last = rng.choice(LAST_NAMES)
            hire_date = p_date_obj - timedelta(days=rng.randint(0, 28))
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
    return population

# COMMAND ----------

# DBTITLE 1, Build population state for this period

population = apply_all_periods_up_to(num_employees, period_num)
active_count = sum(1 for e in population.values() if e["termination_date"] is None)
print(f"Population state: {len(population)} total, {active_count} active")

# COMMAND ----------

# DBTITLE 1, Write all entities

dept_schema = StructType([
    StructField("department_id",StringType(),True),StructField("name",StringType(),True),
    StructField("cost_centre",StringType(),True),StructField("parent_department_id",StringType(),True),
])
spark.createDataFrame(dept_data,schema=dept_schema).write.mode("overwrite").json(f"{landing_path}/departments/")

roles_schema = StructType([
    StructField("role_id",StringType(),True),StructField("title",StringType(),True),
    StructField("band",StringType(),True),StructField("function",StringType(),True),
    StructField("job_family",StringType(),True),
])
spark.createDataFrame(roles_data,schema=roles_schema).write.mode("overwrite").json(f"{landing_path}/job_roles/")

emp_rows = list(population.values())
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
emp_df.write.mode("overwrite").json(f"{landing_path}/employees/")
print(f"Employees written: {emp_df.count()} rows")

rng_c = random.Random(42 + period_num * 31337)
contract_types = ["Permanent","Contractor","Fixed-Term"]
ct_weights = [0.70, 0.20, 0.10]
contract_rows = []
for emp in emp_rows:
    if emp["termination_date"] is None:
        rng_stable = random.Random(hash(emp["employee_id"]) & 0xFFFFFF)
        ct = weighted_choice(rng_stable, contract_types, ct_weights)
        contract_rows.append([f"CON{hash(emp['employee_id'])&0xFFFFF:05d}",
                               emp["employee_id"],ct,emp["hire_date"],None])
contract_schema = StructType([
    StructField("contract_id",StringType(),True),StructField("employee_id",StringType(),True),
    StructField("contract_type",StringType(),True),StructField("start_date",StringType(),True),
    StructField("end_date",StringType(),True),
])
spark.createDataFrame(contract_rows,schema=contract_schema).write.mode("overwrite").json(f"{landing_path}/contracts/")
print(f"Contracts written: {len(contract_rows)} rows")

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
spark.createDataFrame(comp_rows,schema=comp_schema).write.mode("overwrite").json(f"{landing_path}/compensation/")
print(f"Compensation written: {len(comp_rows)} rows")

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
spark.createDataFrame(payroll_rows,schema=payroll_schema).write.mode("overwrite").json(f"{landing_path}/payroll_events/")
print(f"Payroll events written: {len(payroll_rows)} rows")

print(f"\nPeriod {period_num} ({period}) generation complete. All files at {landing_path}")
