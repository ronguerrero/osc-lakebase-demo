# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Setup Lakehouse (Unity Catalog Delta Tables)
# MAGIC Creates a SQL warehouse, UC catalog/schema, and 10 OSC registrant-oversight tables with ~570K total records.
# MAGIC All resources are created under the identity of the executor.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakebase_demo")
dbutils.widgets.text("warehouse_name", "Serverless Starter Warehouse")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Setting up {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Find SQL Warehouse

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import ResourceExhausted, TooManyRequests
from databricks.sdk.service import sql

w = WorkspaceClient()
me = w.current_user.me()
print(f"Running as: {me.user_name}")

warehouse_name = dbutils.widgets.get("warehouse_name")  # an existing warehouse is reused
warehouse_id = None

# Check for existing warehouse
for wh in w.warehouses.list():
    if wh.name == warehouse_name:
        warehouse_id = wh.id
        print(f"Found existing warehouse: {warehouse_name} ({warehouse_id})")
        break

if not warehouse_id:
    print(f"Creating SQL warehouse: {warehouse_name}")
    # Short retry window: a workspace at its warehouse limit answers 429, which the SDK would otherwise
    # retry for 5 minutes and report as a timeout
    w_create = WorkspaceClient(config=Config(retry_timeout_seconds=60))
    try:
        wh = w_create.warehouses.create(
            name=warehouse_name,
            cluster_size="2X-Small",  # some workspaces reject larger sizes; plenty for this dataset
            max_num_clusters=1,
            auto_stop_mins=10,
            enable_photon=True,
            enable_serverless_compute=True,
            warehouse_type=sql.CreateWarehouseRequestWarehouseType.PRO,
        ).result()
    except (ResourceExhausted, TooManyRequests, TimeoutError) as e:
        existing = [x.name for x in w.warehouses.list()]
        raise RuntimeError(
            f"Could not create SQL warehouse '{warehouse_name}' (the workspace may be at its warehouse limit): {e}. "
            f"Set the bundle variable warehouse_name to an existing warehouse: {existing}") from e
    warehouse_id = wh.id
    print(f"Created warehouse: {warehouse_name} ({warehouse_id})")

# Pass warehouse_id to downstream tasks
dbutils.jobs.taskValues.set(key="warehouse_id", value=warehouse_id)
print(f"Warehouse ID: {warehouse_id}")

# COMMAND ----------

# Use the catalog if it exists (e.g. the workspace's default catalog); only create it when missing
if not spark.sql(f"SHOW CATALOGS LIKE '{catalog}'").collect():
    spark.sql(f"CREATE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Catalog and schema ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables

# COMMAND ----------

tables_sql = {
    "account_types": """
        CREATE TABLE IF NOT EXISTS account_types (
            account_type_id INT,
            type_name STRING,
            description STRING,
            is_registered_plan BOOLEAN,
            statement_frequency STRING
        ) USING DELTA
    """,
    "advisors": """
        CREATE TABLE IF NOT EXISTS advisors (
            advisor_id INT,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            nrd_number STRING,
            registration_date DATE,
            region STRING,
            is_active BOOLEAN
        ) USING DELTA
    """,
    "investors": """
        CREATE TABLE IF NOT EXISTS investors (
            investor_id INT,
            first_name STRING,
            last_name STRING,
            date_of_birth DATE,
            email STRING,
            phone STRING,
            address STRING,
            city STRING,
            province STRING,
            postal_code STRING,
            created_at TIMESTAMP
        ) USING DELTA
    """,
    "accounts": """
        CREATE TABLE IF NOT EXISTS accounts (
            account_id INT,
            investor_id INT,
            advisor_id INT,
            account_type_id INT,
            account_number STRING,
            open_date DATE,
            close_date DATE,
            market_value DECIMAL(12,2),
            status STRING,
            created_at TIMESTAMP
        ) USING DELTA
    """,
    "holdings": """
        CREATE TABLE IF NOT EXISTS holdings (
            holding_id INT,
            account_id INT,
            isin STRING,
            issuer STRING,
            security_name STRING,
            year INT,
            asset_class STRING,
            ticker STRING
        ) USING DELTA
    """,
    "beneficiaries": """
        CREATE TABLE IF NOT EXISTS beneficiaries (
            beneficiary_id INT,
            account_id INT,
            first_name STRING,
            last_name STRING,
            relationship STRING,
            percentage DECIMAL(5,2),
            date_of_birth DATE
        ) USING DELTA
    """,
    "kyc_profiles": """
        CREATE TABLE IF NOT EXISTS kyc_profiles (
            kyc_id INT,
            account_id INT,
            risk_tolerance STRING,
            net_worth DECIMAL(12,2),
            annual_income DECIMAL(10,2),
            assessment_date DATE
        ) USING DELTA
    """,
    "contributions": """
        CREATE TABLE IF NOT EXISTS contributions (
            contribution_id INT,
            account_id INT,
            amount DECIMAL(12,2),
            due_date DATE,
            settled_date DATE,
            payment_method STRING,
            status STRING
        ) USING DELTA
    """,
    "complaints": """
        CREATE TABLE IF NOT EXISTS complaints (
            complaint_id INT,
            account_id INT,
            complaint_number STRING,
            complaint_type STRING,
            incident_date DATE,
            report_date DATE,
            amount_claimed DECIMAL(12,2),
            status STRING,
            description STRING
        ) USING DELTA
    """,
    "restitution_payments": """
        CREATE TABLE IF NOT EXISTS restitution_payments (
            payment_id INT,
            complaint_id INT,
            amount DECIMAL(12,2),
            payment_date DATE,
            payment_type STRING,
            status STRING,
            notes STRING
        ) USING DELTA
    """,
}

for table_name, ddl in tables_sql.items():
    spark.sql(ddl)
    print(f"Created table: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate Data

# COMMAND ----------

from pyspark.sql import Row
from datetime import date, datetime, timedelta

# COMMAND ----------

# Check if already populated
row_count = spark.sql("SELECT COUNT(*) AS cnt FROM account_types").collect()[0]["cnt"]
if row_count > 0:
    print("Tables already populated, skipping data generation.")
    dbutils.notebook.exit("ALREADY_POPULATED")

# COMMAND ----------

# Account Types (10 rows)
account_types_data = [
    (1, "RRSP", "Registered Retirement Savings Plan", True, "QUARTERLY"),
    (2, "TFSA", "Tax-Free Savings Account", True, "QUARTERLY"),
    (3, "RRIF", "Registered Retirement Income Fund", True, "QUARTERLY"),
    (4, "RESP", "Registered Education Savings Plan", True, "ANNUALLY"),
    (5, "FHSA", "First Home Savings Account", True, "QUARTERLY"),
    (6, "LIRA", "Locked-In Retirement Account", True, "ANNUALLY"),
    (7, "Cash", "Non-registered cash account", False, "MONTHLY"),
    (8, "Margin", "Non-registered margin account", False, "MONTHLY"),
    (9, "Managed", "Discretionary managed account", False, "QUARTERLY"),
    (10, "Corporate", "Corporate investment account", False, "MONTHLY"),
]

df = spark.createDataFrame(account_types_data, ["account_type_id", "type_name", "description", "is_registered_plan", "statement_frequency"])
df.write.mode("append").saveAsTable("account_types")
print(f"account_types: {df.count()} rows")

# COMMAND ----------

# Advisors (500 rows)
fn = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
      'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
      'Thomas','Sarah','Charles','Karen']
ln = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
      'Rodriguez','Martinez','Anderson','Taylor','Thomas','Moore','Jackson','Martin',
      'Lee','Thompson','White','Harris','Clark','Lewis','Robinson','Walker','Young']
regions = ['Toronto','Ottawa','Hamilton','London','Kingston','Sudbury','Windsor','Mississauga']

advisors_data = []
for i in range(1, 501):
    advisors_data.append(Row(
        advisor_id=i, first_name=fn[i%20], last_name=ln[(i*7+3)%25],
        email=f"advisor{i}@ontariowealth.ca", phone=f"555-{str((i*37)%10000).zfill(4)}",
        nrd_number=f"NRD-{str(i).zfill(6)}",
        registration_date=date(2010,1,1)+timedelta(days=(i*13)%4000),
        region=regions[i%8], is_active=(i%10!=0)
    ))

spark.createDataFrame(advisors_data).write.mode("append").saveAsTable("advisors")
print(f"advisors: {len(advisors_data)} rows")

# COMMAND ----------

# Investors (50,000 rows)
fn2 = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
       'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
       'Thomas','Sarah','Charles','Karen','Daniel','Nancy','Matthew','Lisa',
       'Anthony','Betty','Mark','Margaret','Donald','Sandra']
ln2 = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
       'Rodriguez','Martinez','Anderson','Taylor','Thomas','Moore','Jackson','Martin',
       'Lee','Thompson','White','Harris','Clark','Lewis','Robinson','Walker',
       'Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill',
       'Flores','Green','Adams','Nelson','Baker','Hall','Rivera','Campbell']
streets = ['Yonge St','Bay St','King St W','Queen St E','Bloor St W','Dundas St','Front St','University Ave','Spadina Ave','Bank St']
cities = ['Toronto','Ottawa','Mississauga','Brampton','Hamilton','London','Markham','Vaughan',
          'Kitchener','Windsor','Oakville','Burlington','Oshawa','Barrie','Kingston']
states = ['ON']*15

investors_data = []
for i in range(1, 50001):
    investors_data.append(Row(
        investor_id=i, first_name=fn2[i%30], last_name=ln2[(i*7+3)%40],
        date_of_birth=date(1950,1,1)+timedelta(days=(i*31)%20000),
        email=f"investor{i}@email.ca", phone=f"555-{str((i*41)%10000).zfill(4)}",
        address=f"{(i*17)%9999+1} {streets[i%10]}", city=cities[i%15], province=states[i%15],
        postal_code=f"{'KLMNP'[i%5]}{i%10}{chr(65+(i*7)%26)} {(i*3)%10}{chr(65+(i*11)%26)}{(i*7)%10}",
        created_at=datetime(2015,1,1)+timedelta(days=i%3650)
    ))

spark.createDataFrame(investors_data).write.mode("append").saveAsTable("investors")
print(f"investors: {len(investors_data)} rows")

# COMMAND ----------

# Accounts (100,000 rows)
status_vals = ['ACTIVE','ACTIVE','ACTIVE','DORMANT','CLOSED']

accounts_data = []
for i in range(1, 100001):
    accounts_data.append(Row(
        account_id=i, investor_id=(i%50000)+1, advisor_id=(i%500)+1,
        account_type_id=(i%10)+1, account_number=f"ACC-{str(i).zfill(8)}",
        open_date=date(2018,1,1)+timedelta(days=i%2500),
        close_date=date(2018,1,1)+timedelta(days=(i%2500)+365),
        market_value=float(round(1000+(i*37)%249000, 2)),
        status=status_vals[i%5],
        created_at=datetime(2018,1,1)+timedelta(days=i%2500)
    ))

spark.createDataFrame(accounts_data).write.mode("append").saveAsTable("accounts")
print(f"accounts: {len(accounts_data)} rows")

# COMMAND ----------

# Holdings (60,000 rows)
makes = ['Royal Bank of Canada','Toronto-Dominion Bank','Enbridge','Shopify','Canadian National Railway','Brookfield Corp','Bank of Montreal','Suncor Energy','Manulife Financial','Thomson Reuters']
models = ['Common Shares','Common Shares','4.25% Senior Notes 2031','Class A Subordinate Voting','3.60% Debentures 2029','Cumulative Preferred Series 2','Covered Call Canadian Banks ETF','Common Shares','Monthly Income Fund Series F','Common Shares']
colors = ['Equity','Equity','Fixed Income','Equity','Fixed Income','Preferred Share','ETF','Equity','Mutual Fund','Equity']

holdings_data = []
for i in range(1, 60001):
    holdings_data.append(Row(
        holding_id=i, account_id=i, isin=f"CA{i:09d}{i%10}",
        issuer=makes[i%10], security_name=models[i%10], year=2015+(i%10),
        asset_class=colors[i%10],
        ticker=['RY','TD','ENB','SHOP','CNR','BN.PR.B','ZWB','SU','MFC','TRI'][i%10]+'.TO'
    ))

spark.createDataFrame(holdings_data).write.mode("append").saveAsTable("holdings")
print(f"holdings: {len(holdings_data)} rows")

# COMMAND ----------

# Beneficiaries (80,000 rows)
bfn = ['Emma','Liam','Olivia','Noah','Ava','Ethan','Sophia','Mason',
       'Isabella','Logan','Mia','Lucas','Charlotte','Aiden','Amelia']
bln = ['Smith','Johnson','Brown','Davis','Wilson','Moore','Taylor','Anderson',
       'Thomas','Jackson','White','Harris','Martin','Garcia','Martinez','Clark',
       'Lewis','Lee','Walker','Hall']
rels = ['Spouse','Child','Parent','Sibling','Other']

beneficiaries_data = []
for i in range(1, 80001):
    beneficiaries_data.append(Row(
        beneficiary_id=i, account_id=((i-1)%100000)+1,
        first_name=bfn[i%15], last_name=bln[(i*11)%20],
        relationship=rels[i%5], percentage=float(round(100.0/(1+(i%3)), 2)),
        date_of_birth=date(1960,1,1)+timedelta(days=(i*29)%18000)
    ))

spark.createDataFrame(beneficiaries_data).write.mode("append").saveAsTable("beneficiaries")
print(f"beneficiaries: {len(beneficiaries_data)} rows")

# COMMAND ----------

# KYC Profiles (100,000 rows)
risk_levels = ['Low','Low-Medium','Medium','Medium-High',
             'High','Medium','Low-Medium','Medium']

kyc_data = []
for i in range(1, 100001):
    kyc_data.append(Row(
        kyc_id=i, account_id=((i-1)%100000)+1,
        risk_tolerance=risk_levels[i%8],
        net_worth=float(round(50000+(i*43)%1950000, 2)),
        annual_income=float(round(30000+(i*17)%220000, 2)),
        assessment_date=date(2018,1,1)+timedelta(days=i%2500)
    ))

spark.createDataFrame(kyc_data).write.mode("append").saveAsTable("kyc_profiles")
print(f"kyc_profiles: {len(kyc_data)} rows")

# COMMAND ----------

# Contributions (100,000 rows)
methods = ['PAC','EFT','CHEQUE','WIRE']

contributions_data = []
for i in range(1, 100001):
    due = date(2018,1,1)+timedelta(days=(i*3)%2500)
    paid = None if i%8==0 else due+timedelta(days=i%15)
    status = 'OVERDUE' if i%8==0 else ('PENDING' if i%12==0 else 'PAID')
    contributions_data.append(Row(
        contribution_id=i, account_id=((i-1)%100000)+1,
        amount=float(round(100+(i*23)%6900, 2)),
        due_date=due, settled_date=paid,
        payment_method=methods[i%4], status=status
    ))

spark.createDataFrame(contributions_data).write.mode("append").saveAsTable("contributions")
print(f"contributions: {len(contributions_data)} rows")

# COMMAND ----------

# Complaints (80,000 rows)
complaint_types = ['Unsuitable Investment','Misrepresentation','Unauthorized Trading','Excessive Fees','Failure to Follow Instructions','Account Transfer Delay']
complaint_statuses = ['OPEN','UNDER_REVIEW','RESOLVED','DISMISSED','ESCALATED_TO_OBSI']
descs = ['Investor placed in high-risk product inconsistent with KYC','Advisor misstated fees and expected returns',
         'Trades executed without client authorization','Undisclosed trailing commissions charged',
         'Sell order not executed as instructed','RRSP transfer delayed beyond 60 days']

complaints_data = []
for i in range(1, 80001):
    complaints_data.append(Row(
        complaint_id=i, account_id=((i-1)%100000)+1,
        complaint_number=f"CMP-{str(i).zfill(8)}",
        complaint_type=complaint_types[i%6],
        incident_date=date(2019,1,1)+timedelta(days=i%2200),
        report_date=date(2019,1,1)+timedelta(days=(i%2200)+(i%30)),
        amount_claimed=float(round(500+(i*67)%49500, 2)),
        status=complaint_statuses[i%5], description=descs[i%6]
    ))

spark.createDataFrame(complaints_data).write.mode("append").saveAsTable("complaints")
print(f"complaints: {len(complaints_data)} rows")

# COMMAND ----------

# Restitution Payments (100,000 rows)
pay_types = ['COMPENSATION','PARTIAL','FINAL']
pay_statuses = ['PROCESSED','PENDING','COMPLETED','VOID']
notes = ['Initial compensation','Supplemental compensation','Final settlement','Adjusted amount','OBSI-recommended amount']

payments_data = []
for i in range(1, 100001):
    payments_data.append(Row(
        payment_id=i, complaint_id=((i-1)%80000)+1,
        amount=float(round(100+(i*31)%9900, 2)),
        payment_date=date(2019,3,1)+timedelta(days=i%2100),
        payment_type=pay_types[i%3], status=pay_statuses[i%4],
        notes=notes[i%5]
    ))

spark.createDataFrame(payments_data).write.mode("append").saveAsTable("restitution_payments")
print(f"restitution_payments: {len(payments_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print(f"\n{'='*50}")
print(f"Lakehouse Setup Complete: {catalog}.{schema}")
print(f"{'='*50}")
for t in ["account_types", "advisors", "investors", "accounts", "holdings",
          "beneficiaries", "kyc_profiles", "contributions", "complaints", "restitution_payments"]:
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {t}").collect()[0]["cnt"]
    print(f"  {t}: {cnt:,} rows")
