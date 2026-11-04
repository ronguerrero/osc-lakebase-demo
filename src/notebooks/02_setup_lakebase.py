# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Setup Lakebase Autoscaling
# MAGIC Creates the Lakebase Autoscaling project, database, schema, tables, and populates with matching data.
# MAGIC All resources are created under the identity of the executor.

# COMMAND ----------

# MAGIC %md
# MAGIC Plain `psycopg` (pure-Python mode over the system libpq), not `psycopg[binary]`: the binary wheel's
# MAGIC bundled libpq aborts the process (SIGABRT) on ARM serverless hosts, e.g. Databricks Free Edition.

# COMMAND ----------

# MAGIC %pip install "psycopg>=3.0" "databricks-sdk>=0.81.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("lakebase_project", "osc-lakebase-demo")
dbutils.widgets.text("lakebase_db", "osc_demo")

project_name = dbutils.widgets.get("lakebase_project")
lakebase_db = dbutils.widgets.get("lakebase_db")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakebase Project (if not exists)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.postgres import Project, ProjectSpec

w = WorkspaceClient()
me = w.current_user.me()
print(f"Running as: {me.user_name}")

try:
    project = w.postgres.get_project(name=f"projects/{project_name}")
    print(f"Project already exists: {project.name}")
except NotFound:
    print(f"Creating Lakebase Autoscaling project: {project_name}")
    op = w.postgres.create_project(
        project=Project(
            spec=ProjectSpec(
                display_name=project_name,
                pg_version="17"
            )
        ),
        project_id=project_name
    )
    project = op.wait()
    print(f"Created project: {project.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Endpoint Host and Resource Name

# COMMAND ----------

endpoints = list(w.postgres.list_endpoints(
    parent=f"projects/{project_name}/branches/production"
))
if not endpoints:
    raise ValueError("No endpoints found. The project may still be provisioning.")

lakebase_host = endpoints[0].status.hosts.host
lakebase_endpoint = endpoints[0].name
print(f"Endpoint host: {lakebase_host}")
print(f"Endpoint name: {lakebase_endpoint}")

# Pass discovered values to downstream tasks
dbutils.jobs.taskValues.set(key="lakebase_host", value=lakebase_host)
dbutils.jobs.taskValues.set(key="lakebase_endpoint", value=lakebase_endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database and Schema

# COMMAND ----------

import psycopg
import socket

def get_connection(db_name="databricks_postgres"):
    """Connect to Lakebase using the executor's OAuth token."""
    cred = w.postgres.generate_database_credential(endpoint=lakebase_endpoint)
    username = me.user_name or me.display_name
    try:
        ip = socket.gethostbyname(lakebase_host)
    except Exception:
        ip = lakebase_host
    return psycopg.connect(
        host=lakebase_host, hostaddr=ip, dbname=db_name,
        user=username, password=cred.token, sslmode="require",
    )

conn = get_connection("databricks_postgres")
conn.autocommit = True
cur = conn.cursor()
cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{lakebase_db}'")
if not cur.fetchone():
    cur.execute(f"CREATE DATABASE {lakebase_db}")
    print(f"Created database: {lakebase_db}")
else:
    print(f"Database already exists: {lakebase_db}")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables

# COMMAND ----------

conn = get_connection(lakebase_db)
cur = conn.cursor()

cur.execute("CREATE SCHEMA IF NOT EXISTS lakebase_demo")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.account_types (
    account_type_id INT PRIMARY KEY, type_name TEXT NOT NULL, description TEXT,
    is_registered_plan BOOLEAN, statement_frequency TEXT)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.advisors (
    advisor_id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
    email TEXT, phone TEXT, nrd_number TEXT, registration_date DATE, region TEXT, is_active BOOLEAN)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.investors (
    investor_id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
    date_of_birth DATE, email TEXT, phone TEXT, address TEXT, city TEXT, province TEXT,
    postal_code TEXT, created_at TIMESTAMP)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.accounts (
    account_id INT PRIMARY KEY,
    investor_id INT NOT NULL REFERENCES lakebase_demo.investors(investor_id),
    advisor_id INT NOT NULL REFERENCES lakebase_demo.advisors(advisor_id),
    account_type_id INT NOT NULL REFERENCES lakebase_demo.account_types(account_type_id),
    account_number TEXT NOT NULL, open_date DATE NOT NULL, close_date DATE,
    market_value NUMERIC(12,2), status TEXT, created_at TIMESTAMP)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.holdings (
    holding_id INT PRIMARY KEY, account_id INT NOT NULL, isin TEXT, issuer TEXT,
    security_name TEXT, year INT, asset_class TEXT, ticker TEXT)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.beneficiaries (
    beneficiary_id INT PRIMARY KEY, account_id INT NOT NULL, first_name TEXT NOT NULL,
    last_name TEXT NOT NULL, relationship TEXT, percentage NUMERIC(5,2), date_of_birth DATE)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.kyc_profiles (
    kyc_id INT PRIMARY KEY, account_id INT NOT NULL, risk_tolerance TEXT NOT NULL,
    net_worth NUMERIC(12,2), annual_income NUMERIC(10,2), assessment_date DATE)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.contributions (
    contribution_id INT PRIMARY KEY, account_id INT NOT NULL, amount NUMERIC(12,2) NOT NULL,
    due_date DATE, settled_date DATE, payment_method TEXT, status TEXT)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.complaints (
    complaint_id INT PRIMARY KEY, account_id INT NOT NULL, complaint_number TEXT NOT NULL,
    complaint_type TEXT, incident_date DATE, report_date DATE, amount_claimed NUMERIC(12,2),
    status TEXT, description TEXT)""")

cur.execute("""CREATE TABLE IF NOT EXISTS lakebase_demo.restitution_payments (
    payment_id INT PRIMARY KEY, complaint_id INT NOT NULL, amount NUMERIC(12,2) NOT NULL,
    payment_date DATE, payment_type TEXT, status TEXT, notes TEXT)""")

conn.commit()
conn.close()
print("All Lakebase tables created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate Data

# COMMAND ----------

from datetime import date, timedelta, datetime

def populate_batch(table, num_columns, generator, total, batch_size=5000):
    """Insert data in batches using the executor's identity."""
    conn = get_connection(lakebase_db)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM lakebase_demo.{table}")
    if cur.fetchone()[0] > 0:
        print(f"  {table} already populated, skipping")
        conn.close()
        return

    placeholders = ",".join(["%s"] * num_columns)
    insert_sql = f"INSERT INTO lakebase_demo.{table} VALUES ({placeholders})"
    batch, inserted = [], 0

    for row in generator:
        batch.append(row)
        if len(batch) >= batch_size:
            cur.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
            print(f"  {table}: {inserted}/{total}")
            batch = []
    if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()
        inserted += len(batch)
    conn.close()
    print(f"  {table}: {inserted}/{total} DONE")

# COMMAND ----------

# Account Types
populate_batch("account_types", 5, [
    (1,'RRSP','Registered Retirement Savings Plan',True,'QUARTERLY'),
    (2,'TFSA','Tax-Free Savings Account',True,'QUARTERLY'),
    (3,'RRIF','Registered Retirement Income Fund',True,'QUARTERLY'),
    (4,'RESP','Registered Education Savings Plan',True,'ANNUALLY'),
    (5,'FHSA','First Home Savings Account',True,'QUARTERLY'),
    (6,'LIRA','Locked-In Retirement Account',True,'ANNUALLY'),
    (7,'Cash','Non-registered cash account',False,'MONTHLY'),
    (8,'Margin','Non-registered margin account',False,'MONTHLY'),
    (9,'Managed','Discretionary managed account',False,'QUARTERLY'),
    (10,'Corporate','Corporate investment account',False,'MONTHLY'),
], 10, batch_size=10)

# COMMAND ----------

# Advisors (500)
def gen_advisors():
    fn = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
          'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
          'Thomas','Sarah','Charles','Karen']
    ln = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
          'Rodriguez','Martinez','Anderson','Taylor','Thomas','Moore','Jackson','Martin',
          'Lee','Thompson','White','Harris','Clark','Lewis','Robinson','Walker','Young']
    regions = ['Toronto','Ottawa','Hamilton','London','Kingston','Sudbury','Windsor','Mississauga']
    for i in range(1, 501):
        yield (i, fn[i%20], ln[(i*7+3)%25], f'advisor{i}@ontariowealth.ca',
               f'555-{str((i*37)%10000).zfill(4)}', f'NRD-{str(i).zfill(6)}',
               date(2010,1,1)+timedelta(days=(i*13)%4000), regions[i%8], i%10!=0)

populate_batch("advisors", 9, gen_advisors(), 500)

# COMMAND ----------

# Investors (50,000)
def gen_investors():
    fn = ['James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
          'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
          'Thomas','Sarah','Charles','Karen','Daniel','Nancy','Matthew','Lisa',
          'Anthony','Betty','Mark','Margaret','Donald','Sandra']
    ln = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
          'Rodriguez','Martinez','Anderson','Taylor','Thomas','Moore','Jackson','Martin',
          'Lee','Thompson','White','Harris','Clark','Lewis','Robinson','Walker',
          'Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill',
          'Flores','Green','Adams','Nelson','Baker','Hall','Rivera','Campbell']
    streets = ['Yonge St','Bay St','King St W','Queen St E','Bloor St W','Dundas St','Front St','University Ave','Spadina Ave','Bank St']
    cities = ['Toronto','Ottawa','Mississauga','Brampton','Hamilton','London','Markham','Vaughan',
              'Kitchener','Windsor','Oakville','Burlington','Oshawa','Barrie','Kingston']
    states = ['ON']*15
    for i in range(1, 50001):
        yield (i, fn[i%30], ln[(i*7+3)%40], date(1950,1,1)+timedelta(days=(i*31)%20000),
               f'investor{i}@email.ca', f'555-{str((i*41)%10000).zfill(4)}',
               f'{(i*17)%9999+1} {streets[i%10]}', cities[i%15], states[i%15],
               f"{'KLMNP'[i%5]}{i%10}{chr(65+(i*7)%26)} {(i*3)%10}{chr(65+(i*11)%26)}{(i*7)%10}", datetime(2015,1,1)+timedelta(days=i%3650))

populate_batch("investors", 11, gen_investors(), 50000)

# COMMAND ----------

# Accounts (100,000)
def gen_accounts():
    st = ['ACTIVE','ACTIVE','ACTIVE','DORMANT','CLOSED']
    for i in range(1, 100001):
        yield (i, (i%50000)+1, (i%500)+1, (i%10)+1, f'ACC-{str(i).zfill(8)}',
               date(2018,1,1)+timedelta(days=i%2500), date(2018,1,1)+timedelta(days=(i%2500)+365),
               round(1000+(i*37)%249000,2), st[i%5], datetime(2018,1,1)+timedelta(days=i%2500))

populate_batch("accounts", 10, gen_accounts(), 100000)

# COMMAND ----------

# Holdings (60,000)
def gen_holdings():
    makes = ['Royal Bank of Canada','Toronto-Dominion Bank','Enbridge','Shopify','Canadian National Railway','Brookfield Corp','Bank of Montreal','Suncor Energy','Manulife Financial','Thomson Reuters']
    models = ['Common Shares','Common Shares','4.25% Senior Notes 2031','Class A Subordinate Voting','3.60% Debentures 2029','Cumulative Preferred Series 2','Covered Call Canadian Banks ETF','Common Shares','Monthly Income Fund Series F','Common Shares']
    colors = ['Equity','Equity','Fixed Income','Equity','Fixed Income','Preferred Share','ETF','Equity','Mutual Fund','Equity']
    for i in range(1, 60001):
        yield (i, i, f'CA{i:09d}{i%10}', makes[i%10], models[i%10], 2015+(i%10), colors[i%10],
               ['RY','TD','ENB','SHOP','CNR','BN.PR.B','ZWB','SU','MFC','TRI'][i%10]+'.TO')

populate_batch("holdings", 8, gen_holdings(), 60000)

# COMMAND ----------

# Beneficiaries (80,000)
def gen_beneficiaries():
    fn = ['Emma','Liam','Olivia','Noah','Ava','Ethan','Sophia','Mason',
          'Isabella','Logan','Mia','Lucas','Charlotte','Aiden','Amelia']
    ln = ['Smith','Johnson','Brown','Davis','Wilson','Moore','Taylor','Anderson',
          'Thomas','Jackson','White','Harris','Martin','Garcia','Martinez','Clark',
          'Lewis','Lee','Walker','Hall']
    rels = ['Spouse','Child','Parent','Sibling','Other']
    for i in range(1, 80001):
        yield (i, ((i-1)%100000)+1, fn[i%15], ln[(i*11)%20], rels[i%5],
               round(100.0/(1+(i%3)),2), date(1960,1,1)+timedelta(days=(i*29)%18000))

populate_batch("beneficiaries", 7, gen_beneficiaries(), 80000)

# COMMAND ----------

# KYC Profiles (100,000)
def gen_kyc_profiles():
    types = ['Low','Low-Medium','Medium','Medium-High',
             'High','Medium','Low-Medium','Medium']
    for i in range(1, 100001):
        yield (i, ((i-1)%100000)+1, types[i%8], round(50000+(i*43)%1950000,2),
               round(30000+(i*17)%220000,2), date(2018,1,1)+timedelta(days=i%2500))

populate_batch("kyc_profiles", 6, gen_kyc_profiles(), 100000)

# COMMAND ----------

# Contributions (100,000)
def gen_contributions():
    methods = ['PAC','EFT','CHEQUE','WIRE']
    for i in range(1, 100001):
        due = date(2018,1,1)+timedelta(days=(i*3)%2500)
        paid = None if i%8==0 else due+timedelta(days=i%15)
        status = 'OVERDUE' if i%8==0 else ('PENDING' if i%12==0 else 'PAID')
        yield (i, ((i-1)%100000)+1, round(100+(i*23)%6900,2), due, paid, methods[i%4], status)

populate_batch("contributions", 7, gen_contributions(), 100000)

# COMMAND ----------

# Complaints (80,000)
def gen_complaints():
    types = ['Unsuitable Investment','Misrepresentation','Unauthorized Trading','Excessive Fees','Failure to Follow Instructions','Account Transfer Delay']
    st = ['OPEN','UNDER_REVIEW','RESOLVED','DISMISSED','ESCALATED_TO_OBSI']
    descs = ['Investor placed in high-risk product inconsistent with KYC','Advisor misstated fees and expected returns',
             'Trades executed without client authorization','Undisclosed trailing commissions charged',
             'Sell order not executed as instructed','RRSP transfer delayed beyond 60 days']
    for i in range(1, 80001):
        yield (i, ((i-1)%100000)+1, f'CMP-{str(i).zfill(8)}', types[i%6],
               date(2019,1,1)+timedelta(days=i%2200), date(2019,1,1)+timedelta(days=(i%2200)+(i%30)),
               round(500+(i*67)%49500,2), st[i%5], descs[i%6])

populate_batch("complaints", 9, gen_complaints(), 80000)

# COMMAND ----------

# Restitution Payments (100,000)
def gen_restitution_payments():
    types = ['COMPENSATION','PARTIAL','FINAL']
    st = ['PROCESSED','PENDING','COMPLETED','VOID']
    notes = ['Initial compensation','Supplemental compensation','Final settlement','Adjusted amount','OBSI-recommended amount']
    for i in range(1, 100001):
        yield (i, ((i-1)%80000)+1, round(100+(i*31)%9900,2),
               date(2019,3,1)+timedelta(days=i%2100), types[i%3], st[i%4], notes[i%5])

populate_batch("restitution_payments", 7, gen_restitution_payments(), 100000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

conn = get_connection(lakebase_db)
cur = conn.cursor()

print(f"\n{'='*50}")
print(f"Lakebase Setup Complete: {lakebase_db}")
print(f"{'='*50}")
for t in ['account_types','advisors','investors','accounts','holdings',
          'beneficiaries','kyc_profiles','contributions','complaints','restitution_payments']:
    cur.execute(f"SELECT COUNT(*) FROM lakebase_demo.{t}")
    print(f"  {t}: {cur.fetchone()[0]:,}")
conn.close()
