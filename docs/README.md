# --- Cleanup ---
print(f"\nFinal committed offset: {channel.get_latest_committed_offset_token()}")
channel.close()
client.close()
print("\nDemo complete!")
```

> **Tip:** You can change `DEMO_MINUTES` to a value between 1 and 10 to control how long the script streams data. At the default of 3 minutes, it sends 1,800 rows (5 rows every 0.5 seconds).

## Deploy Live Dashboard

Deploy a Streamlit in Snowflake app that auto-refreshes every 2 seconds so you can watch data arrive in real-time. This dashboard runs in the Snowflake cloud — no local Streamlit installation needed.

### Create a Stage

Run the following SQL in Snowsight:

```sql
CREATE STAGE IF NOT EXISTS SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);
```

### Write the Streamlit App

Create a file called `streamlit_app.py` in your `~/ssv2-quickstart` directory:

```python
import streamlit as st
from snowflake.snowpark.context import get_active_session
import time

st.set_page_config(page_title="Snowpipe Streaming high-performance architecture Monitor", layout="wide")

session = get_active_session()

DATABASE = "SSV2_QUICKSTART_DB"
SCHEMA   = "SSV2_SCHEMA"
TABLE    = "SSV2_QUICKSTART_USERS"
REFRESH_INTERVAL = 2

st.title("Snowpipe Streaming high-performance architecture — Live Monitor")
st.caption(f"Reading from `{DATABASE}.{SCHEMA}.{TABLE}` · refreshes every {REFRESH_INTERVAL}s")

try:
    metrics_df = session.sql(
        f"""SELECT COUNT(*) AS total_rows,
                   COALESCE(SUM(order_amount), 0) AS total_revenue
            FROM {DATABASE}.{SCHEMA}.{TABLE}""",
    ).to_pandas()
    total_rows = metrics_df["TOTAL_ROWS"].iloc[0] if len(metrics_df) > 0 else 0
    total_revenue = metrics_df["TOTAL_REVENUE"].iloc[0] if len(metrics_df) > 0 else 0
except Exception as e:
    st.error(f"Error querying table: {e}")
    total_rows = 0
    total_revenue = 0

if total_rows > 0:
    latest_df = session.sql(
        f"""SELECT MAX(user_id) AS latest_id,
                   COUNT(DISTINCT country) AS unique_countries
            FROM {DATABASE}.{SCHEMA}.{TABLE}""",
    ).to_pandas()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Rows", f"{total_rows:,}")
    col2.metric("Revenue Total", f"${total_revenue:,.2f}")
    col3.metric("Latest User ID", latest_df["LATEST_ID"].iloc[0])
    col4.metric("Unique Countries", latest_df["UNIQUE_COUNTRIES"].iloc[0])
else:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Rows", "0")
    col2.metric("Revenue Total", "$0.00")
    col3.metric("Latest User ID", "—")
    col4.metric("Unique Countries", "—")
    st.info("Waiting for data... Start the streaming demo to see rows appear.")

st.subheader("Most Recent Records")
if total_rows > 0:
    recent_df = session.sql(
        f"""SELECT user_id, first_name, last_name, email, country, order_amount
            FROM {DATABASE}.{SCHEMA}.{TABLE}
            ORDER BY user_id DESC
            LIMIT 20""",
    ).to_pandas()
    st.dataframe(recent_df, use_container_width=True)
else:
    st.write("No data yet.")

if total_rows > 0:
    st.subheader("Revenue Over Time")
    time_df = session.sql(
        f"""SELECT
                DATE_TRUNC('second', registration_date) AS time_bucket,
                SUM(SUM(order_amount)) OVER (ORDER BY DATE_TRUNC('second', registration_date)) AS cumulative_revenue
            FROM {DATABASE}.{SCHEMA}.{TABLE}
            GROUP BY time_bucket
            ORDER BY time_bucket""",
    ).to_pandas()
    st.line_chart(time_df.set_index("TIME_BUCKET"), y="CUMULATIVE_REVENUE", height=300)

    st.subheader("Top 10 Countries by Revenue")
    country_df = session.sql(
        f"""SELECT country, SUM(order_amount) AS revenue
            FROM {DATABASE}.{SCHEMA}.{TABLE}
            GROUP BY country
            ORDER BY revenue DESC
            LIMIT 10""",
    ).to_pandas()
    st.dataframe(country_df, use_container_width=True)

time.sleep(REFRESH_INTERVAL)
st.experimental_rerun()
```

### Upload and Deploy

Upload the Streamlit app to the stage and create the Streamlit object. Run these commands using the [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index):

```bash
snow stage copy streamlit_app.py @SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_STREAMLIT_STAGE --overwrite
```

Then run the following SQL in Snowsight:

```sql
CREATE OR REPLACE STREAMLIT SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_LIVE_MONITOR
    ROOT_LOCATION = '@SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = <YOUR_WAREHOUSE>
    TITLE = 'Snowpipe Streaming high-performance architecture Monitor';

-- Get the URL to open the dashboard
SHOW STREAMLITS IN SCHEMA SSV2_QUICKSTART_DB.SSV2_SCHEMA;
```

Open the Streamlit URL from the `SHOW STREAMLITS` output. The dashboard will show "Waiting for data..." until you start streaming in the next step.

## Stream Sample Data

Now run the demo script to stream fake user data into Snowflake. Make sure your Streamlit dashboard is open in another browser tab so you can watch data arrive in real-time.

### Activate the Virtual Environment

If you are in a new terminal session, reactivate the virtual environment:

```bash
cd ~/ssv2-quickstart
source ssv2_venv/bin/activate
```

### Run the Demo

```bash
python ssv2_demo.py
```

You should see output like:

```
Connecting to Snowflake...
  Database: SSV2_QUICKSTART_DB
  Schema:   SSV2_SCHEMA
  Pipe:     SSV2_QUICKSTART_USERS-streaming (default auto-created pipe)

Opening channel...
  Channel: SSV2_QUICKSTART_CHANNEL
  Status:  0
  Latest committed offset: None

Streaming 1800 rows (360 batches of 5) over ~3 minute(s)...
Watch your Streamlit dashboard to see data arrive in real-time!
```

Switch to your Streamlit dashboard tab — you should see the row count climbing, revenue accumulating, and the chart updating every 2 seconds.

> **What is happening behind the scenes?** The Python SDK sends batches of rows to Snowflake via a streaming channel. Snowflake's high-performance architecture uses a default auto-created pipe (`SSV2_QUICKSTART_USERS-streaming`) to ingest the data directly into the table — no staging files are involved.

## Verify Results

After the demo script finishes, verify that all rows arrived in the table.

### Check Row Count

Run in Snowsight:

```sql
SELECT COUNT(*) AS total_rows FROM SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_QUICKSTART_USERS;
```

You should see **1,800 rows** (or the total matching your `DEMO_MINUTES` setting).

### Sample the Data

```sql
SELECT *
FROM SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_QUICKSTART_USERS
ORDER BY user_id DESC
LIMIT 10;
```

### Check Revenue by Country

```sql
SELECT country, COUNT(*) AS users, SUM(order_amount) AS total_revenue
FROM SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_QUICKSTART_USERS
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 10;
```

## Clean Up

Remove all Snowflake objects created during this quickstart. Run the following SQL in Snowsight:

```sql
DROP STREAMLIT IF EXISTS SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_LIVE_MONITOR;
DROP STAGE IF EXISTS SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_STREAMLIT_STAGE;
DROP TABLE IF EXISTS SSV2_QUICKSTART_DB.SSV2_SCHEMA.SSV2_QUICKSTART_USERS;
DROP SCHEMA IF EXISTS SSV2_QUICKSTART_DB.SSV2_SCHEMA;
DROP DATABASE IF EXISTS SSV2_QUICKSTART_DB;
DROP USER IF EXISTS SSV2_DEMO_USER;
DROP ROLE IF EXISTS SSV2_DEMO_ROLE;
```

Optionally, remove local files:

```bash
cd ~ && rm -rf ~/ssv2-quickstart
```

## Conclusion And Resources

Congratulations! You have successfully built an end-to-end real-time streaming pipeline using Snowpipe Streaming high-performance architecture and seen how Cortex Code can automate the entire workflow with a single prompt.

### What You Learned

- **Snowpipe Streaming high-performance architecture** uses default auto-created pipes — no `CREATE PIPE` SQL needed
- The default pipe follows the naming convention `<TABLE_NAME>-streaming` (with a **hyphen**)
- The Python SDK authenticates via **RSA key-pair (JWT)** and streams rows through channels
- **Streamlit in Snowflake** can be used to build real-time monitoring dashboards with zero local infrastructure
- **Cortex Code skills** can automate the entire workflow — from key generation to dashboard deployment — with a single prompt

### Related Resources

- [Snowpipe Streaming Documentation](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview)
- [Snowpipe Streaming Python SDK Reference](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-python-sdk)
- [Key-Pair Authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)
- [Cortex Code Documentation](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code)
- [SSv2 Cortex Code Skills (GitHub)](https://github.com/sfc-gh-zgebru/SSv2-AI-Webinar)
