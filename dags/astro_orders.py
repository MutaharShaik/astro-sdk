from datetime import datetime      # to define the startdate
from airflow.models import DAG       # import the DAG object
from pandas import DataFrame         # to transform the data from the table

from astro import sql as aql            # spl decorator aql: with this decorator we can execute both the pythpn functions and the SQL requests
from astro.files import File               # import file object and Table object
from astro.sql.table import Table

S3_FILE_PATH = "s3://mutahar-astrosdk"
S3_CONN_ID = "aws_default"             # airflow connections
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_ORDERS = "orders_table"
SNOWFLAKE_FILTERED_ORDERS = "filtered_table"
SNOWFLAKE_JOINED = "joined_table"
SNOWFLAKE_CUSTOMERS = "customers_table"
SNOWFLAKE_REPORTING = "reporting_table"


# step 3: transform operator : allows to transofrm the data and it implements the Transform  of an ETL system
# by running the SQL queroies each time the pipeline does the trannsformation that created a new table from
# the SELECT staement that you can pass to other tasks easily as like Xcoms

# 1 st ransfromation to filter the orders> 150
@aql.transform
def filter_orders(input_table: Table):             # this Table we got from the output in the snowflake
    return "SELECT * FROM {{input_table}} WHERE amount > 150"

# 2nd ransfromation to join between the two tables the filter_orders( orders> 150) and the customers_table we created in the snowflake
# with this astro SDK we not need hook operator

@aql.transform
def join_orders_customers(filtered_orders_table: Table, customers_table =Table):
    return """SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type 
    FROM {{filtered_orders_table}} f JOIN {{customers_table}} c
    ON f.customer_id = c.customer_id"""


# finally to DF
@aql.dataframe              # this decorator helps to transform the SQL table to DF
def transform_dataframe(df: DataFrame):
    purchase_dates = df.loc[:, "purchase_date"]
    print("purchase_dates:",purchase_dates)
    return purchase_dates





# step 1:
# define the DAG object
#orders_data - variable
# load_file - allows you to transfer the data from a file in to the table easily just by specifying the path to the file and the table where you want to put the data
# FILE AND TABLE are the objects used in the input and output
with DAG(dag_id="astro_orders",start_date=datetime(2022, 1, 1),schedule = "@daily",catchup=False):
    orders_data = aql.load_file(
        input_file = File(
            path = S3_FILE_PATH + "/orders_data_header.csv", conn_id = S3_CONN_ID
        ),
        output_table = Table(conn_id = SNOWFLAKE_CONN_ID)
    )


# step 2: now implement the transfirmnation tasks


    customers_table = Table(
        name=SNOWFLAKE_CUSTOMERS,
        conn_id = SNOWFLAKE_CONN_ID,
    )

# buy doing this above we can able tomanipulate to interact with the customer table in snowflake from teh data pipeline

# step 4:
# Finally we specify the dependencies
    # joined_data is temprrary table
    joined_data = join_orders_customers(filter_orders(orders_data),customers_table)



# step 5:
    # now we want to merge the joined data with the reporting table
    # merge operator allows to add the existing table with conflict resolution technique like ignore and update
    # we created reporting_table in snowflake
    reporting_table = aql.merge(
        target_table = Table (
            name = SNOWFLAKE_REPORTING,
            conn_id = SNOWFLAKE_CONN_ID
        ),

        source_table = joined_data,
        target_conflict_columns=["order_id"],            # to merge we use order_id column
        columns=["customer_id","customer_name"],
        if_conflicts="update",
    )
# now transform the data to DF to manipulate the DF using pandas in python

    purchase_dates = transform_dataframe(reporting_table)

# final step to clean the temporay tables which created with out specifying the names
    purchase_dates >> aql.cleanup

