import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, size


## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# set custom logging on
logger = glueContext.get_logger()

# load players df
players = glueContext.create_dynamic_frame.from_catalog(database = "gluedb", table_name = "capstoneplayer_matches_small_parquet")


players = ApplyMapping.apply(frame = players, mappings = [("match_id", "double", "match_id", "bigint"),
    ("account_id", "double", "account_id", "bigint"),
    ("player_slot", "double", "player_slot", "int"),
    ("hero_id", "double", "hero_id", "int"),
    ("item_0", "double", "item_0", "int"),
    ("item_1", "double", "item_1", "int"),
    ("item_2", "double", "item_2", "int"),
    ("item_3", "double", "item_3", "int"),
    ("item_4", "double", "item_4", "int"),
    ("item_5", "double", "item_5", "int"),
    ("kills", "double", "kills", "int"),
    ("deaths", "double", "deaths", "int"),
    ("assists", "double", "assists", "int"),
    ("leaver_status", "double", "leaver_status", "int"),
    ("gold", "double", "gold", "int"),
    ("last_hits", "double", "last_hits", "int"),
    ("denies", "double", "denies", "int"),
    ("gold_per_min", "double", "gold_per_min", "int"),
    ("xp_per_min", "double", "xp_per_min", "int"),
    ("gold_spent", "double", "gold_spent", "int"),
    ("hero_damage", "double", "hero_damage", "int"),
    ("tower_damage", "double", "tower_damage", "int"),
    ("hero_healing", "double", "hero_healing", "int"),
    ("level", "double", "level", "int"),
    ("stuns", "double", "stuns", "int")])
players = ResolveChoice.apply(frame = players, choice = "make_cols")
players = DropNullFields.apply(frame = players)

# test output
output = glueContext.write_dynamic_frame.from_options(frame = players, connection_type = "s3", connection_options = {"path":"s3://jf-dend-capstone/gluejob_output"}, format = "csv")

# drop nulls
players = players.toDF()


players = players.where("`match_id` is NOT NULL")
players = players.where("`account_id` is NOT NULL")
#players = players.where(size(col("match_id")) < 1)
#players = players.where(size(col("account_id")) < 1)
players = DynamicFrame.fromDF(players, glueContext, name='players_dynamicframe')

# data quality check: no results
if players.count() < 1:
    #write into the log file with:
    logger.warning('###### DATA QUALITY: PLAYERS TABLE HAD NO ROWS, ABORTING ######')
else:
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = players, catalog_connection = "capstone-redshift-connection", connection_options = {"dbtable": "players_table", "database": "dev"}, redshift_tmp_dir = args["TempDir"])



job.commit()
