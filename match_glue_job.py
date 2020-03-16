import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# set custom logging on
logger = glueContext.get_logger()

# load skills frame
skill = glueContext.create_dynamic_frame.from_catalog(database = "gluedb", table_name = "capstonematch_skill_csv")
skill = ApplyMapping.apply(frame = skill, mappings = [("match_id", "bigint", "match_id", "bigint"),
    ("skill", "bigint", "skill", "bigint")])
skill = ResolveChoice.apply(frame = skill, choice = "make_cols")
skill = DropNullFields.apply(frame = skill)
skill = skill.rename_field('match_id', 'skill_match_id')

# test output
test1 = glueContext.write_dynamic_frame.from_options(frame = skill, connection_type = "s3", connection_options = {"path":"s3://jf-dend-capstone/gluejob_output"}, format = "csv")


# load matches frame
matches = glueContext.create_dynamic_frame.from_catalog(database = "gluedb", table_name = "capstonematches_small_csv")
matches = ApplyMapping.apply(frame = matches, mappings = [("match_id", "bigint", "match_id", "bigint"),
    ("match_seq_num", "bigint", "match_seq_num", "bigint"),
    ("radiant_win", "string", "radiant_win", "string"),
    ("start_time", "bigint", "start_time", "bigint"),
    ("duration", "bigint", "duration", "bigint"),
    ("tower_status_radiant", "bigint", "tower_status_radiant", "bigint"),
    ("tower_status_dire", "bigint", "tower_status_dire", "bigint"),
    ("barracks_status_radiant", "bigint", "barracks_status_radiant", "bigint"),
    ("barracks_status_dire", "bigint", "barracks_status_dire", "bigint"),
    ("cluster", "bigint", "cluster", "bigint"),
    ("first_blood_time", "bigint", "first_blood_time", "bigint"),
    ("lobby_type", "bigint", "lobby_type", "bigint"),
    ("human_players", "bigint", "human_players", "bigint"),
    ("leagueid", "bigint", "leagueid", "bigint"),
    ("positive_votes", "bigint", "positive_votes", "bigint"),
    ("negative_votes", "bigint", "negative_votes", "bigint"),
    ("game_mode", "bigint", "game_mode", "bigint"),
    ("engine", "bigint", "engine", "bigint"),
    ("parse_status", "bigint", "parse_status", "bigint"),
    ("version", "string", "version", "string")])
matches = ResolveChoice.apply(frame = matches, choice = "make_cols")
matches = DropNullFields.apply(frame = matches)

# test output
test2 = glueContext.write_dynamic_frame.from_options(frame = matches, connection_type = "s3", connection_options = {"path":"s3://jf-dend-capstone/gluejob_output"}, format = "csv")

# convert to dataframe from dynamic dataframe to support left join
skill = skill.toDF()
matches = matches.toDF()
output = matches.join(skill, matches.match_id == skill.skill_match_id, how='left_outer').drop('skill_match_id')
output = output.filter(output['match_id'].isNotNull())


output = DynamicFrame.fromDF(output, glueContext, name='output_dynamicframe')
output = ApplyMapping.apply(frame = output, mappings = [("match_id", "bigint", "match_id", "bigint"),
    ("match_seq_num", "bigint", "match_seq_num", "bigint"),
    ("radiant_win", "string", "radiant_win", "string"),
    ("start_time", "bigint", "start_time", "bigint"),
    ("duration", "bigint", "duration", "bigint"),
    ("tower_status_radiant", "bigint", "tower_status_radiant", "bigint"),
    ("tower_status_dire", "bigint", "tower_status_dire", "bigint"),
    ("barracks_status_radiant", "bigint", "barracks_status_radiant", "bigint"),
    ("barracks_status_dire", "bigint", "barracks_status_dire", "bigint"),
    ("cluster", "bigint", "cluster", "bigint"),
    ("first_blood_time", "bigint", "first_blood_time", "bigint"),
    ("lobby_type", "bigint", "lobby_type", "bigint"),
    ("human_players", "bigint", "human_players", "bigint"),
    ("leagueid", "bigint", "leagueid", "bigint"),
    ("positive_votes", "bigint", "positive_votes", "bigint"),
    ("negative_votes", "bigint", "negative_votes", "bigint"),
    ("game_mode", "bigint", "game_mode", "bigint"),
    ("engine", "bigint", "engine", "bigint"),
    ("parse_status", "bigint", "parse_status", "bigint"),
    ("version", "string", "version", "string"),
    ("skill", "bigint", "skill", "bigint")])
output = ResolveChoice.apply(frame = output, choice = "make_cols", transformation_ctx = "resolvechoice2")
output = DropNullFields.apply(frame = output)

# test output
test3 = glueContext.write_dynamic_frame.from_options(frame = output, connection_type = "s3", connection_options = {"path":"s3://jf-dend-capstone/gluejob_output"}, format = "csv")

# data quality check: no results
if output.count() < 1:
    #write into the log file with:
    logger.warning('###### DATA QUALITY: MATCHES TABLE HAD NO ROWS, ABORTING ######')
else:
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = output, catalog_connection = "capstone-redshift-connection", connection_options = {"dbtable": "matches_table", "database": "dev"}, redshift_tmp_dir = args["TempDir"])


job.commit()
