import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# =========================================================
# AWS GLUE JOB
# Reads raw healthcare files from S3, cleans them, and writes
# cleaned CSV folders back to S3 for Snowflake to COPY INTO.
#
# Expected S3 structure:
# s3://<bucket>/healthcare-insurance/input-data/
#   - Patient_records.csv
#   - subscriber.csv
#   - claims.json
#   - group.csv
#   - subgroup.csv
#   - grpsubgrp.csv
#   - disease.csv
#   - hospital.csv
#
# Output:
# s3://<bucket>/healthcare-insurance/clean-data/<table_name>/
# =========================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
bucket = args["S3_BUCKET"]

RAW_BASE = f"s3://{bucket}/healthcare-insurance/input-data"
CLEAN_BASE = f"s3://{bucket}/healthcare-insurance/clean-data"
LOG_BASE = f"s3://{bucket}/healthcare-insurance/logs"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def read_csv(path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )

def read_json(path):
    return spark.read.option("multiline", True).json(path)

def normalize_empty_to_null(df):
    for c in df.columns:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c).cast("string")) == "", None).otherwise(F.col(c))
        )
    return df

def write_csv(df, path):
    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", True)
          .csv(path)
    )

def null_count_df(df, dataset_name):
    exprs = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    return df.select(exprs).withColumn("dataset_name", F.lit(dataset_name))

def duplicate_count_df(df, subset_cols, dataset_name):
    return (
        df.groupBy(subset_cols)
          .count()
          .filter(F.col("count") > 1)
          .withColumn("dataset_name", F.lit(dataset_name))
    )

def safe_decimal(col_name, precision=14, scale=2):
    return F.regexp_replace(F.col(col_name).cast("string"), ",", "").cast(DecimalType(precision, scale))

def date_cast(col_name):
    return F.to_date(F.col(col_name), "yyyy-MM-dd")

# =========================================================
# 1. READ RAW FILES
# =========================================================
patients_raw = normalize_empty_to_null(read_csv(f"{RAW_BASE}/Patient_records.csv"))
subscribers_raw = normalize_empty_to_null(read_csv(f"{RAW_BASE}/subscriber.csv"))
claims_raw = normalize_empty_to_null(read_json(f"{RAW_BASE}/claims.json"))
groups_raw = normalize_empty_to_null(read_csv(f"{RAW_BASE}/group.csv"))
subgroups_raw = normalize_empty_to_null(read_csv(f"{RAW_BASE}/subgroup.csv"))
grpsubgrp_raw = normalize_empty_to_null(read_csv(f"{RAW_BASE}/grpsubgrp.csv"))
disease_raw = normalize_empty_to_null(read_csv(f"{RAW_BASE}/disease.csv"))
hospitals_raw = normalize_empty_to_null(read_csv(f"{RAW_BASE}/hospital.csv"))

# =========================================================
# 2. NULL REPORTS
# =========================================================
datasets = [
    ("patients", patients_raw),
    ("subscribers", subscribers_raw),
    ("claims", claims_raw),
    ("groups", groups_raw),
    ("subgroups", subgroups_raw),
    ("group_subgroup_bridge", grpsubgrp_raw),
    ("disease_subgroup_map", disease_raw),
    ("hospitals", hospitals_raw),
]

for name, df in datasets:
    write_csv(null_count_df(df, name), f"{LOG_BASE}/null_report/{name}/")

# =========================================================
# 3. CLEAN PATIENTS
# File columns:
# Patient_id, Patient_name, patient_gender, patient_birth_date,
# patient_phone, disease_name, city, hospital_id
# =========================================================
patients_clean = (
    patients_raw
    .withColumnRenamed("Patient_id", "patient_id")
    .withColumnRenamed("Patient_name", "patient_name")
    .withColumnRenamed("patient_gender", "patient_gender")
    .withColumnRenamed("patient_birth_date", "patient_birth_date")
    .withColumnRenamed("patient_phone", "patient_phone")
    .withColumnRenamed("disease_name", "disease_name")
    .withColumnRenamed("city", "city")
    .withColumnRenamed("hospital_id", "hospital_id")
    .withColumn("patient_id", F.trim(F.col("patient_id")))
    .withColumn("patient_name", F.initcap(F.trim(F.col("patient_name"))))
    .withColumn(
        "patient_gender",
        F.when(F.upper(F.trim(F.col("patient_gender"))).isin("M", "MALE"), F.lit("Male"))
         .when(F.upper(F.trim(F.col("patient_gender"))).isin("F", "FEMALE"), F.lit("Female"))
         .otherwise(F.lit("NA"))
    )
    .withColumn("patient_birth_date", date_cast("patient_birth_date"))
    .withColumn("patient_phone", F.trim(F.col("patient_phone")))
    .withColumn("disease_name", F.initcap(F.trim(F.col("disease_name"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("hospital_id", F.upper(F.trim(F.col("hospital_id"))))
    .dropDuplicates(["patient_id"])
)

# =========================================================
# 4. CLEAN SUBSCRIBERS
# File columns:
# sub _id, first_name, last_name, Street, Birth_date, Gender,
# Phone, Country, City, Zip Code, Subgrp_id, Elig_ind, eff_date, term_date
# =========================================================
subscribers_clean = (
    subscribers_raw
    .withColumnRenamed("sub _id", "sub_id")
    .withColumnRenamed("Street", "street")
    .withColumnRenamed("Birth_date", "birth_date")
    .withColumnRenamed("Gender", "gender")
    .withColumnRenamed("Phone", "phone")
    .withColumnRenamed("Country", "country")
    .withColumnRenamed("City", "city")
    .withColumnRenamed("Zip Code", "zip_code")
    .withColumnRenamed("Subgrp_id", "subgrp_id")
    .withColumnRenamed("Elig_ind", "elig_ind")
    .withColumn("sub_id", F.upper(F.trim(F.col("sub_id"))))
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    .withColumn("street", F.initcap(F.trim(F.col("street"))))
    .withColumn("birth_date", date_cast("birth_date"))
    .withColumn(
        "gender",
        F.when(F.upper(F.trim(F.col("gender"))).isin("M", "MALE"), F.lit("Male"))
         .when(F.upper(F.trim(F.col("gender"))).isin("F", "FEMALE"), F.lit("Female"))
         .otherwise(F.lit("NA"))
    )
    .withColumn("phone", F.trim(F.col("phone")))
    .withColumn("country", F.initcap(F.trim(F.col("country"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("zip_code", F.trim(F.col("zip_code")))
    .withColumn("subgrp_id", F.upper(F.trim(F.col("subgrp_id"))))
    .withColumn("elig_ind", F.upper(F.trim(F.col("elig_ind"))))
    .withColumn("eff_date", date_cast("eff_date"))
    .withColumn("term_date", date_cast("term_date"))
    .dropDuplicates(["sub_id"])
)

# =========================================================
# 5. CLEAN CLAIMS
# File keys:
# claim_id, patient_id, disease_name, SUB_ID, Claim_Or_Rejected,
# claim_type, claim_amount, claim_date
# Assumption:
#   Y = Rejected
#   N = Not Rejected / Settled
#   NaN/NULL = Unknown
# =========================================================
claims_clean = (
    claims_raw
    .withColumnRenamed("SUB_ID", "sub_id")
    .withColumnRenamed("Claim_Or_Rejected", "claim_or_rejected")
    .withColumn("claim_id", F.col("claim_id").cast("int"))
    .withColumn("patient_id", F.trim(F.col("patient_id").cast("string")))
    .withColumn("disease_name", F.initcap(F.trim(F.col("disease_name"))))
    .withColumn("sub_id", F.upper(F.trim(F.col("sub_id"))))
    .withColumn(
        "claim_or_rejected",
        F.when(F.upper(F.trim(F.col("claim_or_rejected"))) == "Y", F.lit("Rejected"))
         .when(F.upper(F.trim(F.col("claim_or_rejected"))) == "N", F.lit("Not Rejected"))
         .otherwise(F.lit("Unknown"))
    )
    .withColumn("claim_type", F.initcap(F.trim(F.col("claim_type"))))
    .withColumn("claim_amount", safe_decimal("claim_amount", 14, 2))
    .withColumn("claim_date", date_cast("claim_date"))
    .dropDuplicates(["claim_id"])
)

# =========================================================
# 6. CLEAN GROUPS
# File columns:
# Country, premium_written, zipcode, Grp_Id, Grp_Name, Grp_Type, city, year
# =========================================================
groups_clean = (
    groups_raw
    .withColumnRenamed("Country", "country")
    .withColumnRenamed("premium_written", "premium_written")
    .withColumnRenamed("zipcode", "zipcode")
    .withColumnRenamed("Grp_Id", "grp_id")
    .withColumnRenamed("Grp_Name", "grp_name")
    .withColumnRenamed("Grp_Type", "grp_type")
    .withColumnRenamed("city", "city")
    .withColumnRenamed("year", "year")
    .withColumn("country", F.initcap(F.trim(F.col("country"))))
    .withColumn("premium_written", safe_decimal("premium_written", 14, 2))
    .withColumn("zipcode", F.trim(F.col("zipcode")))
    .withColumn("grp_id", F.upper(F.trim(F.col("grp_id"))))
    .withColumn("grp_name", F.trim(F.col("grp_name")))
    .withColumn(
        "grp_type",
        F.when(F.upper(F.trim(F.col("grp_type"))).isin("GOVT", "GOVT."), F.lit("Government"))
         .when(F.upper(F.trim(F.col("grp_type"))) == "PRIVATE", F.lit("Private"))
         .otherwise(F.lit("NA"))
    )
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("year", F.col("year").cast("int"))
    .dropDuplicates(["grp_id"])
)

# =========================================================
# 7. CLEAN SUBGROUPS
# File columns:
# SubGrp_id, SubGrp_Name, Monthly_Premium
# =========================================================
subgroups_clean = (
    subgroups_raw
    .withColumnRenamed("SubGrp_id", "subgrp_id")
    .withColumnRenamed("SubGrp_Name", "subgrp_name")
    .withColumnRenamed("Monthly_Premium", "monthly_premium")
    .withColumn("subgrp_id", F.upper(F.trim(F.col("subgrp_id"))))
    .withColumn("subgrp_name", F.trim(F.col("subgrp_name")))
    .withColumn("monthly_premium", safe_decimal("monthly_premium", 14, 2))
    .dropDuplicates(["subgrp_id"])
)

# =========================================================
# 8. CLEAN GROUP-SUBGROUP BRIDGE
# File columns:
# SubGrp_ID, Grp_Id
# =========================================================
grpsubgrp_clean = (
    grpsubgrp_raw
    .withColumnRenamed("SubGrp_ID", "subgrp_id")
    .withColumnRenamed("Grp_Id", "grp_id")
    .withColumn("subgrp_id", F.upper(F.trim(F.col("subgrp_id"))))
    .withColumn("grp_id", F.upper(F.trim(F.col("grp_id"))))
    .dropDuplicates(["subgrp_id", "grp_id"])
)

# =========================================================
# 9. CLEAN DISEASE-SUBGROUP MAP
# File columns:
# SubGrpID, Disease_ID, Disease_name
# Note: Disease_ID had a leading space in the source file.
# =========================================================
disease_clean = (
    disease_raw
    .withColumnRenamed("SubGrpID", "subgrp_id")
    .withColumnRenamed(" Disease_ID", "disease_id")
    .withColumnRenamed("Disease_name", "disease_name")
    .withColumn("subgrp_id", F.upper(F.trim(F.col("subgrp_id"))))
    .withColumn("disease_id", F.trim(F.col("disease_id")))
    .withColumn("disease_name", F.initcap(F.trim(F.col("disease_name"))))
    .dropDuplicates(["subgrp_id", "disease_id"])
)

# =========================================================
# 10. CLEAN HOSPITALS
# File columns:
# Hospital_id, Hospital_name, city, state, country
# =========================================================
hospitals_clean = (
    hospitals_raw
    .withColumnRenamed("Hospital_id", "hospital_id")
    .withColumnRenamed("Hospital_name", "hospital_name")
    .withColumnRenamed("city", "city")
    .withColumnRenamed("state", "state")
    .withColumnRenamed("country", "country")
    .withColumn("hospital_id", F.upper(F.trim(F.col("hospital_id"))))
    .withColumn("hospital_name", F.trim(F.col("hospital_name")))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.initcap(F.trim(F.col("state"))))
    .withColumn("country", F.initcap(F.trim(F.col("country"))))
    .dropDuplicates(["hospital_id"])
)

# =========================================================
# 11. DUPLICATE AUDIT REPORTS
# =========================================================
duplicate_specs = [
    ("patients", patients_raw, ["Patient_id"]),
    ("subscribers", subscribers_raw, ["sub _id"]),
    ("claims", claims_raw, ["claim_id"]),
    ("groups", groups_raw, ["Grp_Id"]),
    ("subgroups", subgroups_raw, ["SubGrp_id"]),
    ("group_subgroup_bridge", grpsubgrp_raw, ["SubGrp_ID", "Grp_Id"]),
    ("disease_subgroup_map", disease_raw, ["SubGrpID", " Disease_ID"]),
    ("hospitals", hospitals_raw, ["Hospital_id"]),
]

for name, df, keys in duplicate_specs:
    write_csv(duplicate_count_df(df, keys, name), f"{LOG_BASE}/duplicates/{name}/")

# =========================================================
# 12. WRITE CLEANED DATASETS
# =========================================================
write_csv(patients_clean, f"{CLEAN_BASE}/patients/")
write_csv(subscribers_clean, f"{CLEAN_BASE}/subscribers/")
write_csv(claims_clean, f"{CLEAN_BASE}/claims/")
write_csv(groups_clean, f"{CLEAN_BASE}/groups/")
write_csv(subgroups_clean, f"{CLEAN_BASE}/subgroups/")
write_csv(grpsubgrp_clean, f"{CLEAN_BASE}/group_subgroup_bridge/")
write_csv(disease_clean, f"{CLEAN_BASE}/disease_subgroup_map/")
write_csv(hospitals_clean, f"{CLEAN_BASE}/hospitals/")

job.commit()
