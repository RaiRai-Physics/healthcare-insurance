/*Create Warehouse, database, and schema */
create warehouse if not exists HC_INS_WH
warehouse_size = 'XSMALL';

create database if not exists HEALTHCARE_INSURANCE_DB;

create schema if not exists HEALTHCARE_INSURANCE_DB.PROJECT_CORE;
create schema if not exists HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT;
create schema if not exists HEALTHCARE_INSURANCE_DB.PROJECT_STAGE;

/*Create file format for CSV files */
create or replace file format HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT
type = csv
field_delimiter = ','
skip_header = 1
field_optionally_enclosed_by = '"'
null_if = ('NULL', 'null', '');

/*Create storage integration for S3 access */
create or replace storage integration HC_S3_INT
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::952567463517:role/snowflake_s3_access_role'
storage_allowed_locations = ('s3://takeo-capstone-project/healthcare-insurance/clean-data/');

desc integration HC_S3_INT;

/*Create external stage to access cleaned files in S3*/
create or replace stage HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE
storage_integration = HC_S3_INT
url = 's3://takeo-capstone-project/healthcare-insurance/clean-data/'
file_format = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT;

list @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE;

/* Creating core tables*/
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.PATIENTS (
    PATIENT_ID string,
    PATIENT_NAME string,
    PATIENT_GENDER string,
    PATIENT_BIRTH_DATE date,
    PATIENT_PHONE string,
    DISEASE_NAME string,
    CITY string,
    HOSPITAL_ID string
);

create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS (
    SUB_ID string,
    FIRST_NAME string,
    LAST_NAME string,
    STREET string,
    BIRTH_DATE date,
    GENDER string,
    PHONE string,
    COUNTRY string,
    CITY string,
    ZIP_CODE string,
    SUBGRP_ID string,
    ELIG_IND string,
    EFF_DATE date,
    TERM_DATE date
);

create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.CLAIMS (
    CLAIM_ID number,
    PATIENT_ID string,
    DISEASE_NAME string,
    SUB_ID string,
    CLAIM_OR_REJECTED string,
    CLAIM_TYPE string,
    CLAIM_AMOUNT number(14,2),
    CLAIM_DATE date
);

create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUPS (
    COUNTRY string,
    PREMIUM_WRITTEN number(14,2),
    ZIPCODE string,
    GRP_ID string,
    GRP_NAME string,
    GRP_TYPE string,
    CITY string,
    YEAR number
);

create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBGROUPS (
    SUBGRP_ID string,
    SUBGRP_NAME string,
    MONTHLY_PREMIUM number(14,2)
);

create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUP_SUBGROUP_BRIDGE (
    SUBGRP_ID string,
    GRP_ID string
);

create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.DISEASE_SUBGROUP_MAP (
    SUBGRP_ID string,
    DISEASE_ID string,
    DISEASE_NAME string
);

create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.HOSPITALS (
    HOSPITAL_ID string,
    HOSPITAL_NAME string,
    CITY string,
    STATE string,
    COUNTRY string
);

/*Copying values into tables */
truncate table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.PATIENTS;
truncate table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS;
truncate table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.CLAIMS;
truncate table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUPS;
truncate table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBGROUPS;
truncate table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUP_SUBGROUP_BRIDGE;
truncate table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.DISEASE_SUBGROUP_MAP;
truncate table HEALTHCARE_INSURANCE_DB.PROJECT_CORE.HOSPITALS;

copy into HEALTHCARE_INSURANCE_DB.PROJECT_CORE.PATIENTS
from @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE/patients/
file_format = (format_name = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT);

copy into HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS
from @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE/subscribers/
file_format = (format_name = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT);

copy into HEALTHCARE_INSURANCE_DB.PROJECT_CORE.CLAIMS
from @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE/claims/
file_format = (format_name = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT);

copy into HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUPS
from @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE/groups/
file_format = (format_name = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT);

copy into HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBGROUPS
from @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE/subgroups/
file_format = (format_name = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT);

copy into HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUP_SUBGROUP_BRIDGE
from @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE/group_subgroup_bridge/
file_format = (format_name = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT);

copy into HEALTHCARE_INSURANCE_DB.PROJECT_CORE.DISEASE_SUBGROUP_MAP
from @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE/disease_subgroup_map/
file_format = (format_name = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT);

copy into HEALTHCARE_INSURANCE_DB.PROJECT_CORE.HOSPITALS
from @HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.HC_EXT_STAGE/hospitals/
file_format = (format_name = HEALTHCARE_INSURANCE_DB.PROJECT_STAGE.CSV_FMT);

-- Important assumptions for the uploaded dataset:
-- 1) CLAIM_OR_REJECTED = 'Rejected' means rejected. 'Not Rejected' means not rejected. 'Unknown' remains unknown.
-- 2) A subgroup can belong to multiple groups. To avoid double counting, group-level premium and claim metrics
--    are allocated proportionally across mapped groups using allocation_weight = 1 / number_of_groups_for_that_subgroup.
-- 3) The dataset does not contain cashless_flag, total_charges, procedure_name, or admission_date.
--    So UC12 and UC13 are created as limitation tables.

create or replace view HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.V_SUBGROUP_GROUP_ALLOC as
select
    b.SUBGRP_ID,
    b.GRP_ID,
    1.0 / count(*) over (partition by b.SUBGRP_ID) as ALLOCATION_WEIGHT
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUP_SUBGROUP_BRIDGE b;

-- UC01 Which disease has maximum number of claims
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC01_DISEASE_MAX_CLAIMS as
select disease_name, count(*) as total_claims
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.CLAIMS
group by disease_name
order by total_claims desc, disease_name;

-- UC02 Find subscribers having age less than 30 and they subscribe any subgroup
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC02_SUBSCRIBERS_AGE_LT_30_WITH_SUBGROUP as
select
    sub_id,
    first_name,
    last_name,
    birth_date,
    datediff(year, birth_date, current_date()) as age,
    subgrp_id,
    city,
    elig_ind
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS
where datediff(year, birth_date, current_date()) < 30
  and subgrp_id is not null;

-- UC03 Find out which group has maximum subgroups
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC03_GROUP_MAX_SUBGROUPS as
select
    g.grp_id,
    g.grp_name,
    count(distinct b.subgrp_id) as subgroup_count
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUPS g
join HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUP_SUBGROUP_BRIDGE b
  on g.grp_id = b.grp_id
group by g.grp_id, g.grp_name
order by subgroup_count desc, g.grp_id;

-- UC04 Find out hospital which serves most number of patients
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC04_HOSPITAL_MOST_PATIENTS as
select
    h.hospital_id,
    h.hospital_name,
    count(distinct p.patient_id) as patient_count
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.HOSPITALS h
join HEALTHCARE_INSURANCE_DB.PROJECT_CORE.PATIENTS p
  on h.hospital_id = p.hospital_id
group by h.hospital_id, h.hospital_name
order by patient_count desc, h.hospital_id;

-- UC05 Find out which subgroups are subscribed most number of times
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC05_SUBGROUP_MOST_SUBSCRIPTIONS as
select
    s.subgrp_id,
    sg.subgrp_name,
    count(*) as subscription_count
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS s
left join HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBGROUPS sg
  on s.subgrp_id = sg.subgrp_id
group by s.subgrp_id, sg.subgrp_name
order by subscription_count desc, s.subgrp_id;

-- UC06 Find out total number of claims which were rejected
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC06_TOTAL_REJECTED_CLAIMS as
select count(*) as rejected_claim_count
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.CLAIMS
where claim_or_rejected = 'Rejected';

-- UC07 From where most claims are coming (city)
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC07_CITY_MAX_CLAIMS as
select
    p.city,
    count(*) as total_claims
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.CLAIMS c
join HEALTHCARE_INSURANCE_DB.PROJECT_CORE.PATIENTS p
  on c.patient_id = p.patient_id
group by p.city
order by total_claims desc, p.city;

-- UC08 Which groups of policies subscriber subscribe mostly Government or Private
-- Weighted to avoid overcounting when one subgroup maps to multiple groups.
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC08_POLICY_TYPE_PREFERENCE as
select
    g.grp_type,
    round(sum(a.allocation_weight), 2) as weighted_subscription_count
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS s
join HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.V_SUBGROUP_GROUP_ALLOC a
  on s.subgrp_id = a.subgrp_id
join HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUPS g
  on a.grp_id = g.grp_id
group by g.grp_type
order by weighted_subscription_count desc, g.grp_type;

-- UC09 Average monthly premium subscriber pay to insurance company
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC09_AVG_MONTHLY_PREMIUM as
select round(avg(sg.monthly_premium), 2) as avg_monthly_premium
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS s
join HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBGROUPS sg
  on s.subgrp_id = sg.subgrp_id;

-- UC10 Find out which group is most profitable
-- Profit = allocated subgroup monthly premium - allocated claim amount
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC10_GROUP_PROFITABILITY as
with subscriber_alloc as (
    select
        a.grp_id,
        s.sub_id,
        coalesce(sg.monthly_premium, 0) * 12 * a.allocation_weight as allocated_annual_premium
    from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS s
    join HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.V_SUBGROUP_GROUP_ALLOC a
      on s.subgrp_id = a.subgrp_id
    left join HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBGROUPS sg
      on s.subgrp_id = sg.subgrp_id
),
premium_by_group as (
    select
        grp_id,
        round(sum(allocated_annual_premium), 2) as total_allocated_premium
    from subscriber_alloc
    group by grp_id
),
claim_alloc as (
    select
        a.grp_id,
        c.claim_id,
        coalesce(c.claim_amount, 0) * a.allocation_weight as allocated_claim_amount
    from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.CLAIMS c
    join HEALTHCARE_INSURANCE_DB.PROJECT_CORE.SUBSCRIBERS s
      on c.sub_id = s.sub_id
    join HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.V_SUBGROUP_GROUP_ALLOC a
      on s.subgrp_id = a.subgrp_id
    where c.claim_or_rejected = 'Not Rejected'
),
claim_by_group as (
    select
        grp_id,
        round(sum(allocated_claim_amount), 2) as total_allocated_claim_amount
    from claim_alloc
    group by grp_id
)
select
    g.grp_id,
    g.grp_name,
    coalesce(p.total_allocated_premium, 0) as total_allocated_premium,
    coalesce(c.total_allocated_claim_amount, 0) as total_allocated_claim_amount,
    coalesce(p.total_allocated_premium, 0) - coalesce(c.total_allocated_claim_amount, 0) as profit
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.GROUPS g
left join premium_by_group p
  on g.grp_id = p.grp_id
left join claim_by_group c
  on g.grp_id = c.grp_id
where coalesce(p.total_allocated_premium, 0) <> 0
   or coalesce(c.total_allocated_claim_amount, 0) <> 0
order by profit desc, g.grp_id;

-- UC11 List all the patients below age of 18 who admit for cancer
-- Admission date is not available, so this is interpreted as patients below 18 with any cancer disease record.
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC11_PATIENTS_BELOW_18_CANCER as
select
    p.patient_id,
    p.patient_name,
    p.patient_gender,
    p.patient_birth_date,
    datediff(year, p.patient_birth_date, current_date()) as age,
    p.disease_name,
    p.city,
    p.hospital_id
from HEALTHCARE_INSURANCE_DB.PROJECT_CORE.PATIENTS p
where datediff(year, p.patient_birth_date, current_date()) < 18
  and upper(p.disease_name) like '%CANCER%';

-- UC12 Not possible from uploaded files
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC12_LIMITATION as
select
    'NOT AVAILABLE' as status,
    'cashless insurance flag and total charges are not present in the uploaded dataset' as reason;

-- UC13 Not possible from uploaded files
create or replace table HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC13_LIMITATION as
select
    'NOT AVAILABLE' as status,
    'procedure or surgery information and past-year surgery tracking are not present in the uploaded dataset' as reason;

/* =========================================================
   VALIDATION QUERIES
*/

-- Requirement 1:
-- Which disease has a maximum number of claims
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC01_DISEASE_MAX_CLAIMS
limit 10;

-- Requirement 2:
-- Find subscribers having age less than 30 and subscribed to any subgroup
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC02_SUBSCRIBERS_AGE_LT_30_WITH_SUBGROUP
order by age, sub_id
limit 20;

-- Requirement 3:
-- Find which group has maximum subgroups
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC03_GROUP_MAX_SUBGROUPS
limit 10;

-- Requirement 4:
-- Find hospital serving the most number of patients
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC04_HOSPITAL_MOST_PATIENTS
limit 10;

-- Requirement 5:
-- Find which subgroups are subscribed most number of times
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC05_SUBGROUP_MOST_SUBSCRIPTIONS
limit 10;

-- Requirement 6:
-- Find total number of claims which were rejected
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC06_TOTAL_REJECTED_CLAIMS;

-- Requirement 7:
-- Find the city from where most claims are coming
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC07_CITY_MAX_CLAIMS
limit 10;

-- Requirement 8:
-- Find whether policy subscriptions are mostly Government or Private
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC08_POLICY_TYPE_PREFERENCE;

-- Requirement 9:
-- Find average monthly premium paid by subscribers
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC09_AVG_MONTHLY_PREMIUM;

-- Requirement 10:
-- Find which group is most profitable
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC10_GROUP_PROFITABILITY
limit 10;

-- Requirement 11:
-- List all patients below age 18 admitted for cancer
-- Implemented as patients below 18 with disease name containing CANCER
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC11_PATIENTS_BELOW_18_CANCER
limit 20;

-- Requirement 12:
-- List patients with cashless insurance and total charges >= 50,000
-- Dataset limitation output
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC12_LIMITATION;

-- Requirement 13:
-- List female patients over age 40 who underwent knee surgery in past year
-- Dataset limitation output
select *
from HEALTHCARE_INSURANCE_DB.PROJECT_OUTPUT.UC13_LIMITATION;
