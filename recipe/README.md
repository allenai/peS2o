# Recipe

## 1. Obtain data

There are two queries to run in AWS Athena (`us-west-2`):

1. `1.obtain_data/s2orc.sql` - for the S2ORC dataset
2. `1.obtain_data/s2ag.sql` - for the S2AG dataset

The S2ORC query takes 26.5 minutes to run (2.60 TB scanned); the S2AG query takes 13 minutes to run (155.21 GB scanned).

Make sure to update the dates to the closest dump date to the current date. S2ORC dumps occur every week on a Sunday. Then, match to closest espresso date, which you can find by running

```sql
SELECT partition_0, COUNT(*) FROM "espresso"."pq_paper" GROUP by "partition_0";
```


## 2. Process data

First, install the dependencies:

```bash
pip install -r requirements.txt
```

Then, run:

```shell
python 2_scripts/process_s2ag.py \
  src=s3://ai2-llm/pretraining-data/sources/s2/raw/2024_10_06/s2ag/ \
  dst=s3://ai2-llm/pretraining-data/sources/s2/v0-fos-license/documents/2024-10-06/dataset=s2ag \
  parallel=128

python 2_scripts/process_s2orc.py \
  src=s3://ai2-llm/pretraining-data/sources/s2/raw/2024_10_06/s2orc/ \
  dst=s3://ai2-llm/pretraining-data/sources/s2/v0-fos-license/documents/2024-10-06/dataset=s2orc \
  parallel=128
```

The S2AG script takes 20 minutes to run; the S2ORC script takes 35 minutes to run.


## 3. Load as table

Run the following queries in AWS Athena (`us-west-2`):

1. `3_load_as_table/s2ag.sql`
2. `3_load_as_table/s2orc.sql`

Make sure to run `MSCK REPAIR TABLE <table_name>` after loading the data.


## 4. Mix final

Run the following queries in AWS Athena (`us-west-2`):

1. `4_mix_final/s2ag.sql`
2. `4_mix_final/s2orc.sql`

The S2AG query takes 10 minutes to run; the S2ORC query takes 16 minutes to run.
