# Recipe

## 1. Obtain data

There are two queries to run in AWS Athena (`us-west-2`):

1. `1.obtain_data/s2orc.sql` - for the S2ORC dataset
2. `1.obtain_data/s2ag.sql` - for the S2AG dataset

The S2ORC query takes 26.5 minutes to run (2.60 TB scanned); the S2AG query takes 13.5 minutes to run (155.21 GB scanned).

Make sure to update the dates to the closest dump date to the current date. S2ORC dumps occur every week on a Sunday. Then, match to closest espresso date, which you can find by running

```sql
SELECT partition_0, COUNT(*) FROM "espresso"."pq_paper" GROUP by "partition_0";
```


## 2. Process data

First, install the dependencies:

```bash
pip install -r 2_scripts/requirements.txt
```

Then, run:

```shell
python 2_scripts/process_s2ag.py \
  src=s3://ai2-llm/pretraining-data/sources/s2/raw/2024_10_06/s2ag/ \
  dst=s3://ai2-llm/pretraining-data/sources/s2/v3-fos-license/documents/2024-10-06/dataset=s2ag \
  parallel=128 \
  version="v3-fos-license-2024-10-06" \
  source="pes2o/s2ag"

python 2_scripts/process_s2orc.py \
  src=s3://ai2-llm/pretraining-data/sources/s2/raw/2024_10_06/s2orc/ \
  dst=s3://ai2-llm/pretraining-data/sources/s2/v3-fos-license/documents/2024-10-06/dataset=s2orc \
  parallel=128 \
  version="v3-fos-license-2024-10-06" \
  source="pes2o/s2orc"
```
