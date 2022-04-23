To play with the code, open it as a project in IntelliJ and run the `*LocalIngestion` files from the `bartosz.ingestion`
package for the ingestion step. You need to change the path for your dataset. 

The statistics jobs operate on managed tables, so there is no need to pass the path. You should run them in order, though,
starting by `CustomersDailyStatsGenerationJob`. Two others can be executed simoultanesly.

1.
>Get the four datasets into Spark

In the solution I decided to:

* By "Get the four datasets into Spark" I understood using the managed (internal) tables. That's why I used the
  `.saveAsTable` in the sink. Of course, the solution could use simple DataFrame Writer API.
* Use partitioning and bucketing to leverage storage optimizations in daily job execution. I used the hourly-based
  granularity to simplify running smaller batches in the future (less data to process, less compute needed, less money
  to spend). I also clustered the customers and orders by customer_id to keep the join between them local (no shuffle)
  in the `CustomersDailyStatsGenerationJob`.
* Use `Apache Parquet` storage format because the use case is analytical and columnar format is more efficient for that
  scenario.
* Keep a separate ingestion job for each dataset; I assume here a Big Data scenario where the separation of concerns
  helps scale (4 jobs running at the same time) and maintain (job failure doesn't mean restarting the ingestion
  for the succeeded datasets, or complexifying the job to handle this failure management in the code) them better.
  But I'm aware that the datasets are small and they could be run as a single job in the exercise.
* The jobs don't use any abstraction since their code exclusively relies on Apache Spark API and there is no
  extra business complexity in this layer. We could of course create a `factory writers`, such as
  `writeParquetToTable(tableName String)`.
* Use the static schema; I know we can use schema inference but having the schema defined proves data knowledge and
  avoids Spark runtime inference which would lead to reading the input twice (even with the defined small threshold).
* Even though I'm not using `Dataset`s, I defined case classes to facilitate unit tests setup for the raw data part.
* Hardcoded `local[*]`. I'm running the code from the IntelliJ. I would avoid hardcoding this for the code supposed to
  run on a cluster. Instead, I'd pass the master in the spark-submit `--master` flag
* I assume the ingestion step processes the raw data already stored somewhere in our architecture. That's why instead of
  saving orders and items separately, I denormalized them. It's another assumption: orders + items have much better
  insight value than the orders alone. Also, it avoids joining them each time we need.
  (=> Silver layer of the lakehouse architecture)
* I didn't do nothing with the records having invalid `order_purchase_timestamp` value. We could keep them in a
  separate dead-letter directory to track how bad the data quality is and eventually, ask the producers to fix the issues.
  By correct `order_purchase_timestamp` I mean a not null, not empty (!= 0000-00...), and not in the future (2077-...)
  dates.

2.
> Each day we want to compute summary statistics by customers every day (spending, orders etc.) Create a Spark script to compute for a given day these summary statistics.

Here I'm generating the following daily statistics:

* total spending
* number of orders
* sum of ordered items
* the cheapest order of the day
* the most expensive order of the day

3.
> Run that script over the necessary period to inject historic data. Then, identify the top customers

## CustomersDailyStatsGenerationJobLocalIngestion
I'm running the code from a local runner class `CustomersDailyStatsGenerationJobLocalIngestion`. If we had an
orchestrator and the job should run daily after backfilling the historical data, I would use the native backfill
capability of Apache Airflow. The job is built to accept an input execution date that we can pass in the
`spark-submit` command.

I'm also assuming here a Big Data scenario in real life. With the jobs working on the partitioned dataset we could 
leverage the orchestrator parallelism and compute multiple days at the same time. 

I'm also saving these top customers in partitioned storage, this time, at a daily basis. I assumed we could 
calculate monthly aggregates by processing less data.

## TopCustomersGenerationJob
To get the top consumers for the whole historical dataset, I created an aggregation job called `TopCustomersGenerationJob`:

After running the job, I got the following top 40 customers:
```
+--------------------------------+---------------+
|customer_id                     |total_spendings|
+--------------------------------+---------------+
|1617b1357756262bfa56ab541c47bc16|13664.08       |
|ec5b2ba62e574342386871631fafd3fc|7274.88        |
|c6e2731c5b391845f6800c97401a43a9|6929.31        |
|f48d464a0baaea338cb25f816991ab1f|6922.21        |
|3fd6777bbce08a352fddd04e4a7cc8f6|6726.66        |
|05455dfa7cd02f13d132aa7a6a9729c6|6081.54        |
|df55c14d1476a9a3467f131269c2477f|4950.34        |
|e0a2412720e9ea4f26c1ac985f6a7358|4809.44        |
|24bbf5fd2f2e1b359ee7de94defc4a15|4764.34        |
|3d979689f636322c62418b6346b1c6d2|4681.78        |
|1afc82cd60e303ef09b4ef9837c9505c|4513.32        |
|cc803a2c412833101651d3f90ca7de24|4445.5         |
|926b6a6fb8b6081e00b335edaf578d35|4194.76        |
|35a413c7ca3c69756cb75867d6311c0d|4175.26        |
|e9b0d0eb3015ef1c9ce6cf5b9dcbee9f|4163.51        |
|3be2c536886b2ea4668eced3a80dd0bb|4042.74        |
|eb7a157e8da9c488cd4ddc48711f1097|4034.44        |
|c6695e3b1e48680db36b487419fb0398|4016.91        |
|31e83c01fce824d0ff786fcd48dad009|3979.55        |
|addc91fdf9c2b3045497b57fc710e820|3826.8         |
|19b32919fa1198aefc0773ee2e46e693|3792.59        |
|66657bf1753d82d0a76f2c4719ab8b85|3736.22        |
|7d03bf20fa96e80468bbf678eebbcb3f|3666.42        |
|39d6658037b1b5a07d0a24d423f0bd19|3602.47        |
|e7c905bf4bb13543e8df947af4f3d9e9|3526.46        |
|3c7c62e8d38fb18a33a45db8021f2d69|3406.47        |
|0b16ce67087d02bf833c807e82b1992b|3358.24        |
|803cd9b04f9cd252c6a83a2ecdbc22c3|3351.35        |
|46bb3c0b1a65c8399d0363cefbcc4f37|3297.4         |
|a7ea318cbe9df2ec79ab37cd7ca2135d|3256.14        |
|f7622098214b4634b7fe7eee269b5426|3242.84        |
|7e7d2271d6f55b03b0bd4615c5f2de6e|3209.72        |
|71901689c5f3e5adc27b1dd16b33f0b8|3195.74        |
|c26acf0451e0f8ec1f5218731b9a51cf|3184.55        |
|26dcb450c4b5b390e79e6d5d0f2c6535|3184.34        |
|a95f4bbcf95262b073e4afa481b59ff8|3155.82        |
|10a86619816f9d2afce3f45b04aabc71|3126.5         |
|0a209a88c2e3dc2981c79ad85c558059|3126.5         |
|6152fbfc8a92ee25fd821740bd33b089|3122.72        |
|c03dfaf5db49d8583edbb5627f92058d|3076.13        |
+--------------------------------+---------------+
```

4.
> How many customers are repeaters ?

There are 2997 repeaters. I ran the `RepeatingCustomersGenerationJob` code.

5.
> Optionnal : If you want to show more skills you have, add anything you find usefull :

I extended the project with unit tests.

I wanted to do more, like creating a job in Structured Streaming, but I'm running out of time. I have some others
already existing Spark code snippets if you want to take a look:
- https://github.com/bartosz25/spark-playground - short snippets to illustrate the demos on my blog.
- https://github.com/bartosz25/sessionization-demo - demo code I prepared for my talk at Spark+AI Summit 2019
- https://github.com/bartosz25/data-ai-summit-2020 - demo code I prepared for my talk at Data+AI Summit 2020

Best,
Bartosz.