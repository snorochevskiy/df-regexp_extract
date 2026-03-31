# Exploring Spark regexp_extract

```scala
import org.apache.spark.sql.functions._
import spark.implicits._

// Main
val test_df = Seq(
  (1, "John Doe", "1986-01-01"),
  (2, "Robert Smith", "1995-12-12"),
  (3, "Stanislav Norochevskiy", "1988-09-03")
).toDF("id", "name", "birth_date")
test_df.withColumn("year", regexp_extract(col("birth_date"), "(\\w+)-.*", 1)).show(false)
// +---+----------------------+----------+----+
// |id |name                  |birth_date|year|
// +---+----------------------+----------+----+
// |1  |John Doe              |1986-01-01|1986|
// |2  |Robert Smith          |1995-12-12|1995|
// |3  |Stanislav Norochevskiy|1988-09-03|1988|
// +---+----------------------+----------+----+

// None
val test_df = Seq(
  (1, "John Doe", Some("1986-01-01")),
  (2, "Robert Smith", None),
  (3, "Stanislav Norochevskiy", Some("1988-09-03"))
).toDF("id", "name", "birth_date")
test_df.withColumn("year", regexp_extract(col("birth_date"), "(\\w+)-.*", 1)).show(false)
// +---+----------------------+----------+----+
// |id |name                  |birth_date|year|
// +---+----------------------+----------+----+
// |1  |John Doe              |1986-01-01|1986|
// |2  |Robert Smith          |NULL      |NULL|
// |3  |Stanislav Norochevskiy|1988-09-03|1988|
// +---+----------------------+----------+----+

// Bad capture group
val test_df = Seq(
  (1, "John Doe", "1986-01-01"),
  (2, "Robert Smith", "1995-12-12"),
  (3, "Stanislav Norochevskiy", "1988-09-03")
).toDF("id", "name", "birth_date")
test_df.withColumn("year", regexp_extract(col("birth_date"), "(\\w+)-.*", 3)).show(false)
// org.apache.spark.SparkRuntimeException: [INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in `regexp_extract` is invalid: Expects group index between 0 and 1, but got 3.

// Malformed regex
val test_df = Seq(
  (1, "John Doe", "1986-01-01"),
  (2, "Robert Smith", "1995-12-12"),
  (3, "Stanislav Norochevskiy", "1988-09-03")
).toDF("id", "name", "birth_date")
test_df.withColumn("year", regexp_extract(col("birth_date"), "[", 0)).show(false)
// org.apache.spark.SparkRuntimeException: [INVALID_PARAMETER_VALUE.PATTERN] The value of parameter(s) `regexp` in `regexp_extract` is invalid: '['.

// Non-string column
val test_df = Seq(
  (1, "John Doe", true),
  (2, "Robert Smith", true),
  (3, "Stanislav Norochevskiy", true)
).toDF("id", "name", "birth_date")
test_df.withColumn("year", regexp_extract(col("birth_date"), "(\\w+)-.*", 0)).show(false)
// Doesn't fail
// +---+----------------------+----------+----+
// |id |name                  |birth_date|year|
// +---+----------------------+----------+----+
// |1  |John Doe              |true      |    |
// |2  |Robert Smith          |true      |    |
// |3  |Stanislav Norochevskiy|true      |    |
// +---+----------------------+----------+----+
```