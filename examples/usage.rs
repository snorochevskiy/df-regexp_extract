use datafusion::{error::Result, logical_expr::ScalarUDF, prelude::*};
use datafusion_regexp_extract::re::MyRegexpExtractUDF;

// Simple usage example
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let regexp_extract = ScalarUDF::from(MyRegexpExtractUDF::new());
    ctx.register_udf(regexp_extract.clone());

    // Read data that containsthree columns:
    // * id - number
    // * full name - "first_name middle_name last_name", or just "fist_name last_name"
    // * birth date
    let df = ctx
        .read_csv("test_data/employees.csv", CsvReadOptions::default())
        .await?;

    // Using regexp_extract, extracting middle name into a separate column.
    let result = df.with_column(
        "middle_name",
        regexp_extract.call(vec![col("full_name"), lit(r"(\w+) (\w+) (\w+)"), lit(1)]),
    )?;

    result.show().await?;
    // +----+-------------------------------------+------------+-------------+
    // | id | full_name                           | birth_data | middle_name |
    // +----+-------------------------------------+------------+-------------+
    // | 1  | John Doe                            | 1986-01-01 |             |
    // | 2  | Robert Smith                        | 1995-12-12 |             |
    // | 3  | Stanislav Mikhailovich Norochevskiy | 1988-09-03 | Stanislav   |
    // +----+-------------------------------------+------------+-------------+

    Ok(())
}
