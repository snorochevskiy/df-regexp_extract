use datafusion::{dataframe, error::Result, logical_expr::ScalarUDF, prelude::*};
use datafusion_regexp_extract::re::MyRegexpExtractUDF;

// This binary just prints a physical plan of a dataframe
// that uses regexp_extract function.
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let regexp_extract = ScalarUDF::from(MyRegexpExtractUDF::new());
    ctx.register_udf(regexp_extract.clone());

    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["John Doe", "Robert Smith", "Stanislav Norochevskiy"],
        "birth_date" => ["1986-01-01", "1995-12-12", "1988-09-03"],
    )?;

    let result = df.select(vec![
        regexp_extract
            .call(vec![
                col("birth_date"),
                lit(r"(\d+)-.*"), // Extracts year component
                lit(1),
            ])
            .alias("year"),
    ])?;

    result.explain(false, false)?.show().await?;
    // +---------------+------------------------------------------------------------------------------------+
    // | plan_type     | plan                                                                               |
    // +---------------+------------------------------------------------------------------------------------+
    // | logical_plan  | Projection: regexp_extract(?table?.birth_date, Utf8("(\d+)-.*"), Int32(1)) AS year |
    // |               |   TableScan: ?table? projection=[birth_date]                                       |
    // | physical_plan | ProjectionExec: expr=[regexp_extract(birth_date@0, (\d+)-.*, 1) as year]           |
    // |               |   DataSourceExec: partitions=1, partition_sizes=[1]                                |
    // |               |                                                                                    |
    // +---------------+------------------------------------------------------------------------------------+

    Ok(())
}
