use datafusion::arrow::array::{AsArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, internal_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use regex::Regex;
use std::sync::Arc;

/// A user defined function regexp_extract which
/// tries to be as close as possible to regexp_extract
/// from Apache Spark.
///
/// Usage:
/// ```ignore
/// let persons = create_dataframe_somehow();
/// // Assume dataframe has full_name column, that contains
/// // "first-name middle-name last-name".
/// // We want to extract middle name.
/// let with_middle_name = persons
///     .with_column(
///         "middle_name",
///         regexp_extract.call(vec![col("full_name"), lit(r"(\w+) (\w+) (\w+)"), lit(2)])
///     );
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MyRegexpExtractUDF {
    signature: Signature,
}

impl MyRegexpExtractUDF {
    pub fn new() -> MyRegexpExtractUDF {
        MyRegexpExtractUDF {
            // Would be more error-safe to accept unsigned number for
            // the capture group index, but in Spark the regexp_extract function
            // accepts a signed number.
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Int32],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for MyRegexpExtractUDF {
    fn default() -> Self {
        MyRegexpExtractUDF::new()
    }
}

impl ScalarUDFImpl for MyRegexpExtractUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut call_args = args.args;
        assert_eq!(call_args.len(), 3);

        let capt_group_ind_arg = call_args.pop().unwrap();
        let re_patterns_arg = call_args.pop().unwrap();
        let source_str_arg = call_args.pop().unwrap();

        match (source_str_arg, re_patterns_arg, capt_group_ind_arg) {
            (
                ColumnarValue::Array(source_str),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(re_patterns))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(capt_group_ind))),
            ) => {
                let re = Regex::new(&re_patterns).map_err(|e| {
                    DataFusionError::Execution(
                        // Mimicking original Spark error format
                        format!("[INVALID_PARAMETER_VALUE.PATTERN] : {e}"),
                    )
                })?;

                let max_capt_group = re.captures_len() - 1;
                if capt_group_ind < 0 || capt_group_ind as usize > max_capt_group {
                    return exec_err!(
                        "[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] : Expects group index between 0 and {}, but got {}",
                        max_capt_group,
                        capt_group_ind
                    );
                }

                let res = source_str
                    .as_string::<i32>()
                    .iter()
                    .map(|maybe_str| {
                        maybe_str.map(|s| {
                            re.captures(s)
                                .and_then(|capts| {
                                    capts
                                        .get(capt_group_ind as usize)
                                        .map(|m| m.as_str().to_string())
                                })
                                .unwrap_or_default()
                        })
                    })
                    .collect::<StringArray>();
                Ok(ColumnarValue::Array(Arc::new(res)))
            }
            _ => internal_err!("Invalid argument types for {} function", self.name()),
        }
    }
}

#[cfg(test)]
mod test {
    use core::panic;

    use super::*;
    use datafusion::{
        arrow::datatypes::TimeUnit,
        dataframe,
        logical_expr::ScalarUDF,
        prelude::{SessionContext, cast, col, lit},
    };

    // Main successful scenario
    #[tokio::test]
    async fn tesm_main_scenario() -> Result<()> {
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

        assert_eq!(
            result.collect().await?,
            dataframe!("year" => ["1986", "1995", "1988"])?
                .collect()
                .await?
        );

        Ok(())
    }

    // For an empty value regexp_extract should return empty result
    // (similar to Spark implementation)
    #[tokio::test]
    async fn test_empty_column_value() -> Result<()> {
        let ctx = SessionContext::new();

        let regexp_extract = ScalarUDF::from(MyRegexpExtractUDF::new());
        ctx.register_udf(regexp_extract.clone());

        let df = dataframe!(
            "id" => [1, 2, 3],
            "name" => ["John Doe", "Robert Smith", "Stanislav Norochevskiy"],
            "birth_date" => [Some("1986-01-01"), None, Some("1988-09-03")],
        )?;

        let result = df.select(vec![
            regexp_extract
                .call(vec![col("birth_date"), lit(r"(\d+)-.*"), lit(1)])
                .alias("year"),
        ])?;

        assert_eq!(
            result.collect().await?,
            dataframe!("year" => [Some("1986"), None, Some("1988")])?
                .collect()
                .await?
        );

        Ok(())
    }

    // Verify that regexp_extract returns an empty string if it cannot extract
    // a value.
    #[tokio::test]
    async fn test_fail_to_extract() -> Result<()> {
        let ctx = SessionContext::new();

        let regexp_extract = ScalarUDF::from(MyRegexpExtractUDF::new());
        ctx.register_udf(regexp_extract.clone());

        let df = dataframe!(
            "id" => [1, 2, 3],
            "name" => ["John Doe", "Robert Smith", "Stanislav Norochevskiy"],
            "birth_date" => ["1986-01-01", "ololo", "1988-09-03"],
        )?;

        let result = df.select(vec![
            regexp_extract
                .call(vec![col("birth_date"), lit(r"(\d+)-.*"), lit(1)])
                .alias("year"),
        ])?;

        assert_eq!(
            result.collect().await?,
            dataframe!("year" => ["1986", "", "1988"])?
                .collect()
                .await?
        );

        Ok(())
    }

    // Should fail if the capture group index is negative or greater
    // than number of capture groups in the regular expression.
    #[tokio::test]
    async fn test_wrong_capture_group_index() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();

        let regexp_extract = ScalarUDF::from(MyRegexpExtractUDF::new());
        ctx.register_udf(regexp_extract.clone());

        let df = dataframe!(
            "id" => [1, 2, 3],
            "name" => ["John Doe", "Robert Smith", "Stanislav Norochevskiy"],
            "birth_date" => ["1986-01-01", "1995-12-12", "1988-09-03"],
        )?;

        let df = df.select(vec![
            regexp_extract
                .call(vec![col("birth_date"), lit(r"(\d+)-.*"), lit(99)])
                .alias("year"),
        ])?;

        let result = df.collect().await;

        if let Err(DataFusionError::Execution(msg)) = result {
            let expected_msg = "[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] : Expects group index between 0 and 1, but got 99";

            if msg == expected_msg {
                // Expected error
            } else {
                panic!("Expected another error message");
            }
        } else {
            panic!("Should return Execution error, but got: {result:?}");
        }

        Ok(())
    }

    // Should fail on incorrect regular expression.
    #[tokio::test]
    async fn test_malformed_regular_expression() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();

        let regexp_extract = ScalarUDF::from(MyRegexpExtractUDF::new());
        ctx.register_udf(regexp_extract.clone());

        let df = dataframe!(
            "id" => [1, 2, 3],
            "name" => ["John Doe", "Robert Smith", "Stanislav Norochevskiy"],
            "birth_date" => ["1986-01-01", "1995-12-12", "1988-09-03"],
        )?;

        let df = df.select(vec![
            regexp_extract
                .call(vec![col("birth_date"), lit(r"["), lit(0)])
                .alias("year"),
        ])?;

        let result = df.collect().await;

        if let Err(DataFusionError::Execution(msg)) = result {
            if msg.contains("[INVALID_PARAMETER_VALUE.PATTERN]") {
                // Expected error
            } else {
                panic!("Expected another error message");
            }
        } else {
            panic!("Should return Execution error, but got: {result:?}");
        }

        Ok(())
    }

    // Test that if we pass a non-string column to regexp_extract,
    // it will be converted to string column before applying regexp_extract.
    // (Similar to Spark implementation)
    #[tokio::test]
    async fn test_non_string_column() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();

        let regexp_extract = ScalarUDF::from(MyRegexpExtractUDF::new());
        ctx.register_udf(regexp_extract.clone());

        let df = dataframe!(
            "id" => [1, 2, 3],
            "name" => ["John Doe", "Robert Smith", "Stanislav Norochevskiy"],
            "birth_date_unix_time" => [
                504921600, // 1986-01-01 in unix time format
                818726400, // 1995-12-12
                589248000, // 1988-09-03
            ],
        )?
        .with_column(
            "birth_date",
            cast(col("birth_date_unix_time"), DataType::Timestamp(TimeUnit::Second, None)),
        )?
        .drop_columns(&["birth_date_unix_time"])?;

        let result = df.select(vec![
            regexp_extract
                .call(vec![col("birth_date"), lit(r"(\d+)-.*"), lit(1)])
                .alias("year"),
        ])?;

        assert_eq!(
            result.collect().await?,
            dataframe!("year" => ["1986", "1995", "1988"])?
                .collect()
                .await?
        );

        Ok(())
    }
}
