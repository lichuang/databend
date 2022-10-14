// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use common_expression::from_date_data;
use common_expression::from_timestamp_data;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::Column;
use common_expression::ColumnFrom;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_datetime() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("datetime.txt").unwrap();

    test_to_timestamp(file);
    test_to_datetime(file);
    test_to_date(file);
    test_date_arithmetic_functions(file);
    test_timestamp_arithmetic_functions(file);
}

fn test_to_timestamp(file: &mut impl Write) {
    run_ast(file, "to_timestamp(-30610224000000001)", &[]);
    run_ast(file, "to_timestamp(-315360000000000)", &[]);
    run_ast(file, "to_timestamp(-315360000000)", &[]);
    run_ast(file, "to_timestamp(-100)", &[]);
    run_ast(file, "to_timestamp(-0)", &[]);
    run_ast(file, "to_timestamp(0)", &[]);
    run_ast(file, "to_timestamp(100)", &[]);
    run_ast(file, "to_timestamp(315360000000)", &[]);
    run_ast(file, "to_timestamp(315360000000000)", &[]);
    run_ast(file, "to_timestamp(253402300800000000)", &[]);
    run_ast(file, "to_timestamp(a)", &[(
        "a",
        DataType::Number(NumberDataType::Int64),
        Column::from_data(vec![
            -315360000000000i64,
            315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
}

fn test_to_datetime(file: &mut impl Write) {
    run_ast(file, "to_datetime(-30610224000000001)", &[]);
    run_ast(file, "to_datetime(-315360000000000)", &[]);
    run_ast(file, "to_datetime(-315360000000)", &[]);
    run_ast(file, "to_datetime(-100)", &[]);
    run_ast(file, "to_datetime(-0)", &[]);
    run_ast(file, "to_datetime(0)", &[]);
    run_ast(file, "to_datetime(100)", &[]);
    run_ast(file, "to_datetime(315360000000)", &[]);
    run_ast(file, "to_datetime(315360000000000)", &[]);
    run_ast(file, "to_datetime(253402300800000000)", &[]);
    run_ast(file, "to_datetime(a)", &[(
        "a",
        DataType::Number(NumberDataType::Int64),
        Column::from_data(vec![
            -315360000000000i64,
            315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
}

fn test_to_date(file: &mut impl Write) {
    run_ast(file, "to_date(-354286)", &[]);
    run_ast(file, "to_date(-354285)", &[]);
    run_ast(file, "to_date(-100)", &[]);
    run_ast(file, "to_date(-0)", &[]);
    run_ast(file, "to_date(0)", &[]);
    run_ast(file, "to_date(100)", &[]);
    run_ast(file, "to_date(2932896)", &[]);
    run_ast(file, "to_date(2932897)", &[]);
    run_ast(file, "to_date(a)", &[(
        "a",
        DataType::Number(NumberDataType::Int32),
        Column::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}

fn test_date_arithmetic_functions(file: &mut impl Write) {
    run_ast(file, "add_years(to_date(0), 10000)", &[]); // failed
    run_ast(file, "add_years(to_date(0), 100)", &[]);
    run_ast(file, "add_months(to_date(0), 100)", &[]);
    run_ast(file, "add_days(to_date(0), 100)", &[]);
    run_ast(file, "substract_years(to_date(0), 100)", &[]);
    run_ast(file, "substract_months(to_date(0), 100)", &[]);
    run_ast(file, "substract_days(to_date(0), 100)", &[]);
    run_ast(file, "add_years(a, b)", &[
        ("a", DataType::Date, from_date_data(vec![-100, 0, 100])),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "add_months(a, b)", &[
        ("a", DataType::Date, from_date_data(vec![-100, 0, 100])),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "add_days(a, b)", &[
        ("a", DataType::Date, from_date_data(vec![-100, 0, 100])),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_years(a, b)", &[
        ("a", DataType::Date, from_date_data(vec![-100, 0, 100])),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_months(a, b)", &[
        ("a", DataType::Date, from_date_data(vec![-100, 0, 100])),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_days(a, b)", &[
        ("a", DataType::Date, from_date_data(vec![-100, 0, 100])),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
}

fn test_timestamp_arithmetic_functions(file: &mut impl Write) {
    run_ast(file, "add_years(to_timestamp(0), 10000)", &[]); // failed
    run_ast(file, "add_years(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_months(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_days(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_hours(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_minutes(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_seconds(to_timestamp(0), 100)", &[]);
    run_ast(file, "substract_years(to_timestamp(0), 100)", &[]);
    run_ast(file, "substract_months(to_timestamp(0), 100)", &[]);
    run_ast(file, "substract_days(to_timestamp(0), 100)", &[]);
    run_ast(file, "substract_hours(to_timestamp(0), 100)", &[]);
    run_ast(file, "substract_minutes(to_timestamp(0), 100)", &[]);
    run_ast(file, "substract_seconds(to_timestamp(0), 100)", &[]);
    run_ast(file, "add_years(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "add_months(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "add_days(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "add_hours(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "add_minutes(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "add_seconds(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_years(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_months(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_days(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_hours(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_minutes(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
    run_ast(file, "substract_seconds(a, b)", &[
        (
            "a",
            DataType::Timestamp,
            from_timestamp_data(vec![-100, 0, 100]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int32),
            Column::from_data(vec![1, 2, 3]),
        ),
    ]);
}
