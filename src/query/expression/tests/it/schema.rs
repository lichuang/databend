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

use std::collections::BTreeMap;

use ce::types::NumberDataType;
use common_exception::Result;
use common_expression as ce;
use common_expression::TableField;
use common_expression::TableSchema;

#[test]
fn test_schema_modify_field() -> Result<()> {
    let field1 =
        TableField::new_with_column_id("a", ce::TableDataType::Number(NumberDataType::Int16), 0);
    let field2 =
        TableField::new_with_column_id("b", ce::TableDataType::Number(NumberDataType::Int16), 1);
    let field3 =
        TableField::new_with_column_id("c", ce::TableDataType::Number(NumberDataType::Int16), 2);

    let mut schema =
        TableSchema::new_from_with_max_column_id(vec![field1.clone()], BTreeMap::new(), 1);

    assert_eq!(schema.fields().to_owned(), vec![field1.clone()]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);

    // add column b
    schema.add_column(&field2);
    assert_eq!(schema.fields().to_owned(), vec![
        field1.clone(),
        field2.clone(),
    ]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);

    // drop column b
    schema.drop_column("b")?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(),]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);

    // add column c
    schema.add_column(&field3);
    assert_eq!(schema.fields().to_owned(), vec![field1, field3]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);
    Ok(())
}
