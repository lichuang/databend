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

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

#[test]
fn test_schema_delete_field() -> Result<()> {
    let field1 = DataField::new("a", u64::to_data_type());
    let field2 = DataField::new("b", u64::to_data_type());
    let field3 = DataField::new("c", u64::to_data_type());

    let mut schema =
        DataSchema::new_with_deleted(vec![field1.clone()], BTreeMap::new(), Some(vec![false]));

    assert_eq!(schema.fields().to_owned(), vec![field1.clone()]);
    assert_eq!(schema.columns().to_owned(), vec![(Some(0), field1.clone())]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);

    // add columns
    schema.add_columns(vec![field2.clone(), field3.clone()]);
    assert_eq!(schema.fields().to_owned(), vec![
        field1.clone(),
        field2.clone(),
        field3.clone()
    ]);
    assert_eq!(schema.columns().to_owned(), vec![
        (Some(0), field1.clone()),
        (Some(1), field2.clone()),
        (Some(2), field3.clone())
    ]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);

    // drop column b
    schema.drop_column("b")?;
    assert_eq!(schema.fields().to_owned(), vec![
        field1.clone(),
        field3.clone()
    ]);
    assert_eq!(schema.columns().to_owned(), vec![
        (Some(0), field1.clone()),
        (None, field2.clone()),
        (Some(1), field3.clone())
    ]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);

    // drop column c
    schema.drop_column("c")?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(),]);
    assert_eq!(schema.columns().to_owned(), vec![
        (Some(0), field1.clone()),
        (None, field2.clone()),
        (None, field3.clone())
    ]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);

    Ok(())
}
