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

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

#[test]
fn test_schema_delete_field() -> Result<()> {
    let field1 = DataField::new("a", u64::to_data_type());
    let mut field2 = DataField::new("b", u64::to_data_type());
    let mut field3 = DataField::new("c", u64::to_data_type());

    let mut schema = DataSchema::new(vec![field1.clone()]);

    assert_eq!(schema.fields().to_owned(), vec![field1.clone()]);
    assert_eq!(schema.all_fields().to_owned(), vec![(
        Some(0),
        field1.clone()
    )]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);

    // add columns
    field2.tag_add();
    field3.tag_add();
    schema.add_columns(vec![field2.clone(), field3.clone()]);
    assert_eq!(schema.fields().to_owned(), vec![
        field1.clone(),
        field2.clone(),
        field3.clone()
    ]);
    assert_eq!(schema.all_fields().to_owned(), vec![
        (Some(0), field1.clone()),
        (Some(1), field2.clone()),
        (Some(2), field3.clone())
    ]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);

    // drop column b
    field2.tag_delete();
    schema.drop_column("b")?;
    assert_eq!(schema.fields().to_owned(), vec![
        field1.clone(),
        field3.clone()
    ]);
    assert_eq!(schema.all_fields().to_owned(), vec![
        (Some(0), field1.clone()),
        (None, field2.clone()),
        (Some(1), field3.clone())
    ]);
    for field in schema.fields() {
        assert!(!field.is_deleted());
    }
    assert!(schema.all_fields()[1].1.is_deleted());
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);

    // drop column c
    field3.tag_delete();
    schema.drop_column("c")?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(),]);
    assert_eq!(schema.all_fields().to_owned(), vec![
        (Some(0), field1.clone()),
        (None, field2.clone()),
        (None, field3.clone())
    ]);
    assert!(schema.all_fields()[2].1.is_deleted());
    for field in schema.fields() {
        assert!(!field.is_deleted());
    }
    assert_eq!(schema.column_id_of("a").unwrap(), 0);

    Ok(())
}

#[test]
fn test_schema_project() -> Result<()> {
    let field1 = DataField::new("a", u64::to_data_type());
    let field2 = DataField::new("b", u64::to_data_type());
    let field3 = DataField::new("c", u64::to_data_type());

    let schema = DataSchema::new(vec![field1.clone(), field2.clone(), field3.clone()]);

    let project = [0, 2];
    let project_schema = schema.project(&project);

    assert_eq!(project_schema.fields().to_owned(), vec![
        field1.clone(),
        field3.clone()
    ]);
    assert_eq!(project_schema.all_fields().to_owned(), vec![
        (Some(0), field1.clone()),
        (None, field2.clone()),
        (Some(1), field3.clone())
    ]);
    assert_eq!(project_schema.column_id_of("a").unwrap(), 0);
    assert_eq!(project_schema.column_id_of("c").unwrap(), 2);
    assert_eq!(project_schema.index_of("a").unwrap(), 0);
    assert_eq!(project_schema.index_of("c").unwrap(), 1);

    Ok(())
}
