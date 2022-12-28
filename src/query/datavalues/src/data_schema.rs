// Copyright 2021 Datafuse Labs.
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

use core::fmt;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::usize;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::types::data_type::DataType;
use crate::types::data_type::DataTypeImpl;
use crate::DataField;
use crate::TypeDeserializerImpl;

/// memory layout.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct DataSchema {
    pub(crate) fields: Vec<DataField>,
    pub(crate) metadata: BTreeMap<String, String>,

    // Never serialized.
    #[serde(skip_serializing)]
    pub(crate) all_fields: Vec<(Option<usize>, DataField)>,

    #[serde(skip_serializing)]
    pub(crate) column_id_of_index: Vec<usize>,
}

impl DataSchema {
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: BTreeMap::new(),
            all_fields: vec![],
            column_id_of_index: vec![],
        }
    }

    fn build_all_fields(
        fields: Vec<DataField>,
    ) -> (Vec<DataField>, Vec<(Option<usize>, DataField)>, Vec<usize>) {
        let mut all_fields = Vec::with_capacity(fields.len());
        let mut undeleted_fields = vec![];
        let mut column_id_of_index = vec![];
        fields.into_iter().for_each(|f| {
            if f.is_deleted() {
                all_fields.push((None, f));
            } else {
                undeleted_fields.push(f.clone());
                all_fields.push((Some(undeleted_fields.len() - 1), f));
                column_id_of_index.push(all_fields.len() - 1);
            }
        });

        (undeleted_fields, all_fields, column_id_of_index)
    }

    pub fn new(fields: Vec<DataField>) -> Self {
        let (fields, all_fields, column_id_of_index) = DataSchema::build_all_fields(fields);
        Self {
            fields,
            metadata: BTreeMap::new(),
            all_fields,
            column_id_of_index,
        }
    }

    pub fn new_from(fields: Vec<DataField>, metadata: BTreeMap<String, String>) -> Self {
        let (fields, all_fields, column_id_of_index) = DataSchema::build_all_fields(fields);
        Self {
            fields,
            metadata,
            all_fields,
            column_id_of_index,
        }
    }

    pub fn new_from_project(&self, projection: &[usize]) -> Self {
        // first clone and reset all the index to None
        let mut all_fields = self.all_fields.clone();
        for fields in all_fields.iter_mut() {
            fields.0 = None;
        }

        // re-calculate column of index
        let mut fields = Vec::with_capacity(projection.len());
        let mut column_id_of_index = Vec::with_capacity(projection.len());
        for i in projection {
            fields.push(all_fields[*i].1.clone());
            all_fields[*i].0 = Some(fields.len() - 1);
            column_id_of_index.push(*i);
        }

        Self {
            fields,
            metadata: self.metadata.clone(),
            all_fields,
            column_id_of_index,
        }
    }

    fn delete_field(&mut self, idx: usize) {
        // mark field i has been deleted and move all the fields after i forward.
        for i in idx..self.fields.len() {
            let field = &self.fields[i];
            for all_field in self.all_fields.iter_mut() {
                if all_field.0 == Some(i) {
                    if field.is_deleted() {
                        all_field.0 = None;
                        all_field.1.tag_delete();
                    } else {
                        // move all the fields after idx forward
                        all_field.0 = Some(i - 1);
                    }
                }
            }
        }

        // remove from fields array
        self.fields.remove(idx);
        self.column_id_of_index.remove(idx);
    }

    #[inline]
    pub fn all_fields(&self) -> &Vec<(Option<usize>, DataField)> {
        &self.all_fields
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub fn fields(&self) -> &Vec<DataField> {
        &self.fields
    }

    #[inline]
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    #[inline]
    pub fn has_field(&self, name: &str) -> bool {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return true;
            }
        }
        false
    }

    pub fn fields_map(&self) -> BTreeMap<usize, DataField> {
        let x = self.fields().iter().cloned().enumerate();
        x.collect::<BTreeMap<_, _>>()
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: usize) -> &DataField {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&DataField> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Returns an immutable reference to field `metadata`.
    #[inline]
    pub const fn meta(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return Ok(i);
            }
        }
        let valid_fields: Vec<String> = self.fields.iter().map(|f| f.name().clone()).collect();
        Err(ErrorCode::BadArguments(format!(
            "Unable to get field named \"{}\". Valid fields: {:?}",
            name, valid_fields
        )))
    }

    /// Find the column id with the given name.
    pub fn column_id_of(&self, name: &str) -> Result<usize> {
        let i = self.index_of(name)?;
        Ok(self.column_id_of_index[i])
    }

    pub fn column_id_of_index(&self, i: usize) -> usize {
        self.column_id_of_index[i]
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &DataField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name() == name)
    }

    /// Check to see if `self` is a superset of `other` schema. Here are the comparision rules:
    pub fn contains(&self, other: &DataSchema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (i, field) in other.fields.iter().enumerate() {
            if !self.fields[i].contains(field) {
                return false;
            }
        }
        true
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project(&self, projection: &[usize]) -> Self {
        self.new_from_project(projection)
    }

    /// project with inner columns by path.
    pub fn inner_project(&self, path_indices: &BTreeMap<usize, Vec<usize>>) -> Self {
        let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
        let fields = paths
            .iter()
            .map(|path| Self::traverse_paths(&self.fields(), path, Some(&self.all_fields)).unwrap())
            .collect();
        Self::new_from(fields, self.meta().clone())
    }

    fn traverse_paths(
        fields: &[DataField],
        path: &[usize],
        all_fields: Option<&Vec<(Option<usize>, DataField)>>,
    ) -> Result<DataField> {
        if path.is_empty() {
            return Err(ErrorCode::BadArguments("path should not be empty"));
        }
        // project need to convert from column id to field array index
        let idx = match all_fields {
            Some(all_fields) => all_fields[path[0]].0.unwrap(),
            None => path[0],
        };
        let field = &fields[idx];
        if path.len() == 1 {
            return Ok(field.clone());
        }

        let field_name = field.name();
        if let DataTypeImpl::Struct(struct_type) = &field.data_type() {
            let inner_types = struct_type.types();
            let inner_names = match struct_type.names() {
                Some(inner_names) => inner_names
                    .iter()
                    .map(|name| format!("{}:{}", field_name, name.to_lowercase()))
                    .collect::<Vec<_>>(),
                None => (0..inner_types.len())
                    .map(|i| format!("{}:{}", field_name, i + 1))
                    .collect::<Vec<_>>(),
            };

            let inner_fields = inner_names
                .iter()
                .zip(inner_types.iter())
                .map(|(inner_name, inner_type)| {
                    DataField::new(&inner_name.clone(), inner_type.clone())
                })
                .collect::<Vec<DataField>>();
            return Self::traverse_paths(&inner_fields, &path[1..], None);
        }
        let valid_fields: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        Err(ErrorCode::BadArguments(format!(
            "Unable to get field paths. Valid fields: {:?}",
            valid_fields
        )))
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project_by_fields(&self, fields: Vec<DataField>) -> Self {
        Self::new_from(fields, self.meta().clone())
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields = self
            .fields()
            .iter()
            .map(|f| f.to_arrow())
            .collect::<Vec<_>>();

        ArrowSchema::from(fields).with_metadata(self.metadata.clone())
    }

    pub fn create_deserializers(&self, capacity: usize) -> Vec<TypeDeserializerImpl> {
        let mut deserializers = Vec::with_capacity(self.num_fields());
        for field in self.fields() {
            let data_type = field.data_type();
            deserializers.push(data_type.create_deserializer(capacity));
        }
        deserializers
    }

    pub fn add_columns(&mut self, fields: Vec<DataField>) {
        fields.into_iter().for_each(|f| {
            assert!(f.is_added());
            self.fields.push(f.clone());
            self.all_fields.push((Some(self.fields.len() - 1), f));
            self.column_id_of_index.push(self.all_fields.len() - 1);
        });
    }

    pub fn drop_column(&mut self, column: &str) -> Result<()> {
        let i = self.index_of(column)?;
        let field = &mut self.fields[i];
        field.tag_delete();
        self.delete_field(i);
        Ok(())
    }
}

pub type DataSchemaRef = Arc<DataSchema>;

pub struct DataSchemaRefExt;

impl DataSchemaRefExt {
    pub fn create(fields: Vec<DataField>) -> DataSchemaRef {
        Arc::new(DataSchema::new(fields))
    }
}

impl From<&ArrowSchema> for DataSchema {
    fn from(a_schema: &ArrowSchema) -> Self {
        let fields = a_schema
            .fields
            .iter()
            .map(|arrow_f| arrow_f.into())
            .collect::<Vec<_>>();

        DataSchema::new(fields)
    }
}

#[allow(clippy::needless_borrow)]
impl From<ArrowSchema> for DataSchema {
    fn from(a_schema: ArrowSchema) -> Self {
        (&a_schema).into()
    }
}

impl From<ArrowSchemaRef> for DataSchema {
    fn from(a_schema: ArrowSchemaRef) -> Self {
        (a_schema.as_ref()).into()
    }
}

impl fmt::Display for DataSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(
            format!(
                "fields:{:?}, all_fields:{:?}",
                &self
                    .fields
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>()
                    .join(", "),
                &self
                    .all_fields
                    .iter()
                    .map(|c| format!("(i:{:?}, field:{:?})", c.0, c.1))
                    .collect::<Vec<String>>()
                    .join(", "),
            )
            .as_str(),
        )
    }
}
