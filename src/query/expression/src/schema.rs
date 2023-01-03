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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;
use common_jsonb::Number as JsonbNumber;
use common_jsonb::Object as JsonbObject;
use common_jsonb::Value as JsonbValue;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use serde::Deserialize;
use serde::Serialize;

use crate::types::array::ArrayColumn;
use crate::types::date::DATE_MAX;
use crate::types::date::DATE_MIN;
use crate::types::nullable::NullableColumn;
use crate::types::timestamp::TIMESTAMP_MAX;
use crate::types::timestamp::TIMESTAMP_MIN;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::utils::from_date_data;
use crate::utils::from_timestamp_data;
use crate::utils::ColumnFrom;
use crate::with_number_mapped_type;
use crate::with_number_type;
use crate::BlockEntry;
use crate::Column;
use crate::TypeDeserializerImpl;
use crate::Value;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_VARIANT;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DataSchema {
    pub(crate) fields: Vec<DataField>,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataField {
    name: String,
    default_expr: Option<String>,
    data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TableSchema {
    pub(crate) fields: Vec<TableField>,
    pub(crate) metadata: BTreeMap<String, String>,
    pub(crate) max_column_id: u32,

    // map column id to fields index
    #[serde(skip_serializing)]
    pub(crate) index_of_column_id: HashMap<u32, u32>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableField {
    name: String,
    default_expr: Option<String>,
    data_type: TableDataType,
    column_id: Option<u32>,
}

/// DataType with more information that is only available for table field, e.g, the
/// tuple field name, or the scale of decimal.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum TableDataType {
    Null,
    EmptyArray,
    Boolean,
    String,
    Number(NumberDataType),
    Timestamp,
    Date,
    Nullable(Box<TableDataType>),
    Array(Box<TableDataType>),
    Map(Box<TableDataType>),
    Tuple {
        fields_name: Vec<String>,
        fields_type: Vec<TableDataType>,
    },
    Variant,
}

impl DataSchema {
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: BTreeMap::new(),
        }
    }

    pub fn new(fields: Vec<DataField>) -> Self {
        Self {
            fields,
            metadata: BTreeMap::new(),
        }
    }

    pub fn new_from(fields: Vec<DataField>, metadata: BTreeMap<String, String>) -> Self {
        Self { fields, metadata }
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Vec<DataField> {
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
            if &self.fields[i] != field {
                return false;
            }
        }
        true
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project(&self, projection: &[usize]) -> Self {
        let fields = projection
            .iter()
            .map(|idx| self.fields()[*idx].clone())
            .collect();
        Self::new_from(fields, self.meta().clone())
    }

    /// project with inner columns by path.
    // pub fn inner_project(&self, path_indices: &BTreeMap<usize, Vec<usize>>) -> Self {
    //     let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
    //     let fields = paths
    //         .iter()
    //         .map(|path| Self::traverse_paths(self.fields(), path).unwrap())
    //         .collect();
    //     Self::new_from(fields, self.meta().clone())
    // }
    //
    // fn traverse_paths(fields: &[DataField], path: &[usize]) -> Result<DataField> {
    //     if path.is_empty() {
    //         return Err(ErrorCode::BadArguments(
    //             "path should not be empty".to_string(),
    //         ));
    //     }
    //     let field = &fields[path[0]];
    //     if path.len() == 1 {
    //         return Ok(field.clone());
    //     }
    //
    //     if let TableDataType::Tuple {
    //         fields_name,
    //         fields_type,
    //     } = &field.data_type()
    //     {
    //         let fields = fields_name
    //             .iter()
    //             .zip(fields_type)
    //             .map(|(name, ty)| DataField::new(&name.clone(), ty.clone()))
    //             .collect::<Vec<DataField>>();
    //         return Self::traverse_paths(&fields, &path[1..]);
    //     }
    //     let valid_fields: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
    //     Err(ErrorCode::BadArguments(format!(
    //         "Unable to get field paths. Valid fields: {:?}",
    //         valid_fields
    //     )))
    // }

    /// project will do column pruning.
    #[must_use]
    pub fn project_by_fields(&self, fields: Vec<DataField>) -> Self {
        Self::new_from(fields, self.meta().clone())
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields = self.fields().iter().map(|f| f.into()).collect::<Vec<_>>();

        ArrowSchema::from(fields).with_metadata(self.metadata.clone())
    }

    pub fn create_deserializers(&self, capacity: usize) -> Vec<TypeDeserializerImpl> {
        let mut deserializers = Vec::with_capacity(self.num_fields());
        for field in self.fields() {
            deserializers.push(field.data_type.create_deserializer(capacity));
        }
        deserializers
    }
}

impl TableSchema {
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: BTreeMap::new(),
            max_column_id: 0,
            index_of_column_id: HashMap::new(),
        }
    }

    fn build_members_from_fields(
        fields: Vec<TableField>,
        max_id: Option<u32>,
    ) -> (u32, HashMap<u32, u32>, Vec<TableField>) {
        let mut max_column_id = 0;
        let mut index_of_column_id = HashMap::new();
        let mut mut_fields = Vec::with_capacity(fields.len());
        fields.into_iter().enumerate().for_each(|(i, f)| {
            let (column_id, field) = match f.column_id {
                Some(column_id) => (column_id, f),
                None => {
                    let i = i as u32;
                    let mut field = f;
                    field.column_id = Some(i);
                    (i, field)
                }
            };
            mut_fields.push(field);
            if column_id > max_column_id {
                max_column_id = column_id;
            }
            index_of_column_id.insert(column_id, i as u32);
        });
        (
            max_id.unwrap_or(max_column_id),
            index_of_column_id,
            mut_fields,
        )
    }

    pub fn new(fields: Vec<TableField>) -> Self {
        let (max_column_id, index_of_column_id, fields) =
            Self::build_members_from_fields(fields, None);
        Self {
            fields,
            metadata: BTreeMap::new(),
            max_column_id,
            index_of_column_id,
        }
    }

    pub fn new_from(fields: Vec<TableField>, metadata: BTreeMap<String, String>) -> Self {
        let (max_column_id, index_of_column_id, fields) =
            Self::build_members_from_fields(fields, None);
        Self {
            fields,
            metadata,
            max_column_id,
            index_of_column_id,
        }
    }

    pub fn new_from_with_max_column_id(
        fields: Vec<TableField>,
        metadata: BTreeMap<String, String>,
        max_column_id: u32,
    ) -> Self {
        let (max_column_id, index_of_column_id, fields) =
            Self::build_members_from_fields(fields, Some(max_column_id));
        Self {
            fields,
            metadata,
            max_column_id,
            index_of_column_id,
        }
    }

    #[inline]
    pub const fn max_column_id(&self) -> u32 {
        self.max_column_id
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Vec<TableField> {
        &self.fields
    }

    #[inline]
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    #[inline]
    pub fn has_field(&self, name: &str) -> bool {
        for i in 0..self.fields.len() {
            if self.fields[i].name == name {
                return true;
            }
        }
        false
    }

    pub fn fields_map(&self) -> BTreeMap<usize, TableField> {
        let x = self.fields().iter().cloned().enumerate();
        x.collect::<BTreeMap<_, _>>()
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: usize) -> &TableField {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&TableField> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Returns an immutable reference to field `metadata`.
    #[inline]
    pub const fn meta(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Find the column id with the given name.
    pub fn column_id_of(&self, name: &str) -> Result<u32> {
        let i = self.index_of(name)?;
        Ok(self.fields[i].column_id().unwrap())
    }

    pub fn column_id_of_index(&self, i: usize) -> u32 {
        self.fields[i].column_id().unwrap()
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name == name {
                return Ok(i);
            }
        }
        let valid_fields: Vec<String> = self.fields.iter().map(|f| f.name.clone()).collect();

        Err(ErrorCode::BadArguments(format!(
            "Unable to get field named \"{}\". Valid fields: {:?}",
            name, valid_fields
        )))
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &TableField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name == name)
    }

    /// Check to see if `self` is a superset of `other` schema. Here are the comparision rules:
    pub fn contains(&self, other: &TableSchema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (i, field) in other.fields.iter().enumerate() {
            if &self.fields[i] != field {
                return false;
            }
        }
        true
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project(&self, projection: &[usize]) -> Self {
        let fields = projection
            .iter()
            .map(|idx| self.fields()[*idx].clone())
            .collect();
        Self::new_from(fields, self.meta().clone())
    }

    /// project with inner columns by path.
    pub fn inner_project(&self, path_indices: &BTreeMap<usize, Vec<usize>>) -> Self {
        let paths: Vec<Vec<usize>> = path_indices.values().cloned().collect();
        let fields = paths
            .iter()
            .map(|path| Self::traverse_paths(self.fields(), path).unwrap())
            .collect();
        Self::new_from(fields, self.meta().clone())
    }

    fn traverse_paths(fields: &[TableField], path: &[usize]) -> Result<TableField> {
        if path.is_empty() {
            return Err(ErrorCode::BadArguments(
                "path should not be empty".to_string(),
            ));
        }
        let field = &fields[path[0]];
        if path.len() == 1 {
            return Ok(field.clone());
        }

        if let TableDataType::Tuple {
            fields_name,
            fields_type,
        } = &field.data_type
        {
            let fields = fields_name
                .iter()
                .zip(fields_type)
                .map(|(name, ty)| TableField::new(&name.clone(), ty.clone()))
                .collect::<Vec<_>>();
            return Self::traverse_paths(&fields, &path[1..]);
        }
        let valid_fields: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
        Err(ErrorCode::BadArguments(format!(
            "Unable to get field paths. Valid fields: {:?}",
            valid_fields
        )))
    }

    pub fn leaf_fields(&self) -> Vec<TableField> {
        fn collect_in_field(field: &TableField, fields: &mut Vec<TableField>) {
            match field.data_type() {
                TableDataType::Tuple {
                    fields_type,
                    fields_name,
                } => {
                    for (name, ty) in fields_name.iter().zip(fields_type) {
                        collect_in_field(&TableField::new(name, ty.clone()), fields);
                    }
                }
                _ => fields.push(field.clone()),
            }
        }

        let mut fields = Vec::new();
        for field in self.fields() {
            collect_in_field(field, &mut fields);
        }
        fields
    }

    /// project will do column pruning.
    #[must_use]
    pub fn project_by_fields(&self, fields: Vec<TableField>) -> Self {
        Self::new_from(fields, self.meta().clone())
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields = self.fields().iter().map(|f| f.into()).collect::<Vec<_>>();

        ArrowSchema::from(fields).with_metadata(self.metadata.clone())
    }

    pub fn create_deserializers(&self, capacity: usize) -> Vec<TypeDeserializerImpl> {
        let mut deserializers = Vec::with_capacity(self.num_fields());
        for field in self.fields() {
            let data_type: DataType = field.data_type().into();
            deserializers.push(data_type.create_deserializer(capacity));
        }
        deserializers
    }

    pub fn add_column(&mut self, field: &TableField) {
        let mut field = field.clone();
        field.column_id = Some(self.max_column_id);
        self.index_of_column_id
            .insert(self.max_column_id, self.fields.len() as u32);
        self.fields.push(field);
        self.max_column_id += 1;
    }

    pub fn drop_column(&mut self, column: &str) -> Result<()> {
        let i = self.index_of(column)?;
        let field = &self.fields[i];
        if let Some(column_id) = field.column_id() {
            self.index_of_column_id.remove(&column_id);
        }
        self.fields.remove(i);
        Ok(())
    }
}

impl DataField {
    pub fn new(name: &str, data_type: DataType) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type,
        }
    }

    pub fn new_nullable(name: &str, data_type: DataType) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type: DataType::Nullable(Box::new(data_type)),
        }
    }

    #[must_use]
    pub fn with_default_expr(mut self, default_expr: Option<String>) -> Self {
        self.default_expr = default_expr;
        self
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn default_expr(&self) -> Option<&String> {
        self.default_expr.as_ref()
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.data_type.is_nullable()
    }

    #[inline]
    pub fn is_nullable_or_null(&self) -> bool {
        self.data_type.is_nullable_or_null()
    }
}

impl TableField {
    pub fn new(name: &str, data_type: TableDataType) -> Self {
        TableField {
            name: name.to_string(),
            default_expr: None,
            data_type,
            column_id: None,
        }
    }

    pub fn new_with_column_id(name: &str, data_type: TableDataType, column_id: u32) -> Self {
        TableField {
            name: name.to_string(),
            default_expr: None,
            data_type,
            column_id: Some(column_id),
        }
    }

    #[must_use]
    pub fn with_default_expr(mut self, default_expr: Option<String>) -> Self {
        self.default_expr = default_expr;
        self
    }

    pub fn column_id(&self) -> Option<u32> {
        self.column_id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &TableDataType {
        &self.data_type
    }

    pub fn default_expr(&self) -> Option<&String> {
        self.default_expr.as_ref()
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.data_type.is_nullable()
    }

    #[inline]
    pub fn is_nullable_or_null(&self) -> bool {
        self.data_type.is_nullable_or_null()
    }
}

impl From<&TableDataType> for DataType {
    fn from(data_type: &TableDataType) -> DataType {
        match data_type {
            TableDataType::Null => DataType::Null,
            TableDataType::EmptyArray => DataType::EmptyArray,
            TableDataType::Boolean => DataType::Boolean,
            TableDataType::String => DataType::String,
            TableDataType::Number(ty) => DataType::Number(*ty),
            TableDataType::Timestamp => DataType::Timestamp,
            TableDataType::Date => DataType::Date,
            TableDataType::Nullable(ty) => DataType::Nullable(Box::new((&**ty).into())),
            TableDataType::Array(ty) => DataType::Array(Box::new((&**ty).into())),
            TableDataType::Map(ty) => DataType::Map(Box::new((&**ty).into())),
            TableDataType::Tuple { fields_type, .. } => {
                DataType::Tuple(fields_type.iter().map(Into::into).collect())
            }
            TableDataType::Variant => DataType::Variant,
        }
    }
}

impl TableDataType {
    pub fn wrap_nullable(&self) -> Self {
        match self {
            TableDataType::Nullable(_) => self.clone(),
            _ => Self::Nullable(Box::new(self.clone())),
        }
    }

    pub fn is_nullable(&self) -> bool {
        matches!(self, &TableDataType::Nullable(_))
    }

    pub fn is_nullable_or_null(&self) -> bool {
        matches!(self, &TableDataType::Nullable(_) | &TableDataType::Null)
    }

    pub fn can_inside_nullable(&self) -> bool {
        !self.is_nullable_or_null()
    }

    pub fn remove_nullable(&self) -> Self {
        match self {
            TableDataType::Nullable(ty) => (**ty).clone(),
            _ => self.clone(),
        }
    }

    pub fn sql_name(&self) -> String {
        match self {
            TableDataType::Number(num_ty) => match num_ty {
                NumberDataType::UInt8 => "TINYINT UNSIGNED".to_string(),
                NumberDataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
                NumberDataType::UInt32 => "INT UNSIGNED".to_string(),
                NumberDataType::UInt64 => "BIGINT UNSIGNED".to_string(),
                NumberDataType::Int8 => "TINYINT".to_string(),
                NumberDataType::Int16 => "SMALLINT".to_string(),
                NumberDataType::Int32 => "INT".to_string(),
                NumberDataType::Int64 => "BIGINT".to_string(),
                NumberDataType::Float32 => "FLOAT".to_string(),
                NumberDataType::Float64 => "DOUBLE".to_string(),
            },
            TableDataType::String => "VARCHAR".to_string(),
            TableDataType::Nullable(inner_ty) => format!("{} NULL", inner_ty.sql_name()),
            _ => self.to_string().to_uppercase(),
        }
    }

    pub fn create_random_column(&self, len: usize) -> BlockEntry {
        match self {
            TableDataType::Null => BlockEntry {
                data_type: DataType::Null,
                value: Value::Column(Column::Null { len }),
            },
            TableDataType::EmptyArray => BlockEntry {
                data_type: DataType::EmptyArray,
                value: Value::Column(Column::EmptyArray { len }),
            },
            TableDataType::Boolean => BlockEntry {
                data_type: DataType::Boolean,
                value: Value::Column(Column::from_data(
                    (0..len)
                        .map(|_| SmallRng::from_entropy().gen_bool(0.5))
                        .collect::<Vec<bool>>(),
                )),
            },
            TableDataType::String => BlockEntry {
                data_type: DataType::String,
                value: Value::Column(Column::from_data(
                    (0..len)
                        .map(|_| {
                            let rng = SmallRng::from_entropy();
                            rng.sample_iter(&Alphanumeric)
                                // randomly generate 5 characters.
                                .take(5)
                                .map(u8::from)
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<Vec<u8>>>(),
                )),
            },
            TableDataType::Number(num_ty) => BlockEntry {
                data_type: DataType::Number(*num_ty),
                value: Value::Column(with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => Column::from_data(
                        (0..len)
                            .map(|_| SmallRng::from_entropy().gen())
                            .collect::<Vec<NUM_TYPE>>(),
                    ),
                })),
            },
            TableDataType::Timestamp => BlockEntry {
                data_type: DataType::Timestamp,
                value: Value::Column(from_timestamp_data(
                    (0..len)
                        .map(|_| SmallRng::from_entropy().gen_range(TIMESTAMP_MIN..=TIMESTAMP_MAX))
                        .collect::<Vec<i64>>(),
                )),
            },
            TableDataType::Date => BlockEntry {
                data_type: DataType::Date,
                value: Value::Column(from_date_data(
                    (0..len)
                        .map(|_| SmallRng::from_entropy().gen_range(DATE_MIN..=DATE_MAX))
                        .collect::<Vec<i32>>(),
                )),
            },
            TableDataType::Nullable(inner_ty) => {
                let entry = inner_ty.create_random_column(len);
                BlockEntry {
                    data_type: DataType::Nullable(Box::new(entry.data_type)),
                    value: Value::Column(Column::Nullable(Box::new(NullableColumn {
                        column: entry.value.into_column().unwrap(),
                        validity: Bitmap::from(
                            (0..len)
                                .map(|_| SmallRng::from_entropy().gen_bool(0.5))
                                .collect::<Vec<bool>>(),
                        ),
                    }))),
                }
            }
            TableDataType::Array(inner_ty) => {
                let mut inner_len = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(len + 1);
                offsets.push(inner_len);
                for _ in 0..len {
                    inner_len += SmallRng::from_entropy().gen_range(0..=3);
                    offsets.push(inner_len);
                }
                let entry = inner_ty.create_random_column(inner_len as usize);
                BlockEntry {
                    data_type: DataType::Array(Box::new(entry.data_type)),
                    value: Value::Column(Column::Array(Box::new(ArrayColumn {
                        values: entry.value.into_column().unwrap(),
                        offsets: offsets.into(),
                    }))),
                }
            }
            TableDataType::Tuple { fields_type, .. } => {
                let mut fields = Vec::with_capacity(len);
                let mut types = Vec::with_capacity(len);
                for field_type in fields_type.iter() {
                    let entry = field_type.create_random_column(len);
                    fields.push(entry.value.into_column().unwrap());
                    types.push(entry.data_type);
                }
                BlockEntry {
                    data_type: DataType::Tuple(types),
                    value: Value::Column(Column::Tuple { fields, len }),
                }
            }
            TableDataType::Variant => {
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    let opt = SmallRng::from_entropy().gen_range(0..=6);
                    let val = match opt {
                        0 => JsonbValue::Null,
                        1 => JsonbValue::Bool(true),
                        2 => JsonbValue::Bool(false),
                        3 => {
                            let s = Alphanumeric.sample_string(&mut rand::thread_rng(), 5);
                            JsonbValue::String(Cow::from(s))
                        }
                        4 => {
                            let num = SmallRng::from_entropy().gen_range(i64::MIN..=i64::MAX);
                            JsonbValue::Number(JsonbNumber::Int64(num))
                        }
                        5 => {
                            let arr_len = SmallRng::from_entropy().gen_range(0..=5);
                            let mut values = Vec::with_capacity(arr_len);
                            for _ in 0..arr_len {
                                let num = SmallRng::from_entropy().gen_range(i64::MIN..=i64::MAX);
                                values.push(JsonbValue::Number(JsonbNumber::Int64(num)))
                            }
                            JsonbValue::Array(values)
                        }
                        6 => {
                            let obj_len = SmallRng::from_entropy().gen_range(0..=5);
                            let mut obj = JsonbObject::new();
                            for _ in 0..obj_len {
                                let k = Alphanumeric.sample_string(&mut rand::thread_rng(), 5);
                                let num = SmallRng::from_entropy().gen_range(i64::MIN..=i64::MAX);
                                let v = JsonbValue::Number(JsonbNumber::Int64(num));
                                obj.insert(k, v);
                            }
                            JsonbValue::Object(obj)
                        }
                        _ => JsonbValue::Null,
                    };
                    data.push(val.to_vec());
                }
                BlockEntry {
                    data_type: DataType::Variant,
                    value: Value::Column(Column::from_data(data)),
                }
            }
            _ => todo!(),
        }
    }
}

pub type DataSchemaRef = Arc<DataSchema>;
pub type TableSchemaRef = Arc<TableSchema>;

pub struct DataSchemaRefExt;

pub struct TableSchemaRefExt;

impl DataSchemaRefExt {
    pub fn create(fields: Vec<DataField>) -> DataSchemaRef {
        Arc::new(DataSchema::new(fields))
    }
}

impl TableSchemaRefExt {
    pub fn create(fields: Vec<TableField>) -> TableSchemaRef {
        Arc::new(TableSchema::new(fields))
    }
}

impl From<&ArrowSchema> for TableSchema {
    fn from(a_schema: &ArrowSchema) -> Self {
        let fields = a_schema
            .fields
            .iter()
            .map(|arrow_f| arrow_f.into())
            .collect::<Vec<_>>();

        TableSchema::new(fields)
    }
}

impl From<&TableField> for DataField {
    fn from(f: &TableField) -> Self {
        let data_type = f.data_type.clone();
        let name = f.name.clone();
        DataField::new(&name, DataType::from(&data_type)).with_default_expr(f.default_expr.clone())
    }
}

impl<T: AsRef<TableSchema>> From<T> for DataSchema {
    fn from(t_schema: T) -> Self {
        let fields = t_schema
            .as_ref()
            .fields()
            .iter()
            .map(|t_f| t_f.into())
            .collect::<Vec<_>>();

        DataSchema::new(fields)
    }
}

impl AsRef<TableSchema> for &TableSchema {
    fn as_ref(&self) -> &TableSchema {
        self
    }
}

// conversions code
// =========================
impl From<&ArrowField> for TableField {
    fn from(f: &ArrowField) -> Self {
        Self {
            name: f.name.clone(),
            data_type: f.into(),
            default_expr: None,
            column_id: None,
        }
    }
}

impl From<&ArrowField> for DataField {
    fn from(f: &ArrowField) -> Self {
        Self {
            name: f.name.clone(),
            data_type: DataType::from(&TableDataType::from(f)),
            default_expr: None,
        }
    }
}

// ArrowType can't map to DataType, we don't know the nullable flag
impl From<&ArrowField> for TableDataType {
    fn from(f: &ArrowField) -> Self {
        let ty = with_number_type!(|TYPE| match f.data_type() {
            ArrowDataType::TYPE => TableDataType::Number(NumberDataType::TYPE),

            ArrowDataType::Null => return TableDataType::Null,
            ArrowDataType::Boolean => TableDataType::Boolean,

            ArrowDataType::List(f)
            | ArrowDataType::LargeList(f)
            | ArrowDataType::FixedSizeList(f, _) =>
                TableDataType::Array(Box::new(f.as_ref().into())),

            ArrowDataType::Binary
            | ArrowDataType::LargeBinary
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8 => TableDataType::String,

            ArrowDataType::Timestamp(_, _) => TableDataType::Timestamp,
            ArrowDataType::Date32 | ArrowDataType::Date64 => TableDataType::Date,

            ArrowDataType::Struct(fields) => {
                let (fields_name, fields_type) =
                    fields.iter().map(|f| (f.name.clone(), f.into())).unzip();
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                }
            }
            ArrowDataType::Extension(custom_name, _, _) => match custom_name.as_str() {
                ARROW_EXT_TYPE_VARIANT => TableDataType::Variant,
                ARROW_EXT_TYPE_EMPTY_ARRAY => TableDataType::EmptyArray,
                _ => unimplemented!("data_type: {:?}", f.data_type()),
            },
            // this is safe, because we define the datatype firstly
            _ => {
                unimplemented!("data_type: {:?}", f.data_type())
            }
        });

        if f.is_nullable {
            TableDataType::Nullable(Box::new(ty))
        } else {
            ty
        }
    }
}

impl From<&DataField> for ArrowField {
    fn from(f: &DataField) -> Self {
        let ty = f.data_type().into();
        ArrowField::new(f.name(), ty, f.is_nullable())
    }
}

impl From<&TableField> for ArrowField {
    fn from(f: &TableField) -> Self {
        let ty = f.data_type().into();
        ArrowField::new(f.name(), ty, f.is_nullable())
    }
}

impl From<&DataType> for ArrowDataType {
    fn from(ty: &DataType) -> Self {
        match ty {
            DataType::Null => ArrowDataType::Null,
            DataType::EmptyArray => ArrowDataType::Extension(
                ARROW_EXT_TYPE_EMPTY_ARRAY.to_string(),
                Box::new(ArrowDataType::Null),
                None,
            ),
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::String => ArrowDataType::LargeBinary,
            DataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Date => ArrowDataType::Date32,
            DataType::Nullable(ty) => ty.as_ref().into(),
            DataType::Array(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_array",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            DataType::Map(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_map",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            DataType::Tuple(types) => {
                let fields = types
                    .iter()
                    .map(|ty| ArrowField::new("DUMMY_FIELD_NAME", ty.into(), ty.is_nullable()))
                    .collect();
                ArrowDataType::Struct(fields)
            }
            DataType::Variant => ArrowDataType::Extension(
                ARROW_EXT_TYPE_VARIANT.to_string(),
                Box::new(ArrowDataType::LargeBinary),
                None,
            ),

            _ => unreachable!(),
        }
    }
}

impl From<&TableDataType> for ArrowDataType {
    fn from(ty: &TableDataType) -> Self {
        match ty {
            TableDataType::Null => ArrowDataType::Null,
            TableDataType::EmptyArray => ArrowDataType::Extension(
                ARROW_EXT_TYPE_EMPTY_ARRAY.to_string(),
                Box::new(ArrowDataType::Null),
                None,
            ),
            TableDataType::Boolean => ArrowDataType::Boolean,
            TableDataType::String => ArrowDataType::LargeBinary,
            TableDataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            TableDataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            TableDataType::Date => ArrowDataType::Date32,
            TableDataType::Nullable(ty) => ty.as_ref().into(),
            TableDataType::Array(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_array",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            TableDataType::Map(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_map",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                let fields = fields_name
                    .iter()
                    .zip(fields_type)
                    .map(|(name, ty)| ArrowField::new(name.as_str(), ty.into(), ty.is_nullable()))
                    .collect();
                ArrowDataType::Struct(fields)
            }
            TableDataType::Variant => ArrowDataType::Extension(
                ARROW_EXT_TYPE_VARIANT.to_string(),
                Box::new(ArrowDataType::LargeBinary),
                None,
            ),
        }
    }
}

/// Convert a `DataType` to `TableDataType`.
/// Generally, we don't allow to convert `DataType` to `TableDataType` directly.
/// But for some special cases, for example creating table from a query without specifying
/// the schema. Then we need to infer the corresponding `TableDataType` from `DataType`, and
/// this function may report an error if the conversion is not allowed.
///
/// Do not use this function in other places.
pub fn infer_schema_type(data_type: &DataType) -> Result<TableDataType> {
    match data_type {
        DataType::Null => Ok(TableDataType::Null),
        DataType::Boolean => Ok(TableDataType::Boolean),
        DataType::String => Ok(TableDataType::String),
        DataType::Number(number_type) => Ok(TableDataType::Number(*number_type)),
        DataType::Timestamp => Ok(TableDataType::Timestamp),
        DataType::Date => Ok(TableDataType::Date),
        DataType::Nullable(inner_type) => Ok(TableDataType::Nullable(Box::new(infer_schema_type(
            inner_type,
        )?))),
        DataType::Array(elem_type) => Ok(TableDataType::Array(Box::new(infer_schema_type(
            elem_type,
        )?))),
        DataType::Map(inner_type) => {
            Ok(TableDataType::Map(Box::new(infer_schema_type(inner_type)?)))
        }
        DataType::Variant => Ok(TableDataType::Variant),
        DataType::Tuple(fields) => {
            let fields_type = fields
                .iter()
                .map(infer_schema_type)
                .collect::<Result<Vec<_>>>()?;
            let fields_name = fields
                .iter()
                .enumerate()
                .map(|(idx, _)| idx.to_string())
                .collect::<Vec<_>>();
            Ok(TableDataType::Tuple {
                fields_name,
                fields_type,
            })
        }
        _ => Err(ErrorCode::SemanticError(format!(
            "Cannot create table with type: {}",
            data_type
        ))),
    }
}

/// Infer TableSchema from DataSchema, this is useful when creating table from a query.
pub fn infer_table_schema(data_schema: &DataSchemaRef) -> Result<TableSchemaRef> {
    let mut fields = Vec::with_capacity(data_schema.fields().len());
    for field in data_schema.fields() {
        let field_type = infer_schema_type(field.data_type())?;
        fields.push(TableField::new(field.name(), field_type));
    }
    Ok(TableSchemaRefExt::create(fields))
}
