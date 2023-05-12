// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use common_ast::ast::DataMaskPolicy;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;

// Create Or replace data mask policy.
#[derive(Clone, Debug, PartialEq)]
pub struct CreateDatamaskPolicyPlan {
    pub create: bool,
    pub name: String,
    pub policy: DataMaskPolicy,
}

impl CreateDatamaskPolicyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
