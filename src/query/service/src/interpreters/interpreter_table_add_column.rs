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

use std::sync::Arc;

use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use common_sql::plans::AddTableColumnPlan;
use common_storages_view::view_table::VIEW_ENGINE;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalScalarBuilder;

pub struct AddTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: AddTableColumnPlan,
}

impl AddTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AddTableColumnPlan) -> Result<Self> {
        Ok(AddTableColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AddTableColumnInterpreter {
    fn name(&self) -> &str {
        "AddTableColumnInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = self
            .ctx
            .get_table(catalog_name, db_name, tbl_name)
            .await
            .ok();

        if let Some(table) = &tbl {
            let table_info = table.get_table_info();
            if table_info.engine() == VIEW_ENGINE {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{}.{} engine is VIEW that doesn't support alter",
                    &self.plan.database, &self.plan.table
                )));
            }
            if table_info.from_share.is_some() {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{}.{} doesn't support alter",
                    &self.plan.database, &self.plan.table
                )));
            }

            let catalog = self.ctx.get_catalog(catalog_name)?;
            let mut new_table_meta = table.get_table_info().meta.clone();
            let mut fields = Vec::with_capacity(self.plan.schema.num_fields());
            let input_schema = self.plan.schema.clone();
            for (idx, field) in self.plan.schema.fields().clone().into_iter().enumerate() {
                let mut field = if let Some(Some(scalar)) = &self.plan.field_default_exprs.get(idx)
                {
                    let mut builder = PhysicalScalarBuilder::new(&input_schema);
                    let physical_scaler = builder.build(scalar)?;
                    field.with_default_expr(Some(serde_json::to_string(&physical_scaler)?))
                } else {
                    field
                };
                // tag the field is added new column
                field.tag_add();
                fields.push(field)
            }
            let schema = DataSchemaRefExt::create(fields);
            new_table_meta.add_column(schema, &self.plan.field_comments);

            let table_id = table_info.ident.table_id;
            let table_version = table_info.ident.seq;

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Exact(table_version),
                new_table_meta,
            };

            let tenant = self.ctx.get_tenant();
            let db_name = self.ctx.get_current_database();
            catalog.update_table_meta(&tenant, &db_name, req).await?;
        };

        Ok(PipelineBuildResult::create())
    }
}
