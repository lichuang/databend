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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_license::license_manager::get_license_manager;
use common_sql::plans::DescDatamaskPolicyPlan;
use common_users::UserApiProvider;
use data_mask_feature::get_datamask_handler;
use tracing::warn;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DescDataMaskInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescDatamaskPolicyPlan,
}

impl DescDataMaskInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescDatamaskPolicyPlan) -> Result<Self> {
        Ok(DescDataMaskInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescDataMaskInterpreter {
    fn name(&self) -> &str {
        "DescDataMaskInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            self.ctx.get_tenant(),
            "data_mask".to_string(),
        )?;
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_datamask_handler();
        let policy = handler
            .get_data_mask(meta_api, self.ctx.get_tenant(), self.plan.name.clone())
            .await;

        let policy = match policy {
            Ok(policy) => policy,
            Err(err) => {
                warn!("DescDataMaskInterpreter err: {}", err);
                if err.code() != ErrorCode::UnknownDatamask("").code() {
                    return Err(err);
                }
                return Ok(PipelineBuildResult::create());
            }
        };

        let name: Vec<Vec<u8>> = vec![self.plan.name.as_bytes().to_vec()];
        let create_on: Vec<Vec<u8>> = vec![policy.create_on.to_string().as_bytes().to_vec()];
        let args = format!(
            "({})",
            policy
                .args
                .iter()
                .map(|(arg_name, arg_type)| format!("{} {}", arg_name, arg_type))
                .rev()
                .collect::<Vec<_>>()
                .join(",")
        );
        let signature: Vec<Vec<u8>> = vec![args.as_bytes().to_vec()];
        let return_type = vec![policy.return_type.as_bytes().to_vec()];
        let body = vec![policy.body.as_bytes().to_vec()];
        let comment = vec![match policy.comment {
            Some(comment) => comment.as_bytes().to_vec(),
            None => "".to_string().as_bytes().to_vec(),
        }];

        let blocks = vec![DataBlock::new_from_columns(vec![
            StringType::from_data(name),
            StringType::from_data(create_on),
            StringType::from_data(signature),
            StringType::from_data(return_type),
            StringType::from_data(body),
            StringType::from_data(comment),
        ])];
        PipelineBuildResult::from_blocks(blocks)
    }
}
