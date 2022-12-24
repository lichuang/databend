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

use std::any::Any;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_types::UserIdentity;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::TransformLimit;
use common_storages_fuse::io::BlockReader;
use common_storages_fuse::FuseTable;
use common_storages_table_meta::meta::SegmentInfo;
use serde::Deserialize;
use serde::Serialize;

use crate::result_locations::ResultLocations;
use crate::result_table_source::ResultTableSource;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "t", content = "c")]
pub enum ResultStorageInfo {
    FuseSegment(SegmentInfo),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResultQueryInfo {
    pub query_id: String,
    pub schema: DataSchemaRef,
    pub user: UserIdentity,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResultTableMeta {
    pub query: ResultQueryInfo,
    pub storage: ResultStorageInfo,
}

impl ResultTableMeta {
    fn gen_table_info(&self) -> TableInfo {
        let name = format!("result_{}", self.query.query_id);
        TableInfo {
            desc: name.to_string(),
            name,
            meta: TableMeta {
                schema: self.query.schema.clone(),
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

pub struct ResultTable {
    #[allow(dead_code)]
    query_id: String,
    #[allow(dead_code)]
    meta: ResultTableMeta,
    pub(crate) locations: ResultLocations,
    table_info: TableInfo,
}

impl ResultTable {
    pub async fn try_get(ctx: Arc<dyn TableContext>, query_id: &str) -> Result<Arc<ResultTable>> {
        let locations = ResultLocations::new(query_id);
        let data_accessor = ctx.get_data_operator()?.operator();
        let location = locations.get_meta_location();
        let obj = data_accessor.object(&location);
        let data = match obj.read().await {
            Ok(d) => Ok(d),
            Err(e) => {
                if !obj.is_exist().await? {
                    Err(ErrorCode::HttpNotFound(format!(
                        "result for query_id {} not exists",
                        &query_id
                    )))
                } else {
                    Err(ErrorCode::from_std_error(e))
                }
            }
        }?;
        let meta: ResultTableMeta = serde_json::from_slice(&data)?;
        let table_info = meta.gen_table_info();

        Ok(Arc::new(Self {
            query_id: query_id.to_string(),
            meta,
            locations,
            table_info,
        }))
    }

    #[allow(unused)]
    fn create_block_reader(
        &self,
        ctx: &Arc<dyn TableContext>,
        _push_downs: &Option<PushDownInfo>,
    ) -> Result<Arc<BlockReader>> {
        let indices = (0..self.get_table_info().schema().fields().len())
            .into_iter()
            .collect::<Vec<usize>>();
        let projection = Projection::Columns(indices);

        let operator = ctx.get_data_operator()?.operator();
        let table_schema = self.get_table_info().schema();
        BlockReader::create(operator, table_schema, projection)
    }
}

#[async_trait::async_trait]
impl Table for ResultTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let data_accessor = ctx.get_data_operator()?.operator();
        let meta_location = self.locations.get_meta_location();
        let meta_data = data_accessor.object(&meta_location).read().await?;
        let meta: ResultTableMeta = serde_json::from_slice(&meta_data)?;
        let limit = push_downs.map_or(usize::MAX, |e| e.limit.unwrap_or(usize::MAX));
        match meta.storage {
            ResultStorageInfo::FuseSegment(seg) => Ok(FuseTable::all_columns_partitions(
                &self.meta.query.schema,
                &seg.blocks,
                limit,
            )),
        }
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let block_reader = self.create_block_reader(&ctx, &None)?;

        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(parts_len, max_threads);

        pipeline.add_source(
            |output| ResultTableSource::create(ctx.clone(), output, block_reader.clone()),
            std::cmp::max(1, max_threads),
        )?;

        match &plan.push_downs {
            None => Ok(()),
            Some(PushDownInfo { limit: None, .. }) => Ok(()),
            Some(PushDownInfo {
                limit: Some(limit), ..
            }) => {
                let limit = *limit;
                pipeline.add_transform(|transform_input, transform_output| {
                    TransformLimit::try_create(Some(limit), 0, transform_input, transform_output)
                })
            }
        }
    }
}
