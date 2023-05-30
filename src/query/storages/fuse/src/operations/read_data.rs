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

use std::collections::BTreeMap;
use std::println;
use std::sync::Arc;

use common_base::runtime::Runtime;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use storages_common_index::Index;
use storages_common_index::RangeIndex;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::io::BlockReader;
use crate::operations::fuse_source::build_fuse_source_pipeline;
use crate::operations::read::data_source_mask_transformer::DataSourceMaskTransformer;
use crate::pruning::SegmentLocation;
use crate::FuseTable;

impl FuseTable {
    pub fn create_block_reader(
        &self,
        projection: Projection,
        query_internal_columns: bool,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Arc<BlockReader>> {
        let table_schema = self.table_info.schema();
        BlockReader::create(
            self.operator.clone(),
            table_schema,
            projection,
            ctx,
            query_internal_columns,
        )
    }

    // Build the block reader.
    fn build_block_reader(
        &self,
        plan: &DataSourcePlan,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Arc<BlockReader>> {
        self.create_block_reader(
            PushDownInfo::projection_of_push_downs(&self.table_info.schema(), &plan.push_downs),
            plan.query_internal_columns,
            ctx,
        )
    }

    fn adjust_io_request(&self, ctx: &Arc<dyn TableContext>) -> Result<usize> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        if !self.operator.info().can_blocking() {
            Ok(std::cmp::max(max_threads, max_io_requests))
        } else {
            // For blocking fs, we don't want this to be too large
            Ok(std::cmp::min(max_threads, max_io_requests).clamp(1, 48))
        }
    }

    #[inline]
    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let snapshot_loc = plan.statistics.snapshot.clone();
        let mut lazy_init_segments = Vec::with_capacity(plan.parts.len());

        for part in &plan.parts.partitions {
            if let Some(lazy_part_info) = part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                lazy_init_segments.push(SegmentLocation {
                    segment_idx: lazy_part_info.segment_index,
                    location: lazy_part_info.segment_location.clone(),
                    snapshot_loc: snapshot_loc.clone(),
                });
            }
        }

        if !lazy_init_segments.is_empty() {
            let table = self.clone();
            let table_info = self.table_info.clone();
            let push_downs = plan.push_downs.clone();
            let query_ctx = ctx.clone();
            let dal = self.operator.clone();

            // TODO: need refactor
            pipeline.set_on_init(move || {
                let table = table.clone();
                let table_info = table_info.clone();
                let ctx = query_ctx.clone();
                let dal = dal.clone();
                let push_downs = push_downs.clone();
                // let lazy_init_segments = lazy_init_segments.clone();

                let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                    let (_statistics, partitions) = table
                        .prune_snapshot_blocks(
                            ctx,
                            dal,
                            push_downs,
                            table_info,
                            lazy_init_segments,
                            0,
                        )
                        .await?;

                    Result::<_, ErrorCode>::Ok(partitions)
                })?;

                query_ctx.set_partitions(partitions)?;
                Ok(())
            });
        }

        let block_reader = self.build_block_reader(plan, ctx.clone())?;
        let max_io_requests = self.adjust_io_request(&ctx)?;

        let topk = plan.push_downs.as_ref().and_then(|x| {
            x.top_k(
                plan.schema().as_ref(),
                self.cluster_key_str(),
                RangeIndex::supported_type,
            )
        });

        build_fuse_source_pipeline(
            ctx.clone(),
            pipeline,
            self.storage_format,
            block_reader,
            plan,
            topk,
            max_io_requests,
        )?;

        if let Some(data_mask_policy) = &plan.data_mask_policy {
            // let tr = DataSourceMaskTransformer::create(data_mask_policy);
            // pipeline.add_transform(|transform_input, transform_output| {
            // NativeDeserializeDataTransform::create(
            // ctx.clone(),
            // block_reader.clone(),
            // plan,
            // topk.clone(),
            // transform_input,
            // transform_output,
            // )
            // })?;
            let num_input_columns = plan.schema().num_fields();
            let mut exprs = Vec::with_capacity(num_input_columns);
            for i in 0..num_input_columns {
                let expr = if let Some(remote_expr) = data_mask_policy.get(&i) {
                    Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS))
                } else {
                    None
                };

                exprs.push(expr);
            }
            /*
            let exprs = data_mask_policy
                .iter()
                .map(|(idx, remote_expr)| {
                    let expr = if let RemoteExpr::ColumnRef { id, .. } = remote_expr {
                        if (*idx != *id) {
                            Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS))
                        } else {
                            None
                        }
                    } else {
                        Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS))
                    };
                    expr
                })
                .collect::<Vec<_>>();
            */
            println!("expr: {:?}", exprs);
            let op = BlockOperator::Replace { exprs };

            let query_ctx = ctx.clone();
            let func_ctx = query_ctx.get_function_context()?;

            pipeline.add_transform(|input, output| {
                let transform = CompoundBlockOperator::create(
                    input,
                    output,
                    num_input_columns,
                    func_ctx.clone(),
                    vec![op.clone()],
                );
                Ok(ProcessorPtr::create(transform))
            })?;
        }

        Ok(())
    }
}
