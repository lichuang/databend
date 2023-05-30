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

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::println;
use std::sync::Arc;
use std::vec;

use common_ast::parser::parse_expr;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

pub struct DataSourceMaskTransformer {
    data_mask_policy: BTreeMap<FieldIndex, Expr>,
    data_blocks: VecDeque<DataBlock>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
}

impl DataSourceMaskTransformer {
    pub fn create(
        data_mask_policy: &BTreeMap<FieldIndex, RemoteExpr>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Self {
        let data_mask_policy = data_mask_policy
            .iter()
            .map(|(i, raw_expr)| (*i, raw_expr.as_expr(&BUILTIN_FUNCTIONS)))
            .collect();
        println!("data_mask_policy: {:?}", data_mask_policy);
        DataSourceMaskTransformer {
            data_mask_policy,
            data_blocks: VecDeque::new(),
            input,
            output,
            output_data: None,
        }
    }
}

impl Processor for DataSourceMaskTransformer {
    fn name(&self) -> String {
        String::from("DataSourceMaskTransformer")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            self.data_blocks.push_back(data_block);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(mut data_block) = self.data_blocks.pop_front() {
            let num_rows = data_block.num_rows();
            let mut columns = vec![];
            for (i, column) in data_block.columns().into_iter().enumerate() {
                if let Some(expr) = self.data_mask_policy.get(&i) {
                } else {
                    columns.push(column.clone());
                }
            }
            let new_data_block =
                DataBlock::new_with_meta(columns, num_rows, data_block.take_meta());
            self.output_data = Some(new_data_block);
        }
        Ok(())
    }
}
