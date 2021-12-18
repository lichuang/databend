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

use common_datavalues::prelude::*;

use crate::scalars::strings::String2StringFunction;
use crate::scalars::strings::StringOperator;

#[derive(Clone, Default)]
pub struct Md5 {}

impl StringOperator for Md5 {
    #[inline]
    fn apply_with_no_null<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> usize {
        let buffer = &mut buffer[0..32];
        // TODO md5 lib doesn't allow encode into buffer...
        hex::encode_to_slice(md5::compute(s).as_ref(), buffer).unwrap();
        32
    }

    fn estimate_bytes(&self, array: &DFStringArray) -> usize {
        array.len() * 32
    }
}

pub type Md5HashFunction = String2StringFunction<Md5>;
