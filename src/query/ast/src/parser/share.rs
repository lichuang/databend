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

use nom::combinator::map;

use crate::ast::AstShareCredential;
use crate::ast::UriLocation;
use crate::parser::common::*;
use crate::parser::expr::*;
use crate::parser::input::Input;
use crate::parser::token::HMAC_KEY;
use crate::parser::ErrorKind;
use crate::rule;

pub fn share_endpoint_uri_location(i: Input) -> IResult<UriLocation> {
    map_res(
        rule! {
            #literal_string
        },
        |location| {
            UriLocation::from_uri(location, "".to_string(), BTreeMap::new())
                .map_err(|_| nom::Err::Failure(ErrorKind::Other("invalid uri")))
        },
    )(i)
}

pub fn share_endpoint_credential(i: Input) -> IResult<AstShareCredential> {
    let mut hmac = map(
        rule! {
            "(" ~ HMAC_KEY ~ "=" ~ "\"" ~ #ident ~ "\"" ~ ")"
        },
        |(_, _, _, _, hmac_key, _, _)| AstShareCredential::HMAC(hmac_key),
    );

    rule!(
        #hmac
    )(i)
}
