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

use std::collections::HashMap;
use std::sync::Arc;

use base64::encode;
use bytes::Buf;
use bytes::Bytes;
use databend_common_auth::RefreshableToken;
use databend_common_base::base::GlobalInstance;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::ShareApi;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::share::share_name_ident::ShareNameIdent;
use databend_common_meta_app::share::GetShareEndpointReq;
use databend_common_meta_app::share::ShareCredential;
use databend_common_meta_app::share::ShareEndpointMeta;
use databend_common_meta_app::share::ShareSpec;
use databend_common_meta_app::share::TableInfoMap;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_storage::ShareTableConfig;
use databend_common_users::UserApiProvider;
use log::debug;
use log::error;
use opendal::raw::HttpClient;
use opendal::Buffer;
use reqwest::header::HeaderMap;
use ring::hmac;

use crate::signer::AUTH_METHOD_HEADER;
use crate::signer::HMAC_AUTH_METHOD;
use crate::signer::SIGNATURE_HEADER;
use crate::signer::TENANT_HEADER;

pub struct ShareEndpointClient {}

impl ShareEndpointClient {
    pub fn new() -> Self {
        Self {}
    }

    fn generate_auth_headers(
        path: &str,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(TENANT_HEADER, from_tenant.parse().unwrap());
        if let Some(credential) = &share_endpoint_meta.credential {
            match credential {
                ShareCredential::HMAC(key) => {
                    let key = hmac::Key::new(hmac::HMAC_SHA256, key.as_bytes().to_vec().as_ref());
                    headers.insert(AUTH_METHOD_HEADER, HMAC_AUTH_METHOD.parse().unwrap());
                    let signature = hmac::sign(&key, path.as_bytes());
                    let signature = encode(signature.as_ref());
                    headers.insert(SIGNATURE_HEADER, signature.parse().unwrap());
                }
                _ => {}
            }
        }
        headers
    }

    #[async_backtrace::framed]
    pub async fn get_share_spec_by_name(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        share_name: &str,
    ) -> Result<()> {
        let path = format!("/tenant/{}/{}/share_spec", to_tenant, share_name);
        let uri = format!("{}{}", share_endpoint_meta.url, path.clone());
        let headers = Self::generate_auth_headers(&path, share_endpoint_meta, from_tenant);
        println!("url: {:?}\n", share_endpoint_meta.url);
        println!("headers: {:?}\n", headers);
        println!("path: {:?}\n", path);
        println!("share_endpoint_meta: {:?}\n", share_endpoint_meta);
        let client = reqwest::Client::new();
        let resp = client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                println!("resp: {:?}\n", resp);
                Ok(())
            }
            Err(err) => Ok(()),
        }
    }
}
