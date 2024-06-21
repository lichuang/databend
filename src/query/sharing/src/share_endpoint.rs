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
use databend_common_meta_app::share::ShareSpec;
use databend_common_meta_app::share::TableInfoMap;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_storage::ShareTableConfig;
use databend_common_users::UserApiProvider;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::Method;
use http::Request;
use log::debug;
use log::error;
use opendal::raw::HttpClient;
use opendal::Buffer;

use crate::signer::TENANT_HEADER;

#[derive(Debug)]
struct EndpointConfig {
    pub url: String,
    pub token: RefreshableToken,
    pub tenant: String,
}

pub struct ShareEndpointManager {
    client: HttpClient,
}

impl ShareEndpointManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(ShareEndpointManager {
            client: HttpClient::new()?,
        }));
        Ok(())
    }

    pub fn instance() -> Arc<ShareEndpointManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    async fn get_share_endpoint_config(
        &self,
        from_tenant: &Tenant,
        to_tenant: Option<&Tenant>,
    ) -> Result<Vec<EndpointConfig>> {
        Ok(vec![])
    }

    #[async_backtrace::framed]
    pub async fn get_table_info_map(
        &self,
        from_tenant: &Tenant,
        db_info: &DatabaseInfo,
        tables: Vec<String>,
    ) -> Result<TableInfoMap> {
        Ok(TableInfoMap::default())
    }

    #[async_backtrace::framed]
    pub async fn get_inbound_shares(
        &self,
        from_tenant: &Tenant,
        to_tenant: Option<&Tenant>,
        share_name: Option<ShareNameIdent>,
    ) -> Result<Vec<(String, ShareSpec)>> {
        Ok(vec![])
    }
}
