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

use databend_common_exception::ErrorCode;
use databend_common_meta_api::reply::txn_reply_to_api_result;
use databend_common_meta_api::txn_cond_seq;
use databend_common_meta_api::txn_op_del;
use databend_common_meta_api::txn_op_put;
use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::TenantOwnershipObject;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::NonEmptyStr;
use databend_common_meta_types::NonEmptyString;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use enumflags2::make_bitflags;

use crate::role::role_api::RoleApi;
use crate::serde::check_and_upgrade_to_pb;
use crate::serialize_struct;

static ROLE_API_KEY_PREFIX: &str = "__fd_roles";

static TXN_MAX_RETRY_TIMES: u32 = 5;

static BUILTIN_ROLE_ACCOUNT_ADMIN: &str = "account_admin";

pub struct RoleMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError> + Send + Sync>,
    tenant: NonEmptyString,
    role_prefix: String,
}

impl RoleMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError> + Send + Sync>,
        tenant: NonEmptyStr,
    ) -> Self {
        let t = tenant.get();
        RoleMgr {
            kv_api,
            tenant: tenant.into(),
            role_prefix: format!("{}/{}", ROLE_API_KEY_PREFIX, t),
        }
    }

    #[async_backtrace::framed]
    async fn upsert_role_info(
        &self,
        role_info: &RoleInfo,
        seq: MatchSeq,
    ) -> Result<u64, ErrorCode> {
        let key = self.make_role_key(role_info.identity());
        let value = serialize_struct(role_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        let res = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Update(value), None))
            .await?;
        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownRole(format!(
                "Role '{}' does not exist.",
                role_info.name
            ))),
        }
    }

    #[async_backtrace::framed]
    async fn upgrade_to_pb(
        &self,
        key: String,
        value: Vec<u8>,
        seq: MatchSeq,
    ) -> Result<UpsertKVReply, MetaError> {
        let kv_api = self.kv_api.clone();
        kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Update(value), None))
            .await
    }

    /// Build meta-service for a role grantee, which is a tenant's database, table, stage, udf, etc.
    fn ownership_object_key(&self, object: &OwnershipObject) -> String {
        let grantee = TenantOwnershipObject::new(Tenant::new(self.tenant.as_str()), object.clone());
        grantee.to_string_key()
    }

    /// Build meta-service for a listing keys belongs to the tenant.
    ///
    /// In form of `__fd_object_owners/<tenant>/`.
    fn ownership_object_prefix(&self) -> String {
        let dummy = OwnershipObject::UDF {
            name: "dummy".to_string(),
        };
        let grantee = TenantOwnershipObject::new(Tenant::new(self.tenant.as_str()), dummy);
        grantee.tenant_prefix()
    }

    fn make_role_key(&self, role: &str) -> String {
        format!("{}/{}", self.role_prefix, role)
    }
}

#[async_trait::async_trait]
impl RoleApi for RoleMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add_role(&self, role_info: RoleInfo) -> databend_common_exception::Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let key = self.make_role_key(role_info.identity());
        let value = serialize_struct(&role_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        let upsert_kv = self.kv_api.upsert_kv(UpsertKVReq::new(
            &key,
            match_seq,
            Operation::Update(value),
            None,
        ));

        let res_seq = upsert_kv.await?.added_seq_or_else(|_v| {
            ErrorCode::RoleAlreadyExists(format!("Role '{}' already exists.", role_info.name))
        })?;

        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_role(&self, role: &String, seq: MatchSeq) -> Result<SeqV<RoleInfo>, ErrorCode> {
        let key = self.make_role_key(role);
        let res = self.kv_api.get_kv(&key).await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownRole(format!("Role '{}' does not exist.", role)))?;

        match seq.match_seq(&seq_value) {
            Ok(_) => {
                let data = check_and_upgrade_to_pb(
                    key,
                    &seq_value,
                    self.kv_api.clone(),
                    ErrorCode::UnknownRole,
                    || format!("Role '{}' does not exist.", role),
                )
                .await?
                .data;

                Ok(SeqV::new(seq_value.seq, data))
            }
            Err(_) => Err(ErrorCode::UnknownRole(format!(
                "Role '{}' does not exist.",
                role
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_roles(&self) -> Result<Vec<SeqV<RoleInfo>>, ErrorCode> {
        let role_prefix = self.role_prefix.clone();
        let values = self.kv_api.prefix_list_kv(role_prefix.as_str()).await?;

        let mut r = vec![];
        for (key, val) in values {
            let u = check_and_upgrade_to_pb(
                key,
                &val,
                self.kv_api.clone(),
                ErrorCode::UnknownRole,
                || "",
            )
            .await?
            .data;
            r.push(SeqV::new(val.seq, u));
        }

        Ok(r)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_ownerships(&self) -> Result<Vec<SeqV<OwnershipInfo>>, ErrorCode> {
        let object_owner_prefix = self.ownership_object_prefix();
        let kv_api = self.kv_api.clone();
        let values = kv_api.prefix_list_kv(object_owner_prefix.as_str()).await?;

        let mut r = vec![];
        for (key, val) in values {
            let u = check_and_upgrade_to_pb(
                key,
                &val,
                self.kv_api.clone(),
                ErrorCode::UnknownRole,
                || "",
            )
            .await?
            .data;
            r.push(SeqV::new(val.seq, u));
        }

        Ok(r)
    }

    /// General role update.
    ///
    /// It fetch the role that matches the specified seq number, update it in place, then write it back with the seq it sees.
    ///
    /// Seq number ensures there is no other write happens between get and set.
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn update_role_with<F>(
        &self,
        role: &String,
        seq: MatchSeq,
        f: F,
    ) -> Result<Option<u64>, ErrorCode>
    where
        F: FnOnce(&mut RoleInfo) + Send,
    {
        let SeqV {
            seq,
            data: mut role_info,
            ..
        } = self.get_role(role, seq).await?;

        f(&mut role_info);

        let seq = self
            .upsert_role_info(&role_info, MatchSeq::Exact(seq))
            .await?;
        Ok(Some(seq))
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn grant_ownership(
        &self,
        object: &OwnershipObject,
        new_role: &str,
    ) -> databend_common_exception::Result<()> {
        let old_role = self.get_ownership(object).await?.map(|o| o.role);
        let grant_object = convert_to_grant_obj(object);

        let owner_key = self.ownership_object_key(object);

        let owner_value = serialize_struct(
            &OwnershipInfo {
                object: object.clone(),
                role: new_role.to_string(),
            },
            ErrorCode::IllegalUserInfoFormat,
            || "",
        )?;

        let mut condition = vec![];
        let mut if_then = vec![txn_op_put(&owner_key, owner_value.clone())];

        if let Some(old_role) = old_role {
            // BUILTIN role or Dropped role may get err, no need to revoke
            if let Ok(seqv) = self.get_role(&old_role.to_owned(), MatchSeq::GE(1)).await {
                let old_key = self.make_role_key(&old_role);
                let old_seq = seqv.seq;
                let mut old_role_info = seqv.data;
                old_role_info.grants.revoke_privileges(
                    &grant_object,
                    make_bitflags!(UserPrivilegeType::{ Ownership }).into(),
                );
                condition.push(txn_cond_seq(&old_key, Eq, old_seq));
                if_then.push(txn_op_put(
                    &old_key,
                    serialize_struct(&old_role_info, ErrorCode::IllegalUserInfoFormat, || "")?,
                ));
            }
        }

        // account_admin has all privilege, no need to grant ownership.
        if new_role != BUILTIN_ROLE_ACCOUNT_ADMIN {
            let new_key = self.make_role_key(new_role);
            let SeqV {
                seq: new_seq,
                data: mut new_role_info,
                ..
            } = self.get_role(&new_role.to_owned(), MatchSeq::GE(1)).await?;
            new_role_info.grants.grant_privileges(
                &grant_object,
                make_bitflags!(UserPrivilegeType::{ Ownership }).into(),
            );
            condition.push(txn_cond_seq(&new_key, Eq, new_seq));
            if_then.push(txn_op_put(
                &new_key,
                serialize_struct(&new_role_info, ErrorCode::IllegalUserInfoFormat, || "")?,
            ));
        }

        let mut retry = 0;

        let txn_req = TxnRequest {
            condition: condition.clone(),
            if_then: if_then.clone(),
            else_then: vec![],
        };

        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let tx_reply = self.kv_api.transaction(txn_req.clone()).await?;
            let (succ, _) = txn_reply_to_api_result(tx_reply)?;

            if succ {
                return Ok(());
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("grant_ownership", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_ownership(
        &self,
        object: &OwnershipObject,
    ) -> databend_common_exception::Result<Option<OwnershipInfo>> {
        let key = self.ownership_object_key(object);

        let res = self.kv_api.get_kv(&key).await?;
        let seq_value = match res {
            Some(value) => value,
            None => return Ok(None),
        };

        // if can not get ownership, will directly return None.
        let seq_val = check_and_upgrade_to_pb(
            key,
            &seq_value,
            self.kv_api.clone(),
            ErrorCode::UnknownRole,
            || "",
        )
        .await?;

        Ok(Some(seq_val.data))
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn revoke_ownership(
        &self,
        object: &OwnershipObject,
    ) -> databend_common_exception::Result<()> {
        let role = self.get_ownership(object).await?.map(|o| o.role);

        let owner_key = self.ownership_object_key(object);

        let mut if_then = vec![txn_op_del(&owner_key)];
        let mut condition = vec![];

        if let Some(role) = role {
            if let Ok(seqv) = self.get_role(&role.to_owned(), MatchSeq::GE(1)).await {
                let old_key = self.make_role_key(&role);
                let grant_object = convert_to_grant_obj(object);
                let old_seq = seqv.seq;
                let mut old_role_info = seqv.data;
                old_role_info.grants.revoke_privileges(
                    &grant_object,
                    make_bitflags!(UserPrivilegeType::{ Ownership }).into(),
                );
                condition.push(txn_cond_seq(&old_key, Eq, old_seq));
                if_then.push(txn_op_put(
                    &old_key,
                    serialize_struct(&old_role_info, ErrorCode::IllegalUserInfoFormat, || "")?,
                ));
            }
        }

        let txn_req = TxnRequest {
            condition: condition.clone(),
            if_then: if_then.clone(),
            else_then: vec![],
        };

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            let tx_reply = self.kv_api.transaction(txn_req.clone()).await?;
            let (succ, _) = txn_reply_to_api_result(tx_reply)?;

            if succ {
                return Ok(());
            }
        }

        Err(ErrorCode::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("revoke_ownership", TXN_MAX_RETRY_TIMES).to_string(),
        ))
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn drop_role(&self, role: String, seq: MatchSeq) -> Result<(), ErrorCode> {
        let key = self.make_role_key(&role);

        let res = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Delete, None))
            .await?;

        res.removed_or_else(|_p| {
            ErrorCode::UnknownRole(format!("Role '{}' does not exist.", role))
        })?;
        Ok(())
    }
}

fn convert_to_grant_obj(owner_obj: &OwnershipObject) -> GrantObject {
    match owner_obj {
        OwnershipObject::Database {
            catalog_name,
            db_id,
        } => GrantObject::DatabaseById(catalog_name.to_string(), *db_id),
        OwnershipObject::Table {
            catalog_name,
            db_id,
            table_id,
        } => GrantObject::TableById(catalog_name.to_string(), *db_id, *table_id),
        OwnershipObject::Stage { name } => GrantObject::Stage(name.to_string()),
        OwnershipObject::UDF { name } => GrantObject::UDF(name.to_string()),
    }
}
