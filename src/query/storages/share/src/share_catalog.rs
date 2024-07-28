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

use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::DatabaseIdent;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::ShareCatalogOption;
use databend_common_meta_app::share::GetShareEndpointReq;
use databend_common_meta_app::share::ShareDatabaseSpec;
use databend_common_meta_app::share::ShareSpec;
use databend_common_sharing::ShareEndpointClient;
use databend_common_users::UserApiProvider;

#[derive(Debug)]
pub struct ShareCreator;

impl CatalogCreator for ShareCreator {
    fn try_create(&self, info: Arc<CatalogInfo>) -> Result<Arc<dyn Catalog>> {
        let opt = match &info.meta.catalog_option {
            CatalogOption::Share(opt) => opt,
            _ => unreachable!(
                "trying to create hive catalog from other catalog, must be an internal bug"
            ),
        };

        let catalog: Arc<dyn Catalog> =
            Arc::new(ShareCatalog::try_create(info.clone(), opt.to_owned())?);

        Ok(catalog)
    }
}

#[derive(Clone)]
pub struct ShareCatalog {
    info: Arc<CatalogInfo>,

    option: ShareCatalogOption,
}

impl Debug for ShareCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("HiveCatalog")
            .field("info", &self.info)
            .field("option", &self.option)
            .finish_non_exhaustive()
    }
}

impl ShareCatalog {
    pub fn try_create(info: Arc<CatalogInfo>, option: ShareCatalogOption) -> Result<ShareCatalog> {
        Ok(Self { info, option })
    }

    async fn get_share_spec(&self) -> Result<ShareSpec> {
        let share_option = &self.option;
        let share_name = &share_option.share_name;
        let share_endpoint = &share_option.share_endpoint;
        let provider = &share_option.provider;
        let tenant = &self.info.name_ident.tenant;

        // 1. get share endpoint
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let req = GetShareEndpointReq {
            tenant: tenant.clone(),
            endpoint: Some(share_endpoint.clone()),
        };
        let reply = meta_api.get_share_endpoint(req).await?;
        if reply.share_endpoint_meta_vec.is_empty() {
            return Err(ErrorCode::UnknownShareEndpoint(format!(
                "UnknownShareEndpoint {:?}",
                share_endpoint
            )));
        }

        // 2. check if ShareSpec exists using share endpoint
        let share_endpoint_meta = &reply.share_endpoint_meta_vec[0].1;
        let client = ShareEndpointClient::new();
        let share_spec = client
            .get_share_spec_by_name(
                share_endpoint_meta,
                tenant.tenant_name(),
                provider,
                share_name,
            )
            .await?;

        Ok(share_spec)
    }

    fn generate_share_database_info(&self, database: &ShareDatabaseSpec) -> DatabaseInfo {
        let share_option = &self.option;
        let share_name = &share_option.share_name;
        let share_endpoint = &share_option.share_endpoint;
        let provider = &share_option.provider;
        let tenant = &self.info.name_ident.tenant;

        DatabaseInfo {
            ident: DatabaseIdent::default(),
            name_ident: DatabaseNameIdent::new(provider, &database.name),
            meta: DatabaseMeta {
                engine: "SHARE".to_string(),
                engine_options: BTreeMap::new(),
                options: BTreeMap::new(),
            },
        }
    }
}

#[async_trait::async_trait]
impl Catalog for ShareCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        self.info.name_ident.catalog_name.clone()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        self.info.clone()
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn get_database(&self, _tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        let share_spec = self.get_share_spec().await?;
        if let Some(use_database) = share_spec.use_database {
            if &use_database.name == db_name {}
        }

        let db = self
            .client
            .get_database(FastStr::new(db_name))
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let hive_database: HiveDatabase = db.into();
        let res: Arc<dyn Database> = Arc::new(hive_database);
        Ok(res)
    }

    // Get all the databases.
    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let db_names = self
            .client
            .get_all_databases()
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let mut dbs = Vec::with_capacity(db_names.len());

        for name in db_names {
            let db = self
                .client
                .get_database(name)
                .await
                .map(from_thrift_exception)
                .map_err(from_thrift_error)??;

            let hive_database: HiveDatabase = db.into();
            let res: Arc<dyn Database> = Arc::new(hive_database);
            dbs.push(res)
        }

        Ok(dbs)
    }

    // Operation with database.
    #[async_backtrace::framed]
    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot create database in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot drop database in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop database in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot rename database in HIVE catalog",
        ))
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let res: Arc<dyn Table> = Arc::new(HiveTable::try_create(table_info.clone())?);
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, _table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get table by id in HIVE catalog",
        ))
    }

    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        _table_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get tables name by ids in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_name_by_id(&self, _table_id: MetaId) -> Result<Option<String>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get table name by id in HIVE catalog",
        ))
    }

    async fn get_db_name_by_id(&self, _db_id: MetaId) -> Result<String> {
        Err(ErrorCode::Unimplemented(
            "Cannot get db name by id in HIVE catalog",
        ))
    }

    async fn mget_database_names_by_ids(
        &self,
        _tenant: &Tenant,
        _db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get dbs name by ids in HIVE catalog",
        ))
    }

    // Get one table by db and table name.
    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn get_table(
        &self,
        _tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let table_meta = match self
            .client
            .get_table(FastStr::new(db_name), FastStr::new(table_name))
            .await
        {
            Ok(MaybeException::Ok(meta)) => meta,
            Ok(MaybeException::Exception(exception)) => {
                return Err(ErrorCode::TableInfoError(format!("{exception:?}")));
            }
            Err(e) => {
                return Err(from_thrift_error(e));
            }
        };

        Self::handle_table_meta(&table_meta)?;

        let fields = self
            .client
            .get_schema(FastStr::new(db_name), FastStr::new(table_name))
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;
        let table_info: TableInfo = super::converters::try_into_table_info(
            self.info.clone(),
            self.sp.clone(),
            table_meta,
            fields,
        )?;
        let res: Arc<dyn Table> = Arc::new(HiveTable::try_create(table_info)?);

        Ok(res)
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn list_tables(&self, _tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let table_names = self
            .client
            .get_all_tables(FastStr::new(db_name))
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let mut tables = Vec::with_capacity(table_names.len());

        for name in table_names {
            let table = self.get_table(_tenant, db_name, name.as_str()).await?;
            tables.push(table)
        }

        Ok(tables)
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot list table history in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot create table in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot drop table in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop table in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot commit_table_meta in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot rename table in HIVE catalog",
        ))
    }

    // Check a db.table is exists or not.
    #[async_backtrace::framed]
    async fn exists_table(&self, tenant: &Tenant, db_name: &str, table_name: &str) -> Result<bool> {
        // TODO refine this
        match self.get_table(tenant, db_name, table_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UNKNOWN_TABLE {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot upsert table option in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot set_table_column_mask_policy in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_lock_revisions(&self, _req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_lock_revision(&self, _req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn extend_lock_revision(&self, _req: ExtendLockRevReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn delete_lock_revision(&self, _req: DeleteLockRevReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_locks(&self, _req: ListLocksReq) -> Result<Vec<LockInfo>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<CreateTableIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<DropTableIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_index(&self, _req: CreateIndexReq) -> Result<CreateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_index(&self, _req: DropIndexReq) -> Result<DropIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_index(&self, _req: GetIndexReq) -> Result<GetIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_index(&self, _req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_indexes(&self, _req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_index_ids_by_table_id(&self, _req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_indexes_by_table_id(
        &self,
        _req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_virtual_column(
        &self,
        _req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_virtual_column(
        &self,
        _req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_virtual_column(
        &self,
        _req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_virtual_columns(
        &self,
        _req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        unimplemented!()
    }

    /// Table function

    // Get function by name.
    fn get_table_function(
        &self,
        _func_name: &str,
        _tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        unimplemented!()
    }

    // List all table functions' names.
    fn list_table_functions(&self) -> Vec<String> {
        vec![]
    }

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }

    async fn create_sequence(&self, _req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        unimplemented!()
    }
    async fn get_sequence(&self, _req: GetSequenceReq) -> Result<GetSequenceReply> {
        unimplemented!()
    }

    async fn get_sequence_next_value(
        &self,
        _req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply> {
        unimplemented!()
    }

    async fn drop_sequence(&self, _req: DropSequenceReq) -> Result<DropSequenceReply> {
        unimplemented!()
    }
}
