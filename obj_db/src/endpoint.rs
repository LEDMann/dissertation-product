use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

use crate::{database::{self, table, Database}, endpoint::query::QueryDatabaseCreateTable};

pub mod runnable;
pub mod query;
pub mod script;

pub struct Endpoint<'a> {
    pub name: String,
    pub role: String,
    admin_db: Arc<Mutex<database::Database<'a>>>,
    pub runnable: Arc<Mutex<runnable::Runnable<'a>>>
}

impl<'a> Endpoint<'a> {
    pub fn run(&mut self, mut database: Option<&mut MutexGuard<Database<'a>>>, body: Value, dir_override: Option<String>) {
        match self.runnable.try_lock() {
            Ok(mut e) => e.run(Some(Arc::clone(&self.admin_db)), database, body, dir_override),
            Err(e) => panic!("shits fucked")
        }
    }

    pub fn result(&mut self) -> Result<String, String> {
        match self.runnable.try_lock(){
            Ok(mut e) => e.result(),
            Err(e) => Err(e.to_string())
        }
    }

    pub fn table(&self) -> Result<Arc<Mutex<table::Table>>, String> {
        match self.runnable.try_lock(){
            Ok(e) => e.table(),
            Err(e) => Err(e.to_string())
        }
    }

    fn check_role(&self) {
        todo!()
    }

    /* 
     * MARK: new database server endpoints
     * 
     * generate admin endpoints that a new database server should have
     */
    pub fn new_server(admin_db: Arc<Mutex<Database<'a>>>, role: String) -> Vec<Self> {
        vec![Endpoint { name: "create_database".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryNewDatabase(query::QueryNewDatabase::new("create_database".to_owned()))))) }]
    }

    /* 
     * MARK: new database endpoints
     * 
     * generate endpoints that a new indev database should have
     */
    pub fn new_db(database: Arc<Mutex<database::Database<'a>>>, admin_db: Arc<Mutex<Database<'a>>>, role: String) -> Vec<Arc<Mutex<Self>>> {
        vec![
            Arc::new(Mutex::new(Endpoint { name: "create_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseCreateTable(query::QueryDatabaseCreateTable::new("create_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "update_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseUpdateTable(query::QueryDatabaseUpdateTable::new("update_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "delete_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseDeleteTable(query::QueryDatabaseDeleteTable::new("delete_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "indev_toggle".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseInDevToggle(query::QueryDatabaseInDevToggle::new("indev_toggle".to_owned() )))))) }))
        ]
    }

    /* 
     * MARK: poduction database endpoints
     * 
     * generate endpoints that a reloaded production database should have
     */
    pub fn prod_db(database: Arc<Mutex<database::Database<'a>>>, admin_db: Arc<Mutex<Database<'a>>>, role: String) -> Vec<Arc<Mutex<Self>>> {
        vec![
            Arc::new(Mutex::new(Endpoint { name: "create_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseCreateTable(query::QueryDatabaseCreateTable::new("create_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "update_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseUpdateTable(query::QueryDatabaseUpdateTable::new("update_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "delete_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseDeleteTable(query::QueryDatabaseDeleteTable::new("delete_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "indev_toggle".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseInDevToggle(query::QueryDatabaseInDevToggle::new("indev_toggle".to_owned() )))))) }))
        ]
    }

    /* 
     * MARK: poduction database endpoints
     * 
     * generate endpoints that a reloaded production database should have
     */
    pub fn admin_db(database: Arc<Mutex<database::Database<'a>>>, role: String) -> Vec<Arc<Mutex<Self>>> {
        vec![
            Arc::new(Mutex::new(Endpoint { name: "create_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&database), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseCreateTable(query::QueryDatabaseCreateTable::new("create_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "update_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&database), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseUpdateTable(query::QueryDatabaseUpdateTable::new("update_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "delete_table".to_owned(), role: role.clone(), admin_db: Arc::clone(&database), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseDeleteTable(query::QueryDatabaseDeleteTable::new("delete_table".to_owned() )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "indev_toggle".to_owned(), role: role.clone(), admin_db: Arc::clone(&database), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryDatabase(query::QueryDatabase::QueryDatabaseInDevToggle(query::QueryDatabaseInDevToggle::new("indev_toggle".to_owned() )))))) }))
        ]
    }

    /* 
     * MARK: new table endpoints
     * 
     * generate endpoints that a new indev databases table should have
     */
    pub fn new_table(table: Arc<Mutex<table::Table>>, admin_db: Arc<Mutex<Database<'a>>>, role: String) -> Vec<Arc<Mutex<Self>>> {
        vec![
            Arc::new(Mutex::new(Endpoint { name: "create_record".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryTable(query::QueryTable::TableQueryCreate(query::TableQueryCreate::new("create_record".to_owned(), Arc::clone(&table) )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "read_record".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryTable(query::QueryTable::TableQueryRead(  query::TableQueryRead::new("read_record".to_owned(),     Arc::clone(&table) )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "update_record".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryTable(query::QueryTable::TableQueryUpdate(query::TableQueryUpdate::new("update_record".to_owned(), Arc::clone(&table) )))))) })),
            Arc::new(Mutex::new(Endpoint { name: "delete_record".to_owned(), role: role.clone(), admin_db: Arc::clone(&admin_db), runnable: Arc::new(Mutex::new(runnable::Runnable::Query(query::Query::QueryTable(query::QueryTable::TableQueryDelete(query::TableQueryDelete::new("delete_record".to_owned(), Arc::clone(&table) )))))) })),
        ]
    }
}