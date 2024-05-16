use std::sync::{Arc, Mutex, MutexGuard};
use serde_json::Value;
use crate::database::table;
use crate::database::Database;
use super::query;
use super::script;

pub enum Runnable<'a> {
    Query(query::Query<'a>),
    Script(script::Script),
}

impl<'a> Runnable<'a> {
    pub fn run(&mut self, admin_db: Option<Arc<Mutex<Database<'a>>>>, mut database: Option<&mut MutexGuard<Database<'a>>>, body: Value, dir_override: Option<String>) {
        match self {
            Runnable::Query(q) => q.run(admin_db, database, body, dir_override),
            Runnable::Script(q) => {}
        }
    }

    pub fn result(&mut self) -> Result<String, String> {
        match self {
            Runnable::Query(q) => q.result(),
            Runnable::Script(q) => Err("scripts not implemented".to_owned())
        }
    }

    pub fn table(&self) -> Result<Arc<Mutex<table::Table>>, String> {
        match self {
            Runnable::Query(q) => q.table(),
            Runnable::Script(q) => Err("not a query".to_owned()),
        }
    }
}