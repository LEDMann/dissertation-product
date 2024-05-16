use std::{borrow::Borrow, collections::HashMap, env, fs::{self, File}, io::{Read, Write}, ops::Deref, path::Path, sync::{Arc, Mutex, MutexGuard}};
use serde_json::{Value, json};

use crate::endpoint::Endpoint;

use self::table::Table;

use super::endpoint;
mod part;
pub(crate) mod conditional;
pub(crate) mod cell;
pub(crate) mod record;
pub(crate) mod table;

pub struct Database<'a> {
    pub name: String,
    pub indev: bool,
    directory: String,
    pub tables: Vec<Arc<Mutex<table::Table>>>,
    pub endpoints: Vec<Arc<Mutex<endpoint::Endpoint<'a>>>>,
}

impl<'a> Database<'a> {
    /* 
     * MARK: build new database
     */
    pub fn new(name: String, admin_db: Option<Arc<Mutex<Database<'a>>>>, role: String, dir_override: Option<String>) -> Arc<Mutex<Self>> {
        println!("building a new database called {}", name);
        let mut new_db: Arc<Mutex<Database<'a>>> = Arc::new(Mutex::new(Database {
            name: name.clone(),
            indev: true,
            directory: "".to_string(),
            tables: vec![],
            endpoints: vec![],
        }));
        match new_db.try_lock() {
            Ok(mut e) => {
                match e.init_dir(dir_override, role.clone()) {
                    Ok(_) => match admin_db {
                        Some(db) => e.endpoints.append(&mut Endpoint::new_db(Arc::clone(&new_db), db, role)),
                        None => e.endpoints.append(&mut Endpoint::admin_db(Arc::clone(&new_db), role))
                    },
                    Err(n) => panic!("{}", ["database endpoints could not be initialised".to_owned(), n].concat())
                }
            },
            Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
        }
        new_db
    }

    /* 
     * MARK: build self from directory
     */
    pub fn build_from_dir(db_name: String, admin_db: Option<Arc<Mutex<Database<'a>>>>, dir_override: Option<String>) -> Result<Arc<Mutex<Self>>, String> {
        let curr_dir_res = env::current_dir();
        let curr_dir = match curr_dir_res {
            Ok(ref path_buf) => match path_buf.to_str() {
                Some(path_str) => path_str,
                None => return Err( "unable to parse path as str".to_string())
            },
            Err(e) => return Err( ["unable to find current directory\n".to_string(), e.to_string()].concat()),
        };
        let db_dir = match dir_override {
            Some(dir) => [curr_dir, &dir[..], &db_name].join("\\"),
            None => [curr_dir, "databases", &db_name].join("\\")
        };
        println!("building db from dir {db_dir}");
        let table_dirs = match fs::read_dir(db_dir.clone()) {
            Ok(e) => e.into_iter()
                .map(|a| match a {
                    Ok(e) => match e.file_type().unwrap().is_dir() {
                        true => e.path().display().to_string(),
                        false => "".to_string()
                    },
                    Err(e) => "".to_string(),
                })
                .filter(|b| !b.is_empty())
                .collect::<Vec<String>>(),
            Err(e) => return Err("unable to find database directory".to_string()),
        };
        let db_definition: Result<Value, String> = match File::open([&db_dir, "/.def"].concat()) {
            Ok(mut e) => {
                let mut buf = "".to_owned();
                e.read_to_string(&mut buf);
                Ok(serde_json::from_str(&buf).unwrap())
            },
            Err(e) => Err("unable to open database defintion file".to_string()),
        };
        let new_db = Arc::new(Mutex::new(Database { 
            name: db_name.clone(), 
            indev: false, 
            directory: db_dir.clone(), 
            tables:  table_dirs.into_iter()
                                .map(|table_dir| match Table::build_from_dir(table_dir.clone()) {
                                    Ok(b) => Some(b), 
                                    Err(_) => None
                                })
                                .filter(|c| c.is_some())
                                .map(|d| match d { Some(d) => d, _ => panic!("None exists after filtering out all Nones") })
                                .map(|e| Arc::new(Mutex::new(e)))
                                .collect::<Vec<Arc<Mutex<Table>>>>(), 
            endpoints: vec![]
        }));
        match new_db.try_lock() {
            Ok(mut e) => {
                match admin_db {
                    Some(admin_db) => {
                        e.endpoints.append(&mut Endpoint::prod_db(Arc::clone(&new_db), Arc::clone(&admin_db), match &db_definition { Ok(e) => match e.get("role") { Some(e) => match e.as_str() { Some(e) => e.to_owned(), _ => "admin".to_owned() }, _ => "admin".to_owned() }, _ => "admin".to_owned()}));
                        let tables = e.tables.iter().map(|table| Arc::clone(&table)).collect::<Vec<Arc<Mutex<Table>>>>();
                        tables.iter().for_each(|table| {
                            e.endpoints.append(&mut Endpoint::new_table(Arc::clone(&table), Arc::clone(&admin_db), match &db_definition { Ok(y) => match y.get("role") { Some(y) => match y.as_str() { Some(y) => y.to_owned(), _ => "admin".to_owned() }, _ => "admin".to_owned() }, _ => "admin".to_owned()}));
                        });
                    },
                    None => e.endpoints.append(&mut Endpoint::admin_db(Arc::clone(&new_db), match db_definition { Ok(e) => match e.get("role") { Some(e) => match e.as_str() { Some(e) => e.to_owned(), _ => "admin".to_owned() }, _ => "admin".to_owned() }, _ => "admin".to_owned()}))
                }
            },
            Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
        }

        Ok(new_db)
    }

    /* 
     * MARK: initialise directory
     */
    fn init_dir(&mut self, dir_override: Option<String>, role: String) -> Result<String, String> {
        println!("initialising directory");
        let curr_dir_res = env::current_dir();
        let curr_dir = match curr_dir_res {
            Ok(ref path_buf) => match path_buf.to_str() {
                Some(path_str) => path_str,
                None => return Err(("unable to parse path as str".to_string()))
            },
            Err(e) => return Err((["unable to find current directory\n".to_string(), e.to_string()].concat())),
        };
        let db_dir = match dir_override {
            Some(dir) => [curr_dir, &dir[..], &self.name].join("\\"),
            None => [curr_dir, "databases", &self.name].join("\\")
        };
        println!("directory: {}", db_dir);
        match fs::create_dir_all(&db_dir) {
            Ok(_) => self.directory = db_dir.to_string(),
            Err(e) => return Err((["unable to create specified directories\n".to_string(), e.to_string()].concat())),
        };
        match File::create([&db_dir, ".def"].join("\\")) {
            Ok(mut e) => {write!(e, "{{ \"role\":\"{role}\" }}").unwrap();},
            Err(e) => return Err((["unable to create database definition file\n".to_string(), e.to_string()].concat()))
        };
        match File::create([&db_dir, "/.log"].concat()) {
            Ok(_) => {}
            Err(e) => return Err((["unable to create database log file\n".to_string(), e.to_string()].concat()))
        };
        Ok("directory initialisation successful".to_owned())
    }

    /* 
     * MARK: build a new table
     *                                                                                                        cname   dtype   default value   nullable unique   foreign key
     */
    pub fn build_table(&mut self, admin_db: Arc<Mutex<Database<'a>>>, table_name: String, table_columns: Vec<(String, String, Option<String>, bool, bool, Option<(String, String)>)>) /* -> Result<String, String> */ {
        println!("building a new table {table_name}");
        let db_definition = match File::open([&self.directory, "/.def"].concat()) {
            Ok(mut e) => {
                let mut buf = "".to_owned();
                e.read_to_string(&mut buf);
                Ok(json!(buf))
            },
            Err(e) => Err("unable to open database defintion file".to_string()),
        };
        let new_table = Arc::new(Mutex::new(Table::new(self.name.clone(), table_name, table_columns, true)));
        self.tables.push(Arc::clone(&new_table));
        self.endpoints.append(&mut Endpoint::new_table(Arc::clone(&new_table), admin_db, match db_definition { Ok(e) => match e.get("role") { Some(e) => match e.as_str() { Some(e) => e.to_owned(), _ => "admin".to_owned() }, _ => "admin".to_owned() }, _ => "admin".to_owned()}));
    }

    /* 
     * MARK: delete a table
     */
    pub fn delete_table(&mut self, table_name: String) -> Result<String, String> {
        let table_index = match self.tables.iter_mut().enumerate().find(|(i, table)| match table.try_lock() { Ok(table) => table.name == table_name, Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())}) {
            Some((i, table)) => match table.try_lock() {
                Ok(mut table) => match table.query_delete_table() {
                    Ok(e) => Ok((i, e)),
                    Err(e) => Err(e)
                },
                Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
            },
            None => Err("table does not exist in database".to_owned())
        };
        match table_index {
            Ok(i) => {
                self.tables.remove(i.0);
                Ok("table files deleted and removed from database memory".to_owned())
            },
            Err(e) => Err(e)
        }
    }
}