
use std::{cell::Cell, default, fs::DirEntry, sync::{Arc, Mutex, MutexGuard}};

use regex::Regex;
use serde_json::Value;

use crate::database::{self, cell::{self, CellValue}, conditional::{self, Condition, Conditional, Relation}, record::{self, Record}, table, Database};

/* 
 * MARK: Query
 * when created each query should immediately run their preset 
 * functionality and store the result as a property of itself
 */
pub enum Query<'a> {
    QueryNewDatabase(QueryNewDatabase<'a>),
    QueryDatabase(QueryDatabase),
    QueryTable(QueryTable),
}

impl<'a> Query<'a> {
    pub fn run(&mut self, admin_db: Option<Arc<Mutex<Database<'a>>>>, mut database: Option<&mut MutexGuard<Database<'a>>>, body: Value, dir_override: Option<String>) {
        match self {
            Query::QueryNewDatabase(qnd) => qnd.run(admin_db, body, dir_override),
            Query::QueryDatabase(qd) => match admin_db{ Some(admin_db) => qd.run(admin_db, database, body),  None => qd.set_result(Err("admin database not initialised or not attached".to_owned()))},
            Query::QueryTable(qt) => qt.run(body),
        }
    }

    pub fn result(&self) -> Result<String, String> {
        match self {
            Query::QueryNewDatabase(qnd) => match &qnd.result.clone() { Ok(_) => Ok("database creation successful".to_owned()), Err(e) => Err(e.clone()) },
            Query::QueryDatabase(qd) => qd.result(),
            Query::QueryTable(qt) => qt.result(),
        }
    }

    pub fn table(&self) -> Result<Arc<Mutex<table::Table>>, String> {
        match self {
            Query::QueryNewDatabase(_) => Err("not a table query".to_owned()),
            Query::QueryDatabase(_) => Err("not a table query".to_owned()),
            Query::QueryTable(qt) => qt.table(),
        }
    }
}

/* 
 * MARK: QueryNewDatabase
*/
pub struct QueryNewDatabase<'a> { name: String, pub result: Result<Arc<Mutex<Database<'a>>>, String> }

impl<'a> QueryNewDatabase<'a> {
    pub fn new(qname: String) -> Self {
        println!("constructing new db endpoint");
        QueryNewDatabase { name: qname, result: Err("Query has not yet been run".to_owned()) }
    }
    
    pub fn run(&mut self, admin_db: Option<Arc<Mutex<Database<'a>>>>, mut body: Value, dir_override: Option<String>) {
        match body["database_name"].as_str() {
            Some(db_name) => match body["role"].as_str() {
                    Some(role) => self.result = Ok(Database::new(db_name.to_owned(), admin_db, role.to_owned(), dir_override)),
                    None => self.result = Ok(Database::new(db_name.to_owned(), admin_db, "ADMIN".to_owned(), dir_override))
                },
            None => self.result = Err("could not parse database_name".to_owned())
        }
        match &self.result {
            Ok(_) => println!("db created"),
            Err(e) => println!("{}", e)
        }
    }
}

/* 
 * MARK: Querytable
 * these queries should be limited to only reading, creating or 
 * updating data on existing tables
 */
pub enum QueryTable {
    TableQueryCreate(TableQueryCreate),
    TableQueryRead(TableQueryRead),
    TableQueryUpdate(TableQueryUpdate),
    TableQueryDelete(TableQueryDelete),
}

impl QueryTable {
    pub fn run(&mut self, body: Value) {
        match self {
            QueryTable::TableQueryCreate(TQC) => TQC.parse(body),
            QueryTable::TableQueryRead(TQR)     => TQR.parse(body),
            QueryTable::TableQueryUpdate(TQU) => TQU.parse(body),
            QueryTable::TableQueryDelete(TQD) => TQD.parse(body),
        }
    }

    pub fn result(&self) -> Result<String, String> {
        match self {
            QueryTable::TableQueryCreate(TQC) => TQC.result.clone(),
            QueryTable::TableQueryRead(TQR)     => TQR.result.clone(),
            QueryTable::TableQueryUpdate(TQU) => TQU.result.clone(),
            QueryTable::TableQueryDelete(TQD) => TQD.result.clone(),
        }
    }

    pub fn table(&self) -> Result<Arc<Mutex<table::Table>>, String> {
        match self {
            QueryTable::TableQueryCreate(TQC) => Ok(TQC.table.clone()),
            QueryTable::TableQueryRead(TQR)     => Ok(TQR.table.clone()),
            QueryTable::TableQueryUpdate(TQU) => Ok(TQU.table.clone()),
            QueryTable::TableQueryDelete(TQD) => Ok(TQD.table.clone()),
        }
    }
}

/* 
 * MARK: TableQueryCreate
 */
pub struct TableQueryCreate { table: Arc<Mutex<table::Table>>, name: String, pub result: Result<String, String> }

impl TableQueryCreate {
    pub fn new(qname: String, table: Arc<Mutex<table::Table>>) -> Self {
        TableQueryCreate { table: table, name: qname, result: Err("Query has not yet been run".to_owned()) }
    }

    pub fn parse(&mut self, body: Value) {
        println!("record creation query to parse {}", body);

        let coldefs = match self.table.try_lock() {
            Ok(table) => table.column_definition.iter().map(|celldef| match celldef { 
                    cell::Cell::CellDef { name, ctype, .. } => (name.clone(), ctype.clone()),
                    _ => ("".to_owned(), cell::CellValue::String{ name: "".to_owned(), data: None }) 
                })
                .filter(|celldef| !celldef.0.is_empty())
                .collect::<Vec<(String, CellValue)>>(),
            Err(e) =>panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
        };
        println!("coldefs formatted {:?}", coldefs);
        let records = match body["records"].as_array() {
            Some(records) => {
                println!("records as array {:?}", records);
                let parsed = records.iter().map(|record| {
                    println!("record as val {:?}", record);
                    match record.as_object() {
                        Some(record) => {
                            println!("record as object {:?}", record);
                            record.iter().map(|(record_name, record_value)| match coldefs.iter().find(|celldef| &celldef.0 == record_name) {
                                Some((_, celldefvalue)) => {
                                    let cdv = match celldefvalue {
                                        cell::CellValue::String {..} => cell::CellValue::String { name: record_name.to_owned(), data: match record_value.as_str() { Some(val) => Some(val.to_owned()), None => None } },
                                        cell::CellValue::Bool   {..} => cell::CellValue::Bool   { name: record_name.to_owned(), data: record_value.as_bool() },
                                        cell::CellValue::UInt   {..} => cell::CellValue::UInt   { name: record_name.to_owned(), data: match record_value.as_str() { Some(val) => match val.parse::<u32>() { Ok(val) => Some(val), _ => None } , None => None } },
                                        cell::CellValue::ULong  {..} => cell::CellValue::ULong  { name: record_name.to_owned(), data: match record_value.as_str() { Some(val) => match val.parse::<u128>() { Ok(val) => Some(val), _ => None } , None => None } },
                                        cell::CellValue::IInt   {..} => cell::CellValue::IInt   { name: record_name.to_owned(), data: match record_value.as_str() { Some(val) => match val.parse::<i32>() { Ok(val) => Some(val), _ => None } , None => None } },
                                        cell::CellValue::ILong  {..} => cell::CellValue::ILong  { name: record_name.to_owned(), data: match record_value.as_str() { Some(val) => match val.parse::<i128>() { Ok(val) => Some(val), _ => None } , None => None } },
                                        cell::CellValue::Float  {..} => cell::CellValue::Float  { name: record_name.to_owned(), data: record_value.as_f64() },
                                        cell::CellValue::Bytes  {..} => cell::CellValue::Bytes  { name: record_name.to_owned(), data: match record_value.as_str() { Some(val) => Some(<String as AsRef<[u8]>>::as_ref(&val.to_owned()).chunks(2).map(|pair| match pair[0] { b'A'..=b'F' => pair[0]-b'A'+10, _ => pair[0] } << 4 |  match pair[1] { b'A'..=b'F' => pair[1]-b'A'+10, _ => pair[1] }).map(|a| a as u8).collect::<Vec<u8>>()), None => None } },
                                        _ => CellValue::String { name: "".to_owned(), data: Some("".to_owned()) }
                                    };
                                    cdv
                                },
                                None => CellValue::String { name: "".to_owned(), data: Some("".to_owned()) }
                            })
                            .filter(|a| !a.name().is_empty())
                            .collect::<Vec<CellValue>>()
                        },
                        None => vec![]
                    }
                })
                .filter(|a| !a.is_empty())
                .map(|recordvec| Record { columns: recordvec })
                .collect::<Vec<Record>>();
                Some(parsed)
            },
            None => None
        };
        match records {
            Some(records) => self.run(records),
            None => self.result = Err("no records submitted".to_owned())
        }
    }

    /* 
     * loop over each record supplied in the request, for each record compare it against the table definition to find any columns where values were not supplied
     * and add the default value to the record from the table definition if one exists
     */
    pub fn run(&mut self, mut records: Vec<record::Record> ) {
        let mut full_records: Vec<record::Record> = vec![];
        records.iter().for_each(|record| {
            let mut full_record = Record { columns: vec![] };
            match self.table.try_lock() {
                Ok(table) => table.column_definition.iter().for_each(|cell_def|  match cell_def {
                    cell::Cell::CellDef { name, ctype, default, .. } => {
                        match default {
                            true => match record.columns.iter().find(|column| column.name() == name) {
                                Some(matching_cell) => full_record.columns.push(matching_cell.clone()),
                                None => full_record.columns.push(ctype.clone())
                            },
                            false => match record.columns.iter().find(|column| column.name() == name) {
                                Some(matching_cell) => full_record.columns.push(matching_cell.clone()),
                                None => self.result = Err(["no default value specified for column ", name].concat())
                            }
                        }
                    },
                    _ => {}
                }),
                Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
            };
            full_records.push(full_record);
        });

        println!("query has filled records {:?}", full_records);

        match self.table.try_lock() {
            Ok(mut table) => {
                match table.query_create(full_records) {
                    Ok(_)  => self.result = Ok("records created successfully".to_owned()),
                    Err(e) => self.result = Err(["error creating records ".to_owned(), e.clone()].concat()),
                }
            },
            Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
        }
    }
}

/* 
 * MARK: TableQueryRead
 */
pub struct TableQueryRead   { table: Arc<Mutex<table::Table>>, name: String, pub result: Result<String, String> }

impl TableQueryRead {
    pub fn new(qname: String, table: Arc<Mutex<table::Table>>) -> Self {
        TableQueryRead { table: table, name: qname, result: Err("Query has not yet been run".to_owned()) }
    }

    pub fn parse(&mut self, body: Value) {
        let coldefs = match self.table.try_lock() {
            Ok(table) => table.column_definition.iter().map(|celldef| match celldef { 
                cell::Cell::CellDef { name, ctype, .. } => (name.clone(), ctype.clone()),
                _ => ("".to_owned(), cell::CellValue::String{ name: "".to_owned(), data: None }) })
                .filter(|celldef| !celldef.0.is_empty())
                .collect::<Vec<(String, CellValue)>>(),
            Err(e) =>panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
        };
        let conditions = match body["conditions"].as_array() {
            Some(conditionsarr) => {
                let conditions = conditionsarr.iter().map(|condition| match condition.as_array() {
                    Some(conditionarr) => match conditionarr.len() {
                        1 => {
                            println!("{:?}", conditionarr);
                            match conditionarr.first(){
                                Some(arr) => {
                                    println!("{:?}", arr);
                                    match arr.as_str() {
                                        Some(str) => match str {
                                            "*" => {
                                                let all = conditional::Condition { target_column: "".to_owned(), conditional: conditional::Conditional::All, value: CellValue::Bool { name: "".to_owned(), data: None }, relational: None };
                                                Some(all)
                                            },
                                            _ => None
                                        },
                                        _ => None
                                    }
                                },
                                None => None
                            }
                        },
                        3|4 => {
                            let conditions = match coldefs.iter().find(|celldef| celldef.0 == conditionsarr[0]) {
                                Some(celldef) => match conditionsarr.len() {
                                    3 => {
                                        let condition = match &celldef.1 {
                                            cell::CellValue::String { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::String { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => Some(val.to_owned()), None => None } }, relational: None }),
                                            cell::CellValue::Bool   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Bool   { name: name.to_owned(), data: conditionsarr[3].as_bool() }, relational: None }),
                                            cell::CellValue::UInt   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::UInt   { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<u32>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: None }),
                                            cell::CellValue::ULong  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::ULong  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<u128>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: None }),
                                            cell::CellValue::IInt   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::IInt   { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<i32>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: None }),
                                            cell::CellValue::ILong  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::ILong  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<i128>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: None }),
                                            cell::CellValue::Float  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Float  { name: name.to_owned(), data: conditionsarr[3].as_f64() }, relational: None }),
                                            cell::CellValue::Bytes  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Bytes  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => Some(<String as AsRef<[u8]>>::as_ref(&val.to_owned()).chunks(2).map(|pair| match pair[0] { b'A'..=b'F' => pair[0]-b'A'+10, _ => pair[0] } << 4 |  match pair[1] { b'A'..=b'F' => pair[1]-b'A'+10, _ => pair[1] }).map(|a| a as u8).collect::<Vec<u8>>()), None => None } }, relational: None }),
                                            _ => None,
                                        };
                                        condition
                                    },
                                    4 => {
                                        let condition = match &celldef.1 {
                                            cell::CellValue::String { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::String { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => Some(val.to_owned()), None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                            cell::CellValue::Bool   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Bool   { name: name.to_owned(), data: conditionsarr[3].as_bool() }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                            cell::CellValue::UInt   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::UInt   { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<u32>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                            cell::CellValue::ULong  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::ULong  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<u128>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                            cell::CellValue::IInt   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::IInt   { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<i32>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                            cell::CellValue::ILong  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::ILong  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<i128>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                            cell::CellValue::Float  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Float  { name: name.to_owned(), data: conditionsarr[3].as_f64() }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                            cell::CellValue::Bytes  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Bytes  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => Some(<String as AsRef<[u8]>>::as_ref(&val.to_owned()).chunks(2).map(|pair| match pair[0] { b'A'..=b'F' => pair[0]-b'A'+10, _ => pair[0] } << 4 |  match pair[1] { b'A'..=b'F' => pair[1]-b'A'+10, _ => pair[1] }).map(|a| a as u8).collect::<Vec<u8>>()), None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                            _ => None,
                                        };
                                        condition
                                    },
                                    _ => None
                                },
                                None => None
                            };
                            conditions
                        },
                        _ => None
                    },
                    None => None
                })
                .map(|a| match a { Some(a) => a, None => Condition { target_column: "".to_owned(), conditional: Conditional::parse("!=".to_owned()).unwrap(), value: CellValue::Bool { name: "".to_owned(), data: None }, relational: None } })
                .filter(|conditional| match conditional {
                    Condition { target_column, conditional, ..} => match target_column.is_empty() {
                        true => match conditional {
                            Conditional::All => true,
                            _ => false
                        },
                        false => true
                    }
                })
                .collect::<Vec<Condition>>();
                Some(conditions)
            },
            None => None
        };
        println!("conditions vec {:?}", conditions);
        match conditions {
            Some(conditions) => self.run(conditions),
            _ => self.result = Err("conditions could not be formatted".to_owned())
        }
    }

    /* 
     * loop over each record in the table and check each matching column with the supplied conditions
     */
    pub fn run(&mut self, conditions: Vec<conditional::Condition>) {
        println!("conditions vec {:?}", conditions);
        match conditions.len() {
            1.. => {
                match self.table.try_lock() {
                    Ok(mut table) => self.result = Ok(table.query_search_columns(&conditions)),
                    Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
                }
            },
            _ => self.result = Err("length of conditional list < 1".to_owned())
        }
    }
}

/* 
 * MARK: TableQueryUpdate
 */
pub struct TableQueryUpdate { table: Arc<Mutex<table::Table>>, name: String, pub result: Result<String, String> }

impl TableQueryUpdate {
    pub fn new(qname: String, table: Arc<Mutex<table::Table>>) -> Self {
        TableQueryUpdate { table: table, name: qname, result: Err("Query has not yet been run or implemented".to_owned()) }
    }

    pub fn parse(&mut self, body: Value) {
        todo!()
    }

    pub fn run(&mut self, conditions: Vec<conditional::Condition>, mut records: Vec<record::Record>) {
        todo!();
    }
}

/* 
 * MARK: TableQueryDelete
 */
pub struct TableQueryDelete { table: Arc<Mutex<table::Table>>, name: String, pub result: Result<String, String> }

impl TableQueryDelete {
    pub fn new(qname: String, table: Arc<Mutex<table::Table>>) -> Self {
        TableQueryDelete { table: table, name: qname, result: Err("Query has not yet been run or implemented".to_owned()) }
    }

    pub fn parse(&mut self, body: Value) {
        let coldefs = match self.table.try_lock() {
            Ok(table) => table.column_definition.iter().map(|celldef| match celldef { 
                cell::Cell::CellDef { name, ctype, .. } => (name.clone(), ctype.clone()),
                _ => ("".to_owned(), cell::CellValue::String{ name: "".to_owned(), data: None }) })
                .filter(|celldef| !celldef.0.is_empty())
                .collect::<Vec<(String, CellValue)>>(),
            Err(e) =>panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
        };
        let conditions = match body["conditions"].as_array() {
            Some(conditionsarr) => Some(conditionsarr.iter().map(|condition| match condition.as_array() {
                Some(conditionarr) => match conditionarr.len() {
                    1 => match conditionsarr.first().unwrap().as_str() {
                        Some(str) => match str {
                            "*" => Some(conditional::Condition { target_column: "".to_owned(), conditional: conditional::Conditional::All, value: CellValue::Bool { name: "".to_owned(), data: None }, relational: None }),
                            _ => None
                        },
                        _ => None
                    },
                    3|4 => match coldefs.iter().find(|celldef| celldef.0 == conditionsarr[0]) {
                        Some(celldef) => match conditionsarr.len() {
                            3 => match &celldef.1 {
                                cell::CellValue::String { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::String { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => Some(val.to_owned()), None => None } }, relational: None }),
                                cell::CellValue::Bool   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Bool   { name: name.to_owned(), data: conditionsarr[3].as_bool() }, relational: None }),
                                cell::CellValue::UInt   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::UInt   { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<u32>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: None }),
                                cell::CellValue::ULong  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::ULong  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<u128>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: None }),
                                cell::CellValue::IInt   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::IInt   { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<i32>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: None }),
                                cell::CellValue::ILong  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::ILong  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<i128>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: None }),
                                cell::CellValue::Float  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Float  { name: name.to_owned(), data: conditionsarr[3].as_f64() }, relational: None }),
                                cell::CellValue::Bytes  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Bytes  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => Some(<String as AsRef<[u8]>>::as_ref(&val.to_owned()).chunks(2).map(|pair| match pair[0] { b'A'..=b'F' => pair[0]-b'A'+10, _ => pair[0] } << 4 |  match pair[1] { b'A'..=b'F' => pair[1]-b'A'+10, _ => pair[1] }).map(|a| a as u8).collect::<Vec<u8>>()), None => None } }, relational: None }),
                                _ => None,
                            },
                            4 => match &celldef.1 {
                                cell::CellValue::String { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::String { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => Some(val.to_owned()), None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                cell::CellValue::Bool   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Bool   { name: name.to_owned(), data: conditionsarr[3].as_bool() }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                cell::CellValue::UInt   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::UInt   { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<u32>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                cell::CellValue::ULong  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::ULong  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<u128>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                cell::CellValue::IInt   { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::IInt   { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<i32>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                cell::CellValue::ILong  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::ILong  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => match val.parse::<i128>() { Ok(val) => Some(val), _ => None } , None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                cell::CellValue::Float  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Float  { name: name.to_owned(), data: conditionsarr[3].as_f64() }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                cell::CellValue::Bytes  { name, ..} => Some(conditional::Condition { target_column: conditionsarr[0].as_str().unwrap().to_owned(), conditional: Conditional::parse(conditionsarr[1].as_str().unwrap().to_owned()).unwrap(), value: cell::CellValue::Bytes  { name: name.to_owned(), data: match conditionsarr[3].as_str() { Some(val) => Some(<String as AsRef<[u8]>>::as_ref(&val.to_owned()).chunks(2).map(|pair| match pair[0] { b'A'..=b'F' => pair[0]-b'A'+10, _ => pair[0] } << 4 |  match pair[1] { b'A'..=b'F' => pair[1]-b'A'+10, _ => pair[1] }).map(|a| a as u8).collect::<Vec<u8>>()), None => None } }, relational: Some(Relation::parse(conditionsarr[4].as_str().unwrap().to_owned()).unwrap()) }),
                                _ => None,
                            },
                            _ => None
                        },
                        None => None
                    },
                    _ => None
                },
                None => None
            })
            .map(|a| match a { Some(a) => a, None => Condition { target_column: "".to_owned(), conditional: Conditional::parse("!=".to_owned()).unwrap(), value: CellValue::Bool { name: "".to_owned(), data: None }, relational: None } })
            .filter(|conditional| match conditional {
                Condition { target_column, conditional, ..} => match target_column.is_empty() {
                    true => match conditional {
                        Conditional::All => true,
                        _ => false
                    },
                    false => true
                }
            })
            .collect::<Vec<Condition>>()),
            None => None
        };
        match conditions {
            Some(conditions) => self.run(conditions),
            _ => self.result = Err("conditions could not be formatted".to_owned())
        }
    }

    pub fn run(&mut self, conditions: Vec<conditional::Condition> ) {
        match conditions.len() {
            1.. => match self.table.try_lock() {
                    Ok(mut table) => match conditions.iter().all(|condition| match table.column_definition.iter().find(|coldef| match coldef { 
                        cell::Cell::CellDef { name, .. } => name == &condition.target_column, 
                        _ => false 
                    }) { Some(_) => true, None => { self.result = Err(["target column \"", &condition.target_column[..], "\" does not exist on target table"].concat()); false} }
                ) {
                    true => self.result = table.query_delete_records(&conditions),
                    false => {}
                },
                Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())
            },
            _ => self.result = Err("negative length of conditional list".to_owned())
        }
    }
}



/* 
 * MARK: QueryDatabase
 * these queries should be limited to only operations that 
 * create update or delete table structures within the database
 */
pub enum QueryDatabase {
    QueryDatabaseCreateTable(QueryDatabaseCreateTable),
    QueryDatabaseUpdateTable(QueryDatabaseUpdateTable),
    QueryDatabaseDeleteTable(QueryDatabaseDeleteTable),
    QueryDatabaseInDevToggle(QueryDatabaseInDevToggle),
}

impl QueryDatabase {
    pub fn run<'a>(&mut self, admin_db: Arc<Mutex<Database<'a>>>, mut database: Option<&mut MutexGuard<Database<'a>>>, body: Value) {
        match self {
            QueryDatabase::QueryDatabaseCreateTable(QDCT) => match database { Some(db) => QDCT.parse(admin_db, db, body), None => QDCT.result = Err("no db pointer found".to_owned())},
            QueryDatabase::QueryDatabaseUpdateTable(QDUT) => match database { Some(db) => QDUT.parse(admin_db, db, body), None => QDUT.result = Err("no db pointer found".to_owned())},
            QueryDatabase::QueryDatabaseDeleteTable(QDDT) => match database { Some(db) => QDDT.parse(db, body), None => QDDT.result = Err("no db pointer found".to_owned())},
            QueryDatabase::QueryDatabaseInDevToggle(QDIDT) => match database { Some(db) => QDIDT.run(db), None => QDIDT.result = Err("no db pointer found".to_owned())},
        }
    }

    pub fn result(&self) -> Result<String, String> {
        match self {
            QueryDatabase::QueryDatabaseCreateTable(QDCT) => QDCT.result.clone(), 
            QueryDatabase::QueryDatabaseUpdateTable(QDUT) => QDUT.result.clone(), 
            QueryDatabase::QueryDatabaseDeleteTable(QDDT) => QDDT.result.clone(), 
            QueryDatabase::QueryDatabaseInDevToggle(QDIDT) => QDIDT.result.clone(), 
        }
    }

    pub fn set_result(&mut self, result: Result<String, String>) {
        match self {
            QueryDatabase::QueryDatabaseCreateTable(QDCT) => QDCT.result = result, 
            QueryDatabase::QueryDatabaseUpdateTable(QDUT) => QDUT.result = result, 
            QueryDatabase::QueryDatabaseDeleteTable(QDDT) => QDDT.result = result, 
            QueryDatabase::QueryDatabaseInDevToggle(QDIDT) => QDIDT.result = result, 
        }
    }
}

/* 
 * MARK: QueryDatabaseCreateTable
 */
pub struct QueryDatabaseCreateTable { name: String, pub result: Result<String, String> }

impl QueryDatabaseCreateTable {
    pub fn new(name: String) -> Self {
        QueryDatabaseCreateTable { name, result: Err("query has not yet been run".to_owned()) }
    }

    pub fn parse<'a>(&mut self, admin_db: Arc<Mutex<Database<'a>>>, database: &mut MutexGuard<Database<'a>>, body: Value) {
        println!("build new table query parse");
        let column_defs = match body["columns"].as_array() {
            Some(column_def_arr) => Some(column_def_arr.iter().map(|coldef| match coldef.as_array() {
                    Some(coldefarr) => Some((
                        coldefarr[0].as_str()?.to_owned(), 
                        coldefarr[1].as_str()?.to_owned(), 
                        match coldefarr[2].as_str() { Some(val) => match val.is_empty() { true => None, false => Some(val.to_owned()) }, _ => None }, 
                        match coldefarr[3].as_str() { Some(str) => match str { "true"|"True"|"TRUE"|"1" => true, _ => false }, _ => false }, 
                        match coldefarr[4].as_str() { Some(str) => match str { "true"|"True"|"TRUE"|"1" => true, _ => false }, _ => false },
                        match coldefarr[5].as_str() { Some(str) => match Regex::new(r"(?P<table_name>[\w]*).(?P<column_name>[\w]*)").unwrap().captures(str) {
                                Some(a) => Some((a["table_name"].to_owned(), a["column_name"].to_owned())),
                                None => None
                            }, _ => None 
                        }
                    )),
                    None => None
                }).map(|a| match a {
                    Some(a) => a,
                    None => ("".to_owned(), "".to_owned(), None, false, false, None)
                }).filter(|b| !b.0.is_empty())
                .collect::<Vec<(String, String, Option<String>, bool, bool, Option<(String, String)>)>>()
            ),
            None => None
        };
        match body["table_name"].as_str() {
            Some(table_name) => match column_defs {
                Some(column_defs) => self.run(admin_db, database, table_name.to_owned(), column_defs),
                None => self.result = Err("column definitions could not be parsed".to_owned())
            },
            None => self.result = Err("table name could not be parsed".to_owned())
        }
    }

    pub fn run<'a>(&mut self, admin_db: Arc<Mutex<Database<'a>>>, database: &mut MutexGuard<Database<'a>>, table_name: String, columns: Vec<(String, String, Option<String>, bool, bool, Option<(String, String)>)>) {
        println!("build new table query run db");
        match database.tables.iter().any(|table| match table.try_lock() { Ok(table) => table.name == table_name, Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())}) {
            true => {
                println!("table does exist err");
                self.result = Err("table with requested name already exists".to_owned());
                println!("after");
            },
            false => {
                println!("table does not exist building");
                database.build_table(admin_db, table_name.clone(), columns);
                self.result = match database.tables.iter().any(|table| match table.try_lock() { Ok(table) => table.name == table_name, Err(e) => panic!("{}", ["shits fucked ".to_owned(), e.to_string()].concat())}) { 
                    true => Ok("table successfully created".to_owned()), 
                    false => Err("table could not be created".to_owned())
                }
            }
        };
    }
}


/* 
 * MARK: QueryDatabaseUpdateTable
 */
pub struct QueryDatabaseUpdateTable { name: String, pub result: Result<String, String> }

impl QueryDatabaseUpdateTable {
    pub fn new(name: String) -> Self {
        QueryDatabaseUpdateTable { name , result: Err("query has not yet been run".to_owned()) }
    }

    pub fn parse(&mut self, admin_db: Arc<Mutex<Database>>, database: &mut MutexGuard<Database>, body: Value) {
        println!("parsing");
        self.run(database);
    }

    pub fn run(&mut self, database: &mut MutexGuard<Database>) {
        println!("running");
        self.result = Ok("update query has been run".to_owned());
    }
}


/* 
 * MARK: QueryDatabaseDeleteTable
 */
pub struct QueryDatabaseDeleteTable { name: String, pub result: Result<String, String> }

impl QueryDatabaseDeleteTable {
    pub fn new(name: String) -> Self {
        QueryDatabaseDeleteTable { name , result: Err("query has not yet been run".to_owned()) }
    }

    pub fn parse(&mut self, database: &mut MutexGuard<Database>, body: Value) {
        match body["table_name"].as_str() {
            Some(table_name) => self.run(database, table_name.to_owned()),
            None => self.result = Err("table name could not be parsed".to_owned())
        }
    }

    pub fn run(&mut self, database: &mut MutexGuard<Database>, table_name: String) {
        match database.delete_table(table_name) {
            Ok(e) => self.result = Ok(e),
            Err(e) => self.result = Err(e),
        }
    }
}

pub struct QueryDatabaseInDevToggle { name: String, pub result: Result<String, String> }

impl QueryDatabaseInDevToggle {
    pub fn new(name: String) -> Self {
        QueryDatabaseInDevToggle { name, result: Err("query has not yet been run".to_owned()) }
    }

    pub fn run(&mut self, database: &mut MutexGuard<Database>) {
        database.indev = !database.indev;
        self.result = Ok("databse indev status successfuly toggled".to_owned())
    }
}