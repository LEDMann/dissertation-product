use std::{collections::HashMap, env, fs::{self, File}, io::{Read, Write}, iter::Map, ops::Deref, path::Path, ptr::null};
use crate::database::part::Part;

use super::{cell::{self, Cell, CellValue}, conditional, part, record};
use get_size::GetSize;
use serde_json::{Value, json};

pub struct Table {
    pub name: String,
    pub directory: String,
    pub auto_increment: bool,
    pub column_definition: Vec<cell::Cell>,
    pub records: Vec<part::Part>,
}

/* 
 * some of the code used here was copied from the hex crate which was published under the MIT licence
Copyright (c) 2013-2014 The Rust Project Developers.
Copyright (c) 2015-2020 The rust-hex Developers

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
impl Table {
    /* 
     * MARK: new
     *                                                            cname   dtype   default value   nullable unique foreign key
     */
    pub fn new(db_name: String, table_name: String, columns: Vec<(String, String, Option<String>, bool, bool, Option<(String, String)>)>, ai: bool) -> Self {
        println!("build new table {table_name}");
        let new_table = Table {
            name: table_name,
            directory: "".to_owned(),
            auto_increment: ai,
            column_definition: columns.into_iter().enumerate().map(|(i, (col_name, data_type, default, nullable, unique, pkey))| {
                    match &data_type[..] {
                        "String" => cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::String { name: col_name.to_owned(), data: match &default { Some(val) => Some(val.to_owned()), None => None } },                                                                                                                                                                                                                                                   default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                        "Bool" =>   cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::Bool   { name: col_name.to_owned(), data: match &default { Some(val) => match val.parse::<bool>() { Ok(bool) => Some(bool),  _ => None }, None => None } },                                                                                                                                                                                                 default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                        "UInt" =>   cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::UInt   { name: col_name.to_owned(), data: match &default { Some(val) => match val.parse::<u32>()  { Ok(int) =>   Some(int),   _ => None }, None => None } },                                                                                                                                                                                                 default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                        "ULong" =>  cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::ULong  { name: col_name.to_owned(), data: match &default { Some(val) => match val.parse::<u128>() { Ok(int) =>  Some(int),   _ => None }, None => None } },                                                                                                                                                                                                 default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                        "IInt" =>   cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::IInt   { name: col_name.to_owned(), data: match &default { Some(val) => match val.parse::<i32>()  { Ok(int) =>   Some(int),   _ => None }, None => None } },                                                                                                                                                                                                 default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                        "ILong" =>  cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::ILong  { name: col_name.to_owned(), data: match &default { Some(val) => match val.parse::<i128>() { Ok(int) =>  Some(int),   _ => None }, None => None } },                                                                                                                                                                                                 default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                        "Float" =>  cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::Float  { name: col_name.to_owned(), data: match &default { Some(val) => match val.parse::<f64>()  { Ok(float) => Some(float), _ => None }, None => None } },                                                                                                                                                                                                 default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                        "Bytes" =>  cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::Bytes  { name: col_name.to_owned(), data: match &default { Some(val) => Some(<String as AsRef<[u8]>>::as_ref(&val).chunks(2).map(|pair| match pair[0] { b'A'..=b'F' => pair[0]-b'A'+10, _ => pair[0] } << 4 |  match pair[1] { b'A'..=b'F' => pair[1]-b'A'+10, _ => pair[1] }).map(|a| a as u8).collect::<Vec<u8>>()), _ => None } }, default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                        _ =>        cell::Cell::CellDef { name: col_name.to_string(), index: i as u16, ctype: cell::CellValue::String { name: col_name.to_owned(), data: match &default { Some(val) => Some(val.to_owned()), None => None } },                                                                                                                                                                                                                                                   default: match default { Some(_) => true, None => false }, not_null: nullable, unique: unique, primary_key: match i { 0 => true, _ => false }, foreign_key: match &pkey { Some(_) => pkey, None => None } },
                    }
                }).collect::<Vec<cell::Cell>>(), 
            records: vec![],
        };
        match new_table.init_dir(db_name) {
            Ok(y) => y,
            Err(n) => n.0
        }
    }

    /* 
     * MARK: build from directory
     */
    pub fn build_from_dir(table_dir: String) -> Result<Self, String> {
        // println!("build table from dir {}", &table_dir.clone());
        let parts = match fs::read_dir(&table_dir.clone()) {
            Ok(e)  => e.into_iter()
                                .map(|a| match a {
                                    Ok(e) => e.file_name().into_string().unwrap(),
                                    Err(e) => "".to_string(),
                                })
                                .filter(|b| !b.is_empty())
                                .filter(|c| c.starts_with("p"))
                                .collect::<Vec<String>>(),
            Err(e) => return Err([e.to_string(), "unable to find database directory".to_string()].concat()),
        };
        println!("build table from dir {}", &table_dir.clone());

        let (ai, coldefs): (bool, Vec<Cell>) = match File::open([&table_dir.clone(), ".def"].join("\\")) {
            Ok(mut e) => {
                let mut buf: Vec<u8> = vec![];
                e.read_to_end(&mut buf).unwrap();
                match bincode::deserialize(&buf) {
                    Ok(e) => e,
                    Err(e) => {
                        println!("{}", e.to_string());
                        (false, vec![])
                    }
                }
            },
            Err(e) => (false, vec![])
        };

        Ok(Table {
            name: match &table_dir.rsplit("\\").next() { Some(e) => e.to_string(), None => table_dir.split("\\").last().unwrap().to_owned() },
            directory: table_dir.clone(),
            auto_increment: ai,
            column_definition: coldefs,
            records: parts.into_iter()
                            .enumerate()
                            .map(|(i, a)| match part::Part::load_from_dir([&table_dir[..], &a[..]].join("\\"), 4096, i as u32 ) {
                                Ok(b) => b, 
                                Err(_) => part::Part { 
                                    index: i as u32,
                                    size: 4096,
                                    full: false,
                                    directory: "".to_owned(),
                                    key_range: vec![],
                                    records: vec![],
                            }}).filter(|c| !c.directory.is_empty())
                            .collect::<Vec<part::Part>>(),
        })
    }

    /* 
     * MARK: Initilise Directories
     */
    fn init_dir(mut self, db_name: String) -> Result<Self, (Self, String)> {
        println!("initialising directory {db_name}");
        let curr_dir_res = env::current_dir();
        let curr_dir = match curr_dir_res {
            Ok(ref path_buf) => match path_buf.to_str() {
                Some(path_str) => path_str,
                None => return Err((self, "unable to parse path as str".to_string()))
            },
            Err(e) => return Err((self, ["unable to find current directory\n".to_string(), e.to_string()].concat())),
        };
        let table_dir = [curr_dir, "databases", &db_name, &self.name].join("\\");
        match fs::create_dir_all(&table_dir) {
            Ok(d) => self.directory = table_dir.clone(),
            Err(e) => return Err((self, ["unable to create table directory\n".to_string(), e.to_string()].concat()))
        }
        match File::create([&table_dir, ".def"].join("\\")) {
            Ok(mut e) => match e.write_all(&bincode::serialize(&(&self.auto_increment, &self.column_definition)).unwrap()) {
                Ok(_) => {}
                Err(e) => return Err((self, ["unable to write def to def file\n".to_string(), e.to_string()].concat())),
            },
            Err(e) => return Err((self, ["unable to create table definition file\n".to_string(), e.to_string()].concat()))
        };
        self.records.push(Part::new(&table_dir, 0, 4096));
        self.directory = table_dir;
        Ok(self)
    }

    /* 
     * MARK: Query search in columns
     */
    pub fn query_search_columns(&mut self, conditions: &Vec<conditional::Condition>) -> Result<String, String> {
        let mut res = vec![];
        let _ = &self.records.iter_mut().for_each(|part| {
            match part.reload() { 
                Ok(_) => for record in part.query_search_columns(conditions) {
                    res.push(record.clone());
                }, 
                Err(_) => {} 
            }
        });
        res
    }

    /* 
     * MARK: Query delete records in columns
     */
    pub fn query_delete_records(&mut self, conditions: &Vec<conditional::Condition>) -> Result<String, String> {
        let mut res = Ok("deletion successful".to_owned());
        let _ = &self.records.iter_mut().for_each(|part| {
            match part.reload() { 
                Ok(_) => res = part.query_delete_records(conditions), 
                Err(_) => res = Err("could not reload part".to_owned())
            }
            part.save();
        });
        res
    }

    /* 
     * MARK: Query delete table
     */
    pub fn query_delete_table(&mut self) -> Result<String, String> {
        match self.directory.is_empty() {
            true => {
                self.records = vec![];
                Ok("table object deleted".to_owned())
            },
            false => {
                match self.records.iter_mut().enumerate().map(|(i, part)| part.delete(i as u32)).all(|partres| partres.is_ok()) {
                    true => match fs::remove_file([&self.directory, ".def"].join("\\")) {
                        Ok(_) => match fs::remove_dir(&self.directory) {
                            Ok(_) => Ok("table directory deleted".to_owned()),
                            Err(e) => Err(["could not delete directory ", &e.to_string()].concat())
                        },
                        Err(e) => Err(["could not delete definition file ", &e.to_string()].concat())
                    },
                    false => Err("part could not be deleted".to_owned())
                }
            }
        }
    }

    /* 
     * MARK: Query add record
     * while there are records to be saved search for a part to save it in, if no part is available create a new part and save it in there
     */
    pub fn query_create(&mut self, mut records: Vec<record::Record>) -> Result<String, String> {
        println!("table {} adding {:?}", self.name, records);

        let mut res:  Result<String, String> = Ok("record creation successful".to_owned());

        records.retain_mut(|record| {
            println!("moving a record {:?}", record);
            match self.records.iter_mut().find(|part| !part.full) {
                Some(part) => { 
                    println!("part found {:?}", record);
                    res = part.query_create_record(std::mem::take(record), self.column_definition.first().unwrap().clone()) 
                }
                None => {
                    println!("creating new part {:?}", record);
                    let mut new_part = Part::new(&self.directory[..], self.records.len(), 4096);
                    res = new_part.query_create_record(std::mem::take(record), self.column_definition.first().unwrap().clone());
                    self.records.push(new_part);
                }
            }
            false
        });

        match records.is_empty() {
            true => match res {
                Ok(_) => res = Ok("records created".to_owned()),
                Err(e) => res = Err(e)
            } 
            false => res = Err("records not successfully transferred".to_owned()),
        };
        res
    }
}