use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use super::cell;
use super::cell::Cell;
use super::conditional;
use super::record;
use super::record::Record;

pub struct Part {
    pub index: u32,
    pub size: u16,
    pub full: bool, 
    pub directory: String,
    pub key_range: Vec<u128>,
    pub records: Vec<record::Record>,
}

impl Part {
    // MARK: new
    pub fn new(directory: &str, index: usize, size: u16) -> Self {
        let new_part = Part {
            index: index as u32,
            size: size,
            full: false,
            directory: format!("{}/p{:X}", directory, index).to_owned(),
            key_range: vec![],
            records: vec![],
        };
        match new_part.init_dir() {
            Ok(y) => y,
            Err(n) => n.0
        }
    }

    fn init_dir(mut self) -> Result<Self, (Self, String)> {
        match File::create(&self.directory) {
            Ok(_) => self.directory = self.directory.clone(),
            Err(e) => return Err((self, ["unable to create table directory\n".to_string(), e.to_string()].concat()))
        }
        Ok(self)
    }

    pub fn load_from_dir(path: String, size: u16, index: u32) -> Result<Self, String> {
        println!("build part from file {}", &path.clone());
        let records: Vec<record::Record> = match File::open(path.clone()) {
            Ok(mut e)  => {
                let mut buf = vec![];
                e.read_to_end(&mut buf).unwrap();
                match buf.is_empty() {
                    true => vec![],
                    false => {
                        let rs: Vec<record::Record> = bincode::deserialize(&buf).unwrap();
                        rs
                    }
                }
            },
            Err(e) => return Err([e.to_string(), "unable to find part".to_string()].concat()),
        };

        Ok(Part {
            index: index,
            size: size,
            full: false,
            directory: path.clone(),
            key_range: records.iter().map(|r| match r.columns.first() {
                Some(e) => match e {
                    cell::CellValue::ULong { data, .. } => match data {
                        Some(e) => e.to_string(),
                        None => "".to_owned()
                    },
                    _ => "".to_owned()
                },
                None => "".to_owned()
            }).filter(|b| !b.is_empty()).map(|c| c.parse::<u128>().unwrap()).collect::<Vec<u128>>(),
            records: records,
        })
    }

    pub fn reload(&mut self) -> Result<String, String> {
        println!("reloading {:?}\n\n", self.records);
        match fs::File::open(self.directory.clone()) {
            Ok(mut p) => {
                let mut buf = vec![];
                p.read_to_end(&mut buf).unwrap();
                match buf.is_empty() {
                    true => { self.records = vec![] },
                    false => { self.records = bincode::deserialize(&buf).unwrap(); }
                };
                self.key_range = self.records.iter().map(|r| match r.columns.first() {
                    Some(e) => match e {
                        cell::CellValue::ULong { data, .. } => match data {
                            Some(e) => e.to_string(),
                            None => "".to_owned()
                        },
                        _ => "".to_owned()
                    },
                    None => "".to_owned()
                }).filter(|b| !b.is_empty()).map(|c| c.parse::<u128>().unwrap()).collect::<Vec<u128>>()
            },
            Err(_) => return Err("couldnt open file".to_owned())
        };
        println!("reloading {:?}\n\n", self.records);
        Ok("reloaded successully".to_owned())
    }

    /* 
     * MARK: save records to disc
     * (overwrite contents)
    */
    pub fn save(&mut self) -> Result<String, String> {
        let _ = match fs::OpenOptions::new().write(true).truncate(true).open(self.directory.clone()) {
            Ok(mut p) => {
                let mut writebuf: Vec<u8> = vec![];
                writebuf.extend(bincode::serialize(&self.records).unwrap());
                p.write(&writebuf)
            },
            Err(_) => return Err("couldnt open file".to_owned())
        };
        Ok("records successully saved to disk".to_owned())
    }

    /* 
     * MARK: delete part file
     * permanent
    */
    pub fn delete(&mut self, index: u32) -> Result<String, String> {
        match fs::remove_file(self.directory.clone()) {
            Ok(_) => return Ok("deletion successful".to_owned()),
            Err(e) => return Err(["could not delete part file", &e.to_string()].concat())
        };
    }

    pub fn empty(&mut self) -> Result<String, String> {
        self.records = vec![];
        Ok("records successully cleared from in memory part".to_owned())
    }

    /* 
     * MARK: Query search in columns
     */
    pub fn query_search_columns(&self, conditions: &Vec<conditional::Condition>) -> Result<String, String> {
        println!("{:?}", self.records);
        let matching_records = self.records.iter().filter(|r| conditions.iter().all(|condition| r.query_check(condition)) ).map(|b| b.to_owned()).collect::<record::RecordCollection>().get_vec();
        println!("{:?}", matching_records);
        Ok(matching_records.iter().map(|a| a.to_string()).collect::<Vec<String>>().join(", "))
    }

    /* 
     * MARK: Query create columns
     */
    pub fn query_create_record(&mut self, record: Record, table_indexer: Cell) -> Result<String, String> {
        print!("part {} adding record {:?}", self.index, record);
        let res = match table_indexer {
            Cell::CellDef { ctype, .. } => match record.columns.iter().find(|rcol| rcol.name() == ctype.name()) {
                Some(e) => match e {
                    cell::CellValue::ULong { data, .. } => match data {
                        Some(e)=> Ok(self.key_range.push(e.clone())),
                        None => Err("submitted table index is None")
                    }
                    _ => Err("table indexer waas not correct type of ULong")
                },
                None => Err("table indexer could not find matching name on record")
            },
            _ => Err("table indexer was not correct type")
        };
        match res {
            Ok(_) => {
                self.records.push(record);
                match self.save() {
                    Ok(_) => Ok("record created".to_owned()),
                    Err(e) => Err(e)
                }
            },
            Err(e) => Err(e.to_owned())
        }
    }

    /* 
     * MARK: Query delete in columns
     */
    pub fn query_delete_records(&mut self, conditions: &Vec<conditional::Condition>) -> Result<String, String> {
        let records_len = self.records.len();
        self.records.retain(|r| !conditions.iter().all(|condition| r.query_check(condition)));
        if records_len == self.records.len() {
            Ok("no matching records found or deleted".to_owned())
        } else {
            Ok("matching records found and deleted successfully".to_owned())
        }
    }
}