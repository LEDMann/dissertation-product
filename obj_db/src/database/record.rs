use std::fmt::Display;

use super::cell;
use super::cell::CellValue;
use super::conditional;
use serde::{Deserialize, Serialize};
use get_size::GetSize;

#[derive(Clone, Debug, Default, GetSize, Serialize, Deserialize)]
pub struct Record {
    pub columns: Vec<cell::CellValue>
}

impl Record {
    pub fn query_check(&self, condition: &conditional::Condition) -> bool {
        match condition.conditional {
            conditional::Conditional::NotEqual     => match self.columns.iter().find(|col| col.name() == condition.target_column) { Some(cell) => *cell != condition.value, None => false },
            conditional::Conditional::Equal        => match self.columns.iter().find(|col| col.name() == condition.target_column) { Some(cell) => *cell == condition.value, None => false },
            conditional::Conditional::EqualGreater => match self.columns.iter().find(|col| col.name() == condition.target_column) { Some(cell) => *cell >= condition.value, None => false },
            conditional::Conditional::EqualSmaller => match self.columns.iter().find(|col| col.name() == condition.target_column) { Some(cell) => *cell <= condition.value, None => false },
            conditional::Conditional::Greater      => match self.columns.iter().find(|col| col.name() == condition.target_column) { Some(cell) => *cell >  condition.value, None => false },
            conditional::Conditional::Smaller      => match self.columns.iter().find(|col| col.name() == condition.target_column) { Some(cell) => *cell <  condition.value, None => false },
            conditional::Conditional::All          => true,
        }
    }
}

impl Display for Record {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.write_str("{ ").unwrap();
        self.columns.iter().enumerate().for_each(|(i, a)| { 
            fmt.write_str(&a.to_string()).unwrap(); 
            if i != self.columns.len()-1 { fmt.write_str(", ").unwrap() } else { fmt.write_str("").unwrap() }; 
        });
        fmt.write_str(" }").unwrap();
        Ok(())
    }
}

pub struct RecordCollection(pub Vec<Record>);

impl RecordCollection {
    pub fn new() -> RecordCollection {
        RecordCollection(Vec::new())
    }

    pub fn from(records: Vec<Record>) -> RecordCollection {
        RecordCollection(records)
    }

    pub fn get_vec(self) -> Vec<Record> {
        self.0
    }

    fn add(&mut self, elem: Record) {
        self.0.push(elem);
    }
}

impl FromIterator<Record> for RecordCollection {
    fn from_iter<I: IntoIterator<Item = Record>>(iter: I) -> Self {
        let mut rc = RecordCollection::new();
        for i in iter {
            rc.add(i);
        }
        rc
    }
}

impl Display for RecordCollection {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.write_str("{{");
        self.0.iter().for_each(|a| { fmt.write_str(&a.to_string()); });
        fmt.write_str("}},");
        Ok(())
    }
}