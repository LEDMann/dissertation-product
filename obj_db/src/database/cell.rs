use std::{default, fmt::Display};

use get_size::GetSize;
use serde::{Deserialize, Serialize};

#[derive(Debug, GetSize, Clone, Serialize, Deserialize)]
pub enum Cell {
    CellValue(CellValue),
    CellDef {
        name: String,
        index: u16,
        ctype: CellValue,
        default: bool,
        not_null: bool,
        unique: bool,
        primary_key: bool,
        foreign_key: Option<(String, String)>,
    },
}

#[derive(Debug, GetSize, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CellValue {
    String {
        name: String,
        data: Option<String>,
    },
    Bool {
        name: String,
        data: Option<bool>,
    },
    UInt {
        name: String,
        data: Option<u32>,
    },
    ULong {
        name: String,
        data: Option<u128>,
    },
    IInt {
        name: String,
        data: Option<i32>,
    },
    ILong {
        name: String,
        data: Option<i128>,
    },
    Float {
        name: String,
        data: Option<f64>,
    },
    Bytes {
        name: String,
        data: Option<Vec<u8>>,
    },
}

impl CellValue {
    pub fn name(&self) -> &str {
        match self {
            CellValue::String { name, .. } => return &name,
            CellValue::Bool   { name, .. } => return &name,
            CellValue::UInt   { name, .. } => return &name,
            CellValue::ULong  { name, .. } => return &name,
            CellValue::IInt   { name, .. } => return &name,
            CellValue::ILong  { name, .. } => return &name,
            CellValue::Float  { name, .. } => return &name,
            CellValue::Bytes  { name, .. } => return &name,
        }
    }

    pub fn data_str(&self) -> String {
        match self {
            CellValue::String { data, .. }   => return match &data { Some(e) => e.clone(), _ => "null".to_owned() },
            CellValue::Bool   { data, .. }     => return match &data { Some(e) => match e { true => "true".to_owned(), false => "false".to_owned() }, _ => "null".to_owned() },
            CellValue::UInt   { data, .. }      => return match &data { Some(e) => e.to_string(), _ => "null".to_owned() },
            CellValue::ULong  { data, .. }     => return match &data { Some(e) => e.to_string(), _ => "null".to_owned() },
            CellValue::IInt   { data, .. }      => return match &data { Some(e) => e.to_string(), _ => "null".to_owned() },
            CellValue::ILong  { data, .. }     => return match &data { Some(e) => e.to_string(), _ => "null".to_owned() },
            CellValue::Float  { data, .. }      => return match &data { Some(e) => e.to_string(), _ => "null".to_owned() },
            CellValue::Bytes  { data, .. }  => return match &data { Some(e) => e.iter().map(|a| format!("{:X?}", a).clone()).collect::<String>(), _ => "null".to_owned() },
        }
    }

    pub fn comp_name(self, comp_name: &String) -> bool {
        match self {
            CellValue::String { name, .. } => return name.eq(comp_name).clone(),
            CellValue::Bool   { name, .. } => return name.eq(comp_name).clone(),
            CellValue::UInt   { name, .. } => return name.eq(comp_name).clone(),
            CellValue::ULong  { name, .. } => return name.eq(comp_name).clone(),
            CellValue::IInt   { name, .. } => return name.eq(comp_name).clone(),
            CellValue::ILong  { name, .. } => return name.eq(comp_name).clone(),
            CellValue::Float  { name, .. } => return name.eq(comp_name).clone(),
            CellValue::Bytes  { name, .. } => return name.eq(comp_name).clone(),
        }
    }
}

impl Default for CellValue {
    fn default() -> Self {
        CellValue::String { name: "".to_string(), data: None }
    }
}

impl Display for CellValue {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.write_str("{{")?;
        fmt.write_str(&self.name())?;
        fmt.write_str(", ")?;
        fmt.write_str(&self.data_str())?;
        fmt.write_str("}}")?;
        Ok(())
    }
}