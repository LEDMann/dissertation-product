use regex::Regex;
use std::{
    borrow::BorrowMut, 
    vec, 
    env, 
    fs, 
    ops::Deref, 
    path::Path, 
    fs::{
        create_dir, 
        File
    }, 
    io::{self, 
        BufRead, 
        BufReader, 
        BufWriter, 
        Read, 
        Write
    }
};

pub mod database;
pub mod endpoint;

fn main() {
    todo!()
}