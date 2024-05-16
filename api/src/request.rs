use serde_json::{Value, json};
use std::{fmt, io::prelude::*, collections::HashMap, net::TcpStream};

pub struct Request {
    pub version: String,
    pub method: String,
    pub path: Vec<String>,
    pub headers: HashMap<String, String>,
    pub body: Value
}

impl Request {
    pub fn parse_stream(stream: &mut TcpStream) -> Self {
        let mut buf = [0; 2048];
        stream.read(&mut buf).unwrap();
        let mut stream_str = String::from_utf8_lossy(&buf[..]).into_owned();
        stream_str = stream_str.replace("\0", "").trim().to_string();

        let (headers_raw, body): (String, Value) = match stream_str.ends_with("\r\n\r\n") {
            true => (stream_str.trim().to_string(), json!(null)),
            false => match stream_str.split_once("\r\n\r\n") {
                Some((h, b)) => (h.trim().to_string(), serde_json::from_str(b).unwrap()),
                _ => (stream_str.trim().to_string(), json!(null))
            }
        };

        let (methods, headers): (String, HashMap<String, String>) = match headers_raw.split_once("\r\n") {
            Some((m, h)) => (m.trim().to_string(), h.split("\n").map(|a| a.split_once(": ").unwrap()).map(|b| { (b.0.trim().to_string(), b.1.trim().to_string()) }).collect::<HashMap<String, String>>()),
            _ => panic!("wtf do you mean the headers are only a single line")
        };

        let (method, path, version): (String, Vec<String>, String) = match methods.split_once(" ") {
            Some((method, rest)) => {
                let (path, version) = match rest.split_once(" ") {
                    Some((path_str, version)) => {
                        let path = match path_str.chars().filter(|c| *c == '/').count() {
                            1 => match path_str == "/" {
                                true => vec![],
                                false => vec![path_str.trim_start_matches('/').to_string()],
                            },
                            2.. => path_str.trim_matches('/').split("/").map(|a| a.to_string()).collect::<Vec<String>>(),
                            _ => vec![]
                        };
                        (path, version)
                    },
                    _ => panic!("really only 1 space in the method wtf")
                };
                (method.to_string(), path, version.to_string())
            },
            _ => panic!("really no spaces in the method wtf")
        };

        return Request {version: version, method: method, path: path, headers: headers, body: body }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.method)?;
        fmt.write_str("-/")?;
        fmt.write_str(&self.path.join("/"))?;
        fmt.write_str("-")?;
        fmt.write_str(&self.version)?;
        fmt.write_str("-")?;
        fmt.write_str(&serde_json::to_string(&self.headers).unwrap())?;
        fmt.write_str("-")?;
        fmt.write_str(&self.body.to_string())?;
        Ok(())
    }
}
