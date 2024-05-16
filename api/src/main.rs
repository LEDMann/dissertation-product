use rayon;
use request::Request;
use serde_json::json;
use std::{env, fs::{self, DirEntry, ReadDir}, io::Write, net::TcpListener, ops::{Deref, DerefMut}, sync::{Arc, Mutex, MutexGuard}};
use obj_db::{database::{self, Database}, endpoint::{self, Endpoint, runnable}};

mod request;

// WORK ON PART.RS RECORD CREATION

fn main() {
    let admin_db = Database::new("admin".to_owned(), None, "ADMIN".to_owned(), Some("admin_database".to_owned()));
    let mut databases: Arc<Mutex<Vec<Arc<Mutex<Database<'static>>>>>> = Arc::new(Mutex::new(vec![Arc::clone(&admin_db)]));
    let mut endpoints: Arc<Mutex<Vec<Endpoint<'static>>>> = Arc::new(Mutex::new(Endpoint::new_server(admin_db, "ADMIN".to_owned())));

    build_from_dir(Arc::clone(&databases), Arc::clone(&endpoints));

    let tcp_listener = TcpListener::bind("127.0.0.1:42069").unwrap();

    let tpool = rayon::ThreadPoolBuilder::new()
        .num_threads(12)
        .build()
        .unwrap();

    // accept connections and process them serially
    for stream in tcp_listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => panic!("local port binding failed, {}", e),
        };
        tpool.install(|| {
            let request = request::Request::parse_stream(&mut stream);
            println!("\n{}\n", request);
            let mut msg = "message not replaced".to_string();
            match endpoints.try_lock() {
                Ok(endpoints) => msg = match_endpoint(request, databases.clone(), endpoints),
                Err(e) => msg = e.to_string()
            }
            stream.write(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", msg.len(), msg).as_bytes()).unwrap();
            stream.flush().unwrap();
        });
    }
}

fn build_from_dir(databases: Arc<Mutex<Vec<Arc<Mutex<Database<'static>>>>>>, mut endpoints: Arc<Mutex<Vec<Endpoint<'static>>>>) -> Result<String, String>  {
    let curr_dir_res = env::current_dir();
    match curr_dir_res {
        Ok(ref path_buf) => match path_buf.to_str() {
            Some(path_str) => {
                match fs::read_dir([path_str, "databases"].join("\\")) {
                    Ok(rdr) => {
                        rdr.into_iter().for_each(|a| match a {
                            Ok(de) => match &de.file_type().unwrap().is_dir() {
                                true => match databases.try_lock() {
                                    Ok(mut dbl) => {
                                        let y = de.path().to_str().unwrap().to_owned();
                                        let mut pieces = y.rsplit("\\");
                                        match pieces.next() {
                                            Some(p) => match Database::build_from_dir(p.to_owned(), Some(Arc::clone(dbl.first().unwrap())), None) {
                                                Ok(ndb) => dbl.push(ndb),
                                                Err(e) => panic!("db could not be built from dir")
                                            },
                                            None => panic!("iterator over path could not get end of path"),
                                        }
                                    },
                                    Err(e) => panic!("mutex poisoned")
                                },
                                false => {}
                            },
                            Err(_) => panic!("error getting direntry")
                        });
                        Ok("".to_owned())
                    },
                    Err(e) => Err(e.to_string())
                }
            },
            None => Err("unable to parse path as str".to_string())
        },
        Err(e) => Err(["unable to find current directory\n".to_string(), e.to_string()].concat()),
    }
}

fn match_endpoint(request: Request, databases: Arc<Mutex<Vec<Arc<Mutex<Database<'static>>>>>>, mut endpoints: MutexGuard<Vec<Endpoint<'static>>>) -> String {
    println!("matching endpoint");
    match &request.method[..] {
        "CREATE_DATABASE" => match endpoints.iter_mut().find(|endpoint| endpoint.name == "create_database") {
            Some(mut new_db_endpoint) => {
                new_db_endpoint.run(None, request.body, None);
                match new_db_endpoint.result() {
                    Ok(mut e) => {
                        match new_db_endpoint.runnable.try_lock() { 
                            Ok(mut runnable) => match &mut *runnable {  
                                runnable::Runnable::Query(q) => match q { 
                                    endpoint::query::Query::QueryNewDatabase(qnd) => match &qnd.result { 
                                        Ok(ndb) => match databases.try_lock() { 
                                            Ok(mut dbs) => dbs.push(Arc::clone(&ndb)), 
                                            Err(err) => e = [e, err.to_string()].concat() 
                                        }, 
                                        Err(err) => e = [e, err.to_string()].concat() }, 
                                        _ => e = [e, "query is not a querynewdatabase".to_owned()].concat() 
                                    }, 
                                    _ => e = [e, "runnable is not a query".to_owned()].concat(),
                                runnable::Runnable::Script(_) => e = "scripts not yet implemented".to_owned()
                            },
                            Err(err) => e = err.to_string()
                        };
                        e
                    },
                    Err(e) => e
                }
            },
            None => "new database endpoint not found".to_owned()
        },
        _ => match databases.try_lock() { 
            Ok(dbs) => {
                println!("fdsfds");
                match request.path.len() {
                    1 => match dbs.iter().find(|a| match a.try_lock() { Ok(e) => { println!("matching db name {} == {}", e.name, request.path[0]); e.name == request.path[0] }, Err(_) => false} ) {
                        Some(dba) => match dba.try_lock() {
                            Ok(mut dbmg) => match &request.method[..] {
                                "CREATE_TABLE" => {
                                    println!("CREATE_TABLE");
                                    let endp = match dbmg.endpoints.iter_mut().find(|a| match a.try_lock() { Ok(a) => a.name == "create_table", Err(_) => false}) {
                                        Some(e) => Ok(Arc::clone(e)),
                                        None => Err("table creation endpoint not found".to_owned())
                                    };
                                    match endp {
                                        Ok(e) => match e.try_lock() {
                                            Ok(mut e) => {
                                                e.run(Some(&mut dbmg), request.body, None);
                                                match e.result() { Ok(e) => e, Err(e) => e }
                                            },
                                            Err(e) => e.to_string()
                                        }
                                        Err(e) => e
                                    }
                                },
                                "UPDATE_TABLE" => {
                                    println!("UPDATE_TABLE");
                                    let endp = match dbmg.endpoints.iter_mut().find(|a| match a.try_lock() { Ok(a) => a.name == "update_table", Err(_) => false}) {
                                        Some(e) => Ok(Arc::clone(e)),
                                        None => Err("table update endpoint not found".to_owned())
                                    };
                                    match endp {
                                        Ok(e) => match e.try_lock() {
                                            Ok(mut e) => {
                                                e.run(Some(&mut dbmg), request.body, None);
                                                match e.result() { Ok(e) => e, Err(e) => e }
                                            },
                                            Err(e) => e.to_string()
                                        }
                                        Err(e) => e
                                    }
                                },
                                "DELETE_TABLE" => {
                                    println!("DELETE_TABLE");
                                    let endp = match dbmg.endpoints.iter_mut().find(|a| match a.try_lock() { Ok(a) => a.name == "delete_table", Err(_) => false}) {
                                        Some(e) => Ok(Arc::clone(e)),
                                        None => Err("table delete endpoint not found".to_owned())
                                    };
                                    match endp {
                                        Ok(e) => match e.try_lock() {
                                            Ok(mut e) => {
                                                e.run(Some(&mut dbmg), request.body, None);
                                                match e.result() { Ok(e) => e, Err(e) => e }
                                            },
                                            Err(e) => e.to_string()
                                        }
                                        Err(e) => e
                                    }
                                },
                                "INDEV_TOGGLE" => {
                                    println!("INDEV_TOGGLE");
                                    let endp = match dbmg.endpoints.iter_mut().find(|a| match a.try_lock() { Ok(a) => a.name == "indev_toggle", Err(_) => false}) {
                                        Some(e) => Ok(Arc::clone(e)),
                                        None => Err("table indev toggle endpoint not found".to_owned())
                                    };
                                    match endp {
                                        Ok(e) => match e.try_lock() {
                                            Ok(mut e) => {
                                                e.run(Some(&mut dbmg), request.body, None);
                                                match e.result() { Ok(e) => e, Err(e) => e }
                                            },
                                            Err(e) => e.to_string()
                                        }
                                        Err(e) => e
                                    }
                                },
                                _ => "request method not recognised".to_owned()
                            },
                            Err(e) => "matching database could not be accessed do to multithreading blocking".to_owned()
                        },
                        None => "database not found".to_owned()
                    },
                    2 => match dbs.iter().find(|a| match a.try_lock() { Ok(e) => e.name == request.path[0], Err(_) => false} ) {
                        Some(dba) => match dba.try_lock() {
                            Ok(mut dbmg) => match &request.method[..] {
                                "CREATE_RECORD" => {
                                    println!("CREATE_RECORD {}", request.body);
                                    let endp = match dbmg.endpoints.iter_mut().find(|a| match a.try_lock() { Ok(a) => a.name == "create_record" && match a.table() { Ok(e) => match e.try_lock() { Ok(e) => e.name == request.path[1], Err(_) => false }, Err(_) => false }, Err(_) => false } ) {
                                        Some(e) => Ok(Arc::clone(e)),
                                        None => Err("record creation endpoint not found".to_owned())
                                    };
                                    match endp {
                                        Ok(e) => match e.try_lock() {
                                            Ok(mut e) => {
                                                e.run(Some(&mut dbmg), request.body, None);
                                                match e.result() { Ok(e) => e, Err(e) => e }
                                            },
                                            Err(e) => e.to_string()
                                        }
                                        Err(e) => e
                                    }
                                },
                                "READ_RECORD" => {
                                    println!("READ_RECORD");
                                    let endp = match dbmg.endpoints.iter_mut().find(|a| match a.try_lock() { Ok(a) => a.name == "read_record" && match a.table() { Ok(e) => match e.try_lock() { Ok(e) => e.name == request.path[1], Err(_) => false }, Err(_) => false }, Err(_) => false } ) {
                                        Some(e) => Ok(Arc::clone(e)),
                                        None => Err("record read endpoint not found".to_owned())
                                    };
                                    match endp {
                                        Ok(e) => match e.try_lock() {
                                            Ok(mut e) => {
                                                e.run(Some(&mut dbmg), request.body, None);
                                                match e.result() { Ok(e) => e, Err(e) => e }
                                            },
                                            Err(e) => e.to_string()
                                        }
                                        Err(e) => e
                                    }
                                },
                                "UPDATE_RECORD" => {
                                    println!("UPDATE_RECORD");
                                    let endp = match dbmg.endpoints.iter_mut().find(|a| match a.try_lock() { Ok(a) => a.name == "update_record" && match a.table() { Ok(e) => match e.try_lock() { Ok(e) => e.name == request.path[1], Err(_) => false }, Err(_) => false }, Err(_) => false } ) {
                                        Some(e) => Ok(Arc::clone(e)),
                                        None => Err("record update endpoint not found".to_owned())
                                    };
                                    match endp {
                                        Ok(e) => match e.try_lock() {
                                            Ok(mut e) => {
                                                e.run(Some(&mut dbmg), request.body, None);
                                                match e.result() { Ok(e) => e, Err(e) => e }
                                            },
                                            Err(e) => e.to_string()
                                        }
                                        Err(e) => e
                                    }
                                },
                                "DELETE_RECORD" => {
                                    println!("DELETE_RECORD");
                                    let endp = match dbmg.endpoints.iter_mut().find(|a| match a.try_lock() { Ok(a) => a.name == "delete_record" && match a.table() { Ok(e) => match e.try_lock() { Ok(e) => e.name == request.path[1], Err(_) => false }, Err(_) => false }, Err(_) => false } ) {
                                        Some(e) => Ok(Arc::clone(e)),
                                        None => Err("record deletion endpoint not found".to_owned())
                                    };
                                    match endp {
                                        Ok(e) => match e.try_lock() {
                                            Ok(mut e) => {
                                                e.run(Some(&mut dbmg), request.body, None);
                                                match e.result() { Ok(e) => e, Err(e) => e }
                                            },
                                            Err(e) => e.to_string()
                                        }
                                        Err(e) => e
                                    }
                                },
                                _ => "request method not recognised".to_owned()
                            },
                            Err(e) => "matching database could not be accessed do to multithreading blocking".to_owned()
                        },
                        None => "database not found".to_owned()
                    },
                    _ => "requires path to database".to_owned()
                }
            }, Err(e) => e.to_string()
        }
    }
}