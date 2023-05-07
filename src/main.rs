// do i actually need these extern crates?
extern crate rocksdb;
extern crate log;
extern crate simplelog;

use rocksdb::{DB};
use log::{info, warn, LevelFilter};
use simplelog::{CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};

use std::io::{Read, Write, Error as IoError};
use std::net::{TcpListener, TcpStream};
use std::fs::OpenOptions;

fn handle_client(mut stream: TcpStream, db: &DB) {
    loop {
        match read_client_command(&mut stream) {
            Ok(command) => {
                if command.is_empty() {
                    continue;
                }
                let response = process_command(&command, db);
                if let Err(e) = send_response(&mut stream, &response) {
                    warn!("Network: Request: {}", e);
                }
            }
            Err(e) => {
                warn!("Network: Response: {}", e);
                break;
            }
        }
    }
}

fn read_client_command(stream: &mut TcpStream) -> Result<Vec<String>, IoError> {
    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer)?;
    let command: Vec<String> = String::from_utf8_lossy(&buffer[..bytes_read])
        .split_whitespace()
        .map(|s| s.to_owned())
        .collect();
    Ok(command)
}

fn process_command(command: &[String], db: &DB) -> String {
    match command.get(0).map(String::as_str) {
        Some("put") if command.len() == 3 => {
            let key = command[1].as_bytes();
            let value = command[2].as_bytes();
            db.put(key, value).unwrap();
            "Success\n".to_string()
        }
        Some("get") if command.len() == 2 => {
            let key = command[1].as_bytes();
            match db.get(key).unwrap() {
                Some(value) => String::from_utf8(value.to_vec()).unwrap() + "\n",
                None => "Key not found\n".to_string(),
            }
        }
        Some("delete") if command.len() == 2 => {
            let key = command[1].as_bytes();
            db.delete(key).unwrap();
            "Success\n".to_string()
        }
        _ => "Invalid command\n".to_string(),
    }
}

fn send_response(stream: &mut TcpStream, response: &str) -> Result<(), IoError> {
    stream.write_all(response.as_bytes())
}

fn main() -> Result<(), Error> {
    let log_file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("logs/log")?;
    let write_logger = WriteLogger::new(LevelFilter::Info, Config::default(), log_file);
    let term_logger = TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed);

    if let Some(term_logger) = term_logger {
        let combined_logger = CombinedLogger::new(vec![Box::new(write_logger), Box::new(term_logger)]);
        combined_logger.init()?;
    } else {
        eprintln!("Failed to create TermLogger, logs will only be written to the log file");
        write_logger.init()?;
    }

    let path = "yadb";
    let db = DB::open_default(path).unwrap();

    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    info!("server up");

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        println!("client at: {}", stream.peer_addr().unwrap());
        handle_client(stream, &db);
    }
}
