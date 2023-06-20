extern crate log;
extern crate simplelog;
extern crate sled;

use log::{info, warn, LevelFilter};
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};
use sled::Tree;

use std::fs::File;
use std::io::{Error as IoError, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;


struct LSMTree {
    tree: Tree,
}

impl LSMTree {
    fn new(tree: Tree) -> Self {
        LSMTree { tree }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> sled::Result<()> {
        self.tree.insert(key, value)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> sled::Result<Option<sled::IVec>> {
        self.tree.get(key)
    }

    fn delete(&self, key: &[u8]) -> sled::Result<()> {
        self.tree.remove(key)?;
        Ok(())
    }
}

fn handle_client(mut stream: TcpStream, storage: &LSMTree) {
    loop {
        match read_client_command(&mut stream) {
            Ok(command) => {
                if command.is_empty() {
                    continue;
                }
                let response = process_command(&command, storage);
                if let Err(e) = send_response(&mut stream, &response) {
                    warn!("Network: Response: {}", e);
                }
            }
            Err(e) => {
                warn!("Network: Request: {}", e);
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

fn process_command(command: &[String], storage: &LSMTree) -> String {
    match command.get(0).map(String::as_str) {
        Some("put") if command.len() == 3 => {
            let key = command[1].as_bytes();
            let value = command[2].as_bytes();
            storage.put(key, value).unwrap();
            "Success\n".to_string()
        }
        Some("get") if command.len() == 2 => {
            let key = command[1].as_bytes();
            match storage.get(key).unwrap() {
                Some(value) => String::from_utf8(value.to_vec()).unwrap() + "\n",
                None => "Key not found\n".to_string(),
            }
        }
        Some("delete") if command.len() == 2 => {
            let key = command[1].as_bytes();
            storage.delete(key).unwrap();
            "Success\n".to_string()
        }
        _ => "Invalid command\n".to_string(),
    }
}

fn send_response(stream: &mut TcpStream, response: &str) -> Result<(), IoError> {
    stream.write_all(response.as_bytes())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log_path = Path::new("./logs/log");
    let log_file = File::create(log_path).expect("Failed to create log file");

    let term_logger = TermLogger::new(
        LevelFilter::Warn,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    );

    let write_logger = WriteLogger::new(LevelFilter::Info, Config::default(), log_file);

    CombinedLogger::init(vec![term_logger, write_logger])
        .expect("Failed to initialize combined logger");

    let db = sled::open("yadb").unwrap();
    let tree = db.open_tree("default").unwrap();
    let storage = LSMTree::new(tree);

    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    info!("server up");

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        info!("client at: {}", stream.peer_addr().unwrap());
        handle_client(stream, &storage);
    }

    Ok(())
}
