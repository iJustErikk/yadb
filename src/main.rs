
use log::{info, warn, LevelFilter};
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};

use std::fs::File;
use std::io::{Error as IoError, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;

pub mod storage;
use storage::Tree;

async fn handle_client(mut stream: TcpStream, storage: &mut Tree) {
    loop {
        match read_client_command(&mut stream) {
            Ok(command) => {
                if command.is_empty() {
                    continue;
                }
                let response = process_command(&command, storage).await;
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

async fn process_command(command: &[String], storage: &mut Tree) -> String {
    // TODO: revisit this to handle errors better
    match command.get(0).map(String::as_str) {
        Some("put") if command.len() == 3 => {
            let key = command[1].as_bytes().to_vec();
            let value = command[2].as_bytes().to_vec();
            storage.put(&key, &value).await.unwrap().unwrap();
            "Success\n".to_string()
        }
        Some("get") if command.len() == 2 => {
            let key = command[1].as_bytes().to_vec();
            match storage.get(&key).await.unwrap() {
                Ok(Some(value)) => String::from_utf8(value.to_vec()).unwrap() + "\n",
                Ok(None) => "Key not found\n".to_string(),
                Err(_) => "Get failed.".to_string()
            }
        }
        Some("delete") if command.len() == 2 => {
            let key = command[1].as_bytes().to_vec();
            storage.delete(&key).await.unwrap().unwrap();
            "Success\n".to_string()
        }
        _ => "Invalid command\n".to_string(),
    }
}

fn send_response(stream: &mut TcpStream, response: &str) -> Result<(), IoError> {
    stream.write_all(response.as_bytes())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let mut db = Tree::new("yadb");
    // pretty innit
    db.init().await?;

    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    info!("server up");

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        info!("client at: {}", stream.peer_addr().unwrap());
        handle_client(stream, &mut db).await;
    }

    Ok(())
}
