use std::io::{self, Read, Write};
use std::net::TcpStream;

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:8000").unwrap();
    let mut buffer = [0; 1024];

    loop {
        let mut command = String::new();
        io::stdin().read_line(&mut command).unwrap();
        stream.write_all(command.as_bytes()).unwrap();

        let bytes_read = stream.read(&mut buffer).unwrap();
        // https://stackoverflow.com/questions/19076719/how-do-i-convert-a-vector-of-bytes-u8-to-a-string
        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        print!("{}", response);
    }
}
