use std::fs::File;
use std::io::{self, Read, Write};
use std::error::Error;
use std::sync::mpsc;
use std::thread;

fn main() -> Result<(), Box<dyn Error>> {
    let mut buffer: Box<[u8]> = vec![0; 4096].into_boxed_slice(); 
    let mut stdin = io::stdin();
    let mut file: File = File::options().read(true).write(true).create(true).open("target/output.txt")?;
    file.set_len(0)?;
    let (txstdout, rxstdout) = mpsc::channel::<Vec<u8>>();
    let (txfile, rxfile) = mpsc::channel::<Vec<u8>>();
    let (txcomplete1, rxcomplete) = mpsc::channel::<bool>();
    let txcomplete2 = txcomplete1.clone();
    thread::spawn(move || {
        let mut stdout = io::stdout();
        let mut stop = false;
        while ! stop {
            let read_result = rxstdout.recv();
            if read_result.is_err() {
                stop = true;
                continue;
            }
            let read = read_result.unwrap();
            stdout.write(&read).unwrap();
            txcomplete1.send(true).unwrap();
        }
    });
    thread::spawn(move || {
        let mut stop: bool = false;
        while ! stop {
            let read_result = rxfile.recv();
            if read_result.is_err() {
                stop = true;
                continue;
            }
            let read = read_result.unwrap();
            file.write(&read).unwrap();
            txcomplete2.send(true).unwrap();
        }
    });
    let mut stop: bool = false;
    while ! stop {
        let read_data = stdin.read(&mut buffer)?;
        if read_data == 0 {
            stop = true;
            continue;
        }
        txstdout.send(buffer[0..read_data].to_vec())?;
        txfile.send(buffer[0..read_data].to_vec())?;
        rxcomplete.recv()?;
        rxcomplete.recv()?;
    }
    Ok(())
}
