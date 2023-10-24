use std::fmt::Display;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "stdout-rotator")]
#[command(about = "Allows to apply log-rotate to console output programs")]
struct Args {
    #[arg(long, default_value = "output.log")]
    output_file: String,
    #[arg(long, default_value = ".")]
    rotation_directory: String,
    #[arg(short, long, default_value_t = false)]
    gunzip: bool,
    #[arg(short, long, default_value_t = 5)]
    max_history: u32,
}

#[derive(Debug)]
struct RotatorError {
    msg: String,
}

impl RotatorError {
    fn new(msg: &str) -> RotatorError {
        RotatorError {
            msg: msg.to_string(),
        }
    }
}

impl Display for RotatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl From<String> for RotatorError {
    fn from(value: String) -> Self {
        RotatorError::new(&value)
    }
}

fn start_stdout_writing(rxstdout: Receiver<Vec<u8>>, txcomplete: Sender<bool>) {
    thread::spawn(move || {
        let mut stdout = io::stdout();
        let mut stop = false;
        while !stop {
            let read_result = rxstdout.recv();
            if read_result.is_err() {
                stop = true;
                continue;
            }
            let read = read_result.unwrap();
            stdout.write(&read).unwrap();
            txcomplete.send(true).unwrap();
        }
    });
}

fn start_file_writing(
    output: &str,
    rxfile: Receiver<Vec<u8>>,
    txcomplete: Sender<bool>,
) -> Result<(), RotatorError> {
    if let Some(parent) = Path::new(output).parent() {
        fs::create_dir_all(parent).map_err(|op| {
            format!(
                "Failure during creation of parent directory of '{}': {}",
                output,
                op.to_string()
            )
        })?;
    }

    let mut file: File = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(output)
        .map_err(|op| {
            format!(
                "Error during opening of target file '{}', {}",
                output,
                op.to_string()
            )
        })?;
    file.set_len(0).map_err(|op| {
        format!(
            "Error during truncate of file '{}', {}",
            output,
            op.to_string()
        )
    })?;
    thread::spawn(move || {
        let mut stop: bool = false;
        while !stop {
            let read_result = rxfile.recv();
            if read_result.is_err() {
                stop = true;
                continue;
            }
            let read = read_result.unwrap();
            file.write(&read).unwrap();
            txcomplete.send(true).unwrap();
        }
    });
    Ok(())
}

fn start_read_cycle(
    txstdout: Sender<Vec<u8>>,
    txfile: Sender<Vec<u8>>,
    rxcomplete: Receiver<bool>,
) -> Result<(), RotatorError> {
    let mut buffer: Box<[u8]> = vec![0; 4096].into_boxed_slice();
    let mut stdin = io::stdin();
    let mut stop: bool = false;
    while !stop {
        let read_data = stdin
            .read(&mut buffer)
            .map_err(|op| format!("Impossible to read from stdin: {}", op.to_string()))?;
        if read_data == 0 {
            stop = true;
            continue;
        }
        txstdout.send(buffer[0..read_data].to_vec()).map_err(|op| {
            format!(
                "Error while sending last chunk to stdout: {}",
                op.to_string()
            )
        })?;
        txfile
            .send(buffer[0..read_data].to_vec())
            .map_err(|op| format!("Error while sending last chunk to file: {}", op.to_string()))?;
        rxcomplete.recv().map_err(|op| {
            format!(
                "Error while receiving first confirmation from thread: {}",
                op.to_string()
            )
        })?;
        rxcomplete.recv().map_err(|op| {
            format!(
                "Error while receiving second confirmation from thread: {}",
                op.to_string()
            )
        })?;
    }
    Ok(())
}

fn main() -> Result<(), RotatorError> {
    let args = Args::parse();
    let (txstdout, rxstdout) = mpsc::channel::<Vec<u8>>();
    let (txfile, rxfile) = mpsc::channel::<Vec<u8>>();
    let (txcomplete1, rxcomplete) = mpsc::channel::<bool>();
    let txcomplete2 = txcomplete1.clone();
    start_stdout_writing(rxstdout, txcomplete1);
    start_file_writing(&args.output_file, rxfile, txcomplete2)?;
    start_read_cycle(txstdout, txfile, rxcomplete)?;
    Ok(())
}
