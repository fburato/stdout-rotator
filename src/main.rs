use flate2::write::GzEncoder;
use flate2::Compression;
use log::{self, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;
use regex::Regex;
use std::fmt::Display;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use clap::Parser;

const LOGGER: &str = "rotator";

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
    #[arg(long, default_value = None)]
    log_config: Option<String>,
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

struct RotationResult {
    existing_rotated: Vec<PathBuf>,
    next_rotation: PathBuf,
}

impl RotationResult {
    fn new(existing_rotated: Vec<PathBuf>, next_rotation: PathBuf) -> RotationResult {
        RotationResult {
            existing_rotated: existing_rotated,
            next_rotation: next_rotation,
        }
    }
}

fn start_stdout_writing(rxstdout: Receiver<Vec<u8>>, txcomplete: Sender<bool>) -> JoinHandle<()> {
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
    })
}

fn start_file_writing(
    output: &str,
    rotation_directory: &str,
    rxfile: Receiver<Vec<u8>>,
    txcomplete: Sender<bool>,
) -> Result<JoinHandle<()>, RotatorError> {
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
    let result = next_file(output, rotation_directory)?;
    let gunzip: File = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(result.next_rotation)
        .map_err(|op| {
            format!(
                "Error during opening of target file '{}', {}",
                output,
                op.to_string()
            )
        })?;
    let mut compressor = GzEncoder::new(gunzip, Compression::default());
    file.set_len(0).map_err(|op| {
        format!(
            "Error during truncate of file '{}', {}",
            output,
            op.to_string()
        )
    })?;
    let handle = thread::spawn(move || {
        let mut stop: bool = false;
        while !stop {
            let read_result = rxfile.recv();
            if read_result.is_err() {
                stop = true;
                continue;
            }
            let read = read_result.unwrap();
            file.write_all(&read).unwrap();
            compressor.write_all(&read).unwrap();
            txcomplete.send(true).unwrap();
        }
        let mut finish = compressor.finish().unwrap();
        finish.flush().unwrap();
        file.flush().unwrap();
    });
    Ok(handle)
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

fn config_logger(maybe_config: &Option<String>) -> Result<(), RotatorError> {
    match maybe_config {
        None => {
            let stderr_logger = ConsoleAppender::builder()
                .target(log4rs::append::console::Target::Stderr)
                .encoder(Box::new(PatternEncoder::new(
                    "{d(%Y-%m-%dT%H:%M:%S%Z)(utc)} {l:>8} {t:>10.15} - {m}{n}",
                )))
                .build();
            let config = Config::builder()
                .appender(Appender::builder().build("console", Box::new(stderr_logger)))
                .build(
                    Root::builder()
                        .appender("console")
                        .build(LevelFilter::Debug),
                )
                .map_err(|op| {
                    format!(
                        "Error during initialisation of default console logger: {}",
                        op.to_string()
                    )
                })?;
            log4rs::init_config(config).map_err(|op| {
                format!(
                    "Error during initialising of logger configuration: {}",
                    op.to_string()
                )
            })?;
            Ok(())
        }
        Some(log_config) => {
            log4rs::init_file(log_config, Default::default()).map_err(|op| {
                format!(
                    "Error during load of logging configuration from '{}': {}",
                    log_config,
                    op.to_string()
                )
            })?;
            Ok(())
        }
    }
}

fn next_file(output_file: &str, rotation_directory: &str) -> Result<RotationResult, RotatorError> {
    let base_path = PathBuf::from(output_file);
    let parent = if rotation_directory.to_string() == "" {
        ".".to_string()
    } else {
        rotation_directory.to_string()
    };
    log::debug!(target: LOGGER, "parent={}", &parent);
    let paths = fs::read_dir(&parent).map_err(|op| {
        format!(
            "Error while listing files of '{}': {}",
            &parent,
            op.to_string()
        )
    })?;
    let mut maximum = 0;
    let base_name = base_path.file_name().unwrap().to_str().unwrap();
    let pattern = format!("^{}\\.(?<digit>[0-9]+)\\.gz$", base_name);
    let path_regex = Regex::new(&pattern).unwrap();
    let mut existing_rotated: Vec<(i32, PathBuf)> = vec![];
    log::debug!(target: LOGGER, "pattern={}", &path_regex);
    for path_result in paths {
        let path = path_result.map_err(|op| {
            format!(
                "Error while listing files of '{}': {}",
                parent,
                op.to_string()
            )
        })?;
        let file_name = path.file_name().to_str().unwrap().to_string();
        log::debug!(target: LOGGER, "file_name={}", file_name);
        if let Some(capture) = path_regex.captures(&file_name) {
            let digit = &capture["digit"];
            let parsed = digit.parse::<i32>().unwrap();
            let mut existing_buffer = PathBuf::from(rotation_directory);
            existing_buffer.push(file_name);
            existing_rotated.push((parsed, existing_buffer));
            if maximum <= parsed {
                maximum = parsed;
            }
        }
    }
    existing_rotated.sort_by(|(d1, _), (d2, _)| d1.cmp(d2));
    let mut output_path = PathBuf::from(rotation_directory);
    output_path.push(format!("{}.{}.gz", base_name, (maximum + 1)));
    let existing_rotated: Vec<PathBuf> = existing_rotated
        .iter()
        .map(move |(_, path)| path.to_owned())
        .collect();
    log::debug!(target: LOGGER, "next_file={}, existing={:?}", &output_path.display(), &existing_rotated);
    Ok(RotationResult::new(existing_rotated, output_path))
}

fn app(args: Args) -> Result<(), RotatorError> {
    config_logger(&args.log_config)?;
    log::info!(target: LOGGER, "Parsed command line arguments: {:?}", args);
    log::debug!(target: LOGGER, "Test debug");
    let (txstdout, rxstdout) = mpsc::channel::<Vec<u8>>();
    let (txfile, rxfile) = mpsc::channel::<Vec<u8>>();
    let (txcomplete1, rxcomplete) = mpsc::channel::<bool>();
    let txcomplete2 = txcomplete1.clone();
    log::info!(target: LOGGER, "Starting stdout writing");
    let stdout_handle = start_stdout_writing(rxstdout, txcomplete1);
    log::info!(target: LOGGER, "Starting file writing");
    let file_handle = start_file_writing(
        &args.output_file,
        &args.rotation_directory,
        rxfile,
        txcomplete2,
    )?;
    log::info!(target: LOGGER, "Starting stdout reading");
    start_read_cycle(txstdout, txfile, rxcomplete)?;
    stdout_handle
        .join()
        .map_err(|_| format!("Error on join of stdout"))?;
    file_handle
        .join()
        .map_err(|_| format!("Error on join of file"))?;
    Ok(())
}

fn main() {
    let args = Args::parse();
    match app(args) {
        Ok(()) => {}
        Err(err) => {
            log::error!(target: LOGGER, "{}", err.msg);
            eprintln!("{}", err.msg);
            exit(1);
        }
    }
}
