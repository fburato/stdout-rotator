use flate2::write::GzEncoder;
use flate2::Compression;
use log::{self, debug, error, info, warn, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;
use parse_size::parse_size;
use regex::Regex;
use std::fmt::Display;
use std::fs::{self, File};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use clap::Parser;

const LOGGER: &str = "rotator";

#[derive(Parser, Debug)]
#[command(name = "stdout-rotator")]
#[command(about = "Log-rotate console output programs to specific location")]
#[command(long_about = r#"
stdout-rotator replicates its standard input to standard output and to a file and standard output, applying maximum size based log-rotation to it. It can be used to pipe the standard output of a process to a file which is automatically rotated without requiring the program to support log rotation. 
"#)]
struct Args {
    #[arg(
        long,
        default_value = "output.log",
        help = "Path to the file where the standard input is re-directed and rotated"
    )]
    output_file: String,
    #[arg(
        short,
        long,
        default_value_t = false,
        help = "Activates gunzip compression of rotated files"
    )]
    gunzip: bool,
    #[arg(long, default_value = None, help = "Directory where rotated files are saved. If not provided, the same directory of the output file will be used")]
    rotation_directory: Option<String>,
    #[arg(
        short,
        long,
        default_value_t = 5,
        help = "Maximum number of rotated files retained"
    )]
    max_history: u32,
    #[arg(long, default_value = None, help = "Configuration to log4rs logging configuration. If not provided the default logging configuration is used, using stderr")]
    log_config: Option<String>,
    #[arg(long, default_value = "50MB", value_parser = file_size, help = "Size of the output file which triggers rotation")]
    max_size: u64,
    #[arg(long, default_value_t = 4096, help = "Read buffer size")]
    buffer_size: u32,
}

fn file_size(size: &str) -> Result<u64, String> {
    parse_size(size).map_err(|op| format!("Error while parsing size: {}", op.to_string()))
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
            existing_rotated,
            next_rotation,
        }
    }
}

fn start_stdout_writing(rxstdout: Receiver<Vec<u8>>, txcomplete: Sender<bool>) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut stdout = io::stdout();
        let mut stop = false;
        let logger = "stdout_writer";
        while !stop {
            let read_result = rxstdout.recv();
            if let Err(result) = read_result {
                stop = true;
                warn!(target: logger, "Error while reading result: {}", result.to_string());
                continue;
            }
            let read = read_result.unwrap();
            if let Err(result) = stdout.write(&read) {
                stop = true;
                error!(target: logger, "Error while writing result: {}", result.to_string());
                continue;
            }
            if let Err(result) = txcomplete.send(true) {
                stop = true;
                warn!(target: logger, "Error while sending acknowledgment: {}", result.to_string());
            }
        }
    })
}

fn start_file_writing(
    max_history: u32,
    max_size: u64,
    compress: bool,
    output: &str,
    rotation_directory: Option<&str>,
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
    file.set_len(0).map_err(|op| {
        format!(
            "Error during truncate of file '{}', {}",
            output,
            op.to_string()
        )
    })?;
    let rotation_copy = rotation_directory.map(|s| s.to_string());
    let output_copy = output.to_string();
    let handle = thread::spawn(move || {
        let mut stop: bool = false;
        let logger = "file_writer";
        while !stop {
            let read_result = rxfile.recv();
            if let Err(result) = read_result {
                stop = true;
                warn!(target: logger, "Error while reading result: {}", result.to_string());
                continue;
            }
            let read = read_result.unwrap();
            let write = file.write_all(&read);
            if let Err(result) = write {
                stop = true;
                error!(target: logger, "Error while writing result to file: {}", result.to_string());
                continue;
            }
            let rotation_result = perform_rotation(
                &mut file,
                max_history,
                max_size,
                compress,
                &output_copy,
                rotation_copy.as_ref().map(|s| s.as_str()),
            );
            if let Err(result) = rotation_result {
                stop = true;
                error!(target: logger, "Error while rotating file: {}", result.to_string());
                continue;
            }
            if let Err(result) = txcomplete.send(true) {
                stop = true;
                warn!(target: logger, "Error while sending confirmation: {}", result.to_string());
            }
        }
        if let Err(result) = file.flush() {
            error!(target: "file_writer", "Error while flushing file: {}", result.to_string());
        }
    });
    Ok(handle)
}

fn perform_rotation(
    current_file: &mut File,
    max_history: u32,
    max_size: u64,
    compress: bool,
    output_file: &str,
    rotation_directory: Option<&str>,
) -> Result<(), RotatorError> {
    let current_position = current_file.stream_position().unwrap();
    if current_position <= max_size {
        return Ok(());
    }
    info!(target: LOGGER, "File size reached {} bytes, rotating", current_position);
    let rotation_result = next_file(compress, output_file, rotation_directory)?;
    if max_history == 0 {
        cleanup_rotations(max_history, &rotation_result)?;
        current_file
            .set_len(0)
            .map_err(|op| format!("Error while truncating {}: {}", output_file, op.to_string()))?;
        current_file.seek(io::SeekFrom::Start(0)).map_err(|op| {
            format!(
                "Error while seeking to beginning of {}: {}",
                output_file,
                op.to_string()
            )
        })?;
        return Ok(());
    }
    cleanup_rotations(max_history - 1, &rotation_result)?;
    current_file
        .flush()
        .map_err(|op| format!("Error while flushing {}: {}", output_file, op.to_string()))?;
    current_file.seek(io::SeekFrom::Start(0)).map_err(|op| {
        format!(
            "Error while seeking to beginning of {}: {}",
            output_file,
            op.to_string()
        )
    })?;
    let mut target: File = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&rotation_result.next_rotation)
        .map_err(|op| {
            format!(
                "Error during opening of target file '{}', {}",
                &rotation_result.next_rotation.display(),
                op.to_string()
            )
        })?;
    if compress {
        let mut compressor = GzEncoder::new(target, Compression::default());
        io::copy(current_file, &mut compressor).map_err(|op| {
            format!(
                "Error while copying {} to {} during compression: {}",
                output_file,
                &rotation_result.next_rotation.display(),
                op.to_string()
            )
        })?;
        compressor
            .finish()
            .map_err(|op| format!("Error while finishing compression: {}", op.to_string()))?
            .flush()
            .map_err(|op| format!("Error while flushing compressed file: {}", op.to_string()))?;
    } else {
        io::copy(current_file, &mut target).map_err(|op| {
            format!(
                "Error while copying {} to {}: {}",
                output_file,
                &rotation_result.next_rotation.display(),
                op.to_string()
            )
        })?;
        target
            .flush()
            .map_err(|op| format!("Error while flushing file: {}", op.to_string()))?;
    }
    current_file
        .set_len(0)
        .map_err(|op| format!("Error while truncating {}: {}", output_file, op.to_string()))?;
    current_file.seek(io::SeekFrom::Start(0)).map_err(|op| {
        format!(
            "Error while seeking to beginning of {}: {}",
            output_file,
            op.to_string()
        )
    })?;
    Ok(())
}

fn start_read_cycle(
    buffer_size: u32,
    txstdout: Sender<Vec<u8>>,
    txfile: Sender<Vec<u8>>,
    rxcomplete: Receiver<bool>,
) -> Result<(), RotatorError> {
    let mut buffer: Box<[u8]> = vec![0; buffer_size.try_into().unwrap()].into_boxed_slice();
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
                .build(Root::builder().appender("console").build(LevelFilter::Info))
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

fn next_file(
    compression: bool,
    output_file: &str,
    rotation_directory: Option<&str>,
) -> Result<RotationResult, RotatorError> {
    let base_path = PathBuf::from(output_file);
    let base_parent = base_path
        .parent()
        .unwrap()
        .to_path_buf()
        .to_str()
        .unwrap()
        .to_string();
    let base_parent = if base_parent == "" { ".".to_string() } else { base_parent };
    let parent = rotation_directory.unwrap_or(&base_parent);
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
    let pattern = if compression {
        format!("^{}\\.(?<digit>[0-9]+)\\.gz$", base_name)
    } else {
        format!("^{}\\.(?<digit>[0-9]+)$", base_name)
    };
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
            let mut existing_buffer = PathBuf::from(&parent);
            existing_buffer.push(file_name);
            existing_rotated.push((parsed, existing_buffer));
            if maximum <= parsed {
                maximum = parsed;
            }
        }
    }
    existing_rotated.sort_by(|(d1, _), (d2, _)| d1.cmp(d2));
    let mut output_path = PathBuf::from(&parent);
    let path = if compression {
        format!("{}.{}.gz", base_name, (maximum + 1))
    } else {
        format!("{}.{}", base_name, (maximum + 1))
    };
    output_path.push(path);
    let existing_rotated: Vec<PathBuf> = existing_rotated
        .iter()
        .map(|(_, path)| path.to_owned())
        .collect();
    log::debug!(target: LOGGER, "next_file={}, existing={:?}", &output_path.display(), &existing_rotated);
    Ok(RotationResult::new(existing_rotated, output_path))
}

fn cleanup_rotations(max_files: u32, rotation_result: &RotationResult) -> Result<(), RotatorError> {
    if rotation_result.existing_rotated.len() > max_files.try_into().unwrap() {
        let to_remove: u32 =
            u32::try_from(rotation_result.existing_rotated.len()).unwrap() - max_files;
        for i in 0..to_remove {
            let file_to_clean = &rotation_result.existing_rotated[usize::try_from(i).unwrap()];
            debug!(target: LOGGER, "Removing '{}'", file_to_clean.display());
            fs::remove_file(file_to_clean).map_err(|op| {
                format!(
                    "Error while removing '{}': {}",
                    file_to_clean.display(),
                    op.to_string()
                )
            })?;
        }
    }
    Ok(())
}

fn app(args: Args) -> Result<(), RotatorError> {
    config_logger(&args.log_config)?;
    log::info!(target: LOGGER, "Parsed command line arguments: {:?}", args);
    log::debug!(target: LOGGER, "Cleaning up rotations");
    let rotation_result = next_file(
        args.gunzip,
        &args.output_file,
        args.rotation_directory.as_ref().map(|s| s.as_str()),
    )?;
    cleanup_rotations(args.max_history, &rotation_result)?;
    let (txstdout, rxstdout) = mpsc::channel::<Vec<u8>>();
    let (txfile, rxfile) = mpsc::channel::<Vec<u8>>();
    let (txcomplete1, rxcomplete) = mpsc::channel::<bool>();
    let txcomplete2 = txcomplete1.clone();
    log::info!(target: LOGGER, "Starting stdout writing");
    let stdout_handle = start_stdout_writing(rxstdout, txcomplete1);
    log::info!(target: LOGGER, "Starting file writing");
    let file_handle = start_file_writing(
        args.max_history,
        args.max_size,
        args.gunzip,
        &args.output_file,
        args.rotation_directory.as_ref().map(|s| s.as_str()),
        rxfile,
        txcomplete2,
    )?;
    log::info!(target: LOGGER, "Starting stdout reading");
    start_read_cycle(args.buffer_size, txstdout, txfile, rxcomplete)?;
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
