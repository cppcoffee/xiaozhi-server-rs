/// Runtime log: writes to stdout and to size-rotated files
use std::borrow::Cow;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::config::LogCfg;

const FILE_NAME: &str = "xiaozhi.log";

pub fn ascii(text: &str) -> Cow<'_, str> {
    if text.is_ascii() {
        return Cow::Borrowed(text);
    }
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_ascii() {
            escaped.push(ch);
        } else {
            for unit in ch.encode_utf16(&mut [0; 2]) {
                write!(escaped, "\\u{unit:04x}").expect("writing to a String cannot fail");
            }
        }
    }
    Cow::Owned(escaped)
}

struct AsciiWriter<W>(W);

impl<'a, W: MakeWriter<'a>> MakeWriter<'a> for AsciiWriter<W> {
    type Writer = AsciiWriter<W::Writer>;

    fn make_writer(&'a self) -> Self::Writer {
        AsciiWriter(self.0.make_writer())
    }
}

impl<W: Write> Write for AsciiWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_ascii() {
            self.0.write_all(buf)?;
        } else {
            // The tracing formatter writes each formatted UTF-8 event as one buffer.
            self.0
                .write_all(ascii(&String::from_utf8_lossy(buf)).as_bytes())?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

/// Init logging: level from RUST_LOG or config, files under log.dir (default ./logs)
pub fn init(cfg: &LogCfg) -> Option<WorkerGuard> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cfg.level));
    let stdout = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(AsciiWriter(io::stdout));
    let max_bytes = cfg.max_mb.saturating_mul(1024 * 1024);
    let mut guard = None;
    let files = match RotatingFile::new(&cfg.dir, cfg.keep, max_bytes) {
        Ok(w) => {
            let (non_blocking, worker_guard) = tracing_appender::non_blocking(w);
            guard = Some(worker_guard);
            Some(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(AsciiWriter(non_blocking)),
            )
        }
        Err(e) => {
            let message = format!(
                "Log directory unavailable ({}): {e}. Using stdout only.",
                cfg.dir
            );
            eprintln!("{}", ascii(&message));
            None
        }
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(stdout)
        .with(files)
        .init();
    guard
}

/// Past max_bytes rotate current -> .1, .1 -> .2 ..., keeping only `keep` files
pub struct RotatingFile {
    path: PathBuf,
    max_bytes: u64,
    keep: usize,
    state: Mutex<FileState>,
}

#[derive(Default)]
struct FileState {
    file: Option<File>,
    size: u64,
}

impl RotatingFile {
    pub fn new(dir: &str, keep: usize, max_bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            path: Path::new(dir).join(FILE_NAME),
            max_bytes: max_bytes.max(1),
            keep: keep.max(1),
            state: Mutex::new(FileState::default()),
        })
    }

    fn write_bytes(&self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.open(&mut state)?;
        if state.size + buf.len() as u64 > self.max_bytes {
            self.rotate(&mut state)?;
        }
        if let Some(file) = state.file.as_mut() {
            file.write_all(buf)?;
            state.size += buf.len() as u64;
        }
        Ok(buf.len())
    }

    fn open(&self, state: &mut FileState) -> io::Result<()> {
        if state.file.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
            state.size = file.metadata()?.len();
            state.file = Some(file);
        }
        Ok(())
    }

    fn rotate(&self, state: &mut FileState) -> io::Result<()> {
        state.file = None;
        if self.keep > 1 {
            let oldest = path_with_index(&self.path, self.keep - 1);
            let _ = fs::remove_file(oldest);
            for i in (1..self.keep - 1).rev() {
                let _ = fs::rename(
                    path_with_index(&self.path, i),
                    path_with_index(&self.path, i + 1),
                );
            }
            fs::rename(&self.path, path_with_index(&self.path, 1))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        state.size = 0;
        state.file = Some(file);
        Ok(())
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_bytes(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match state.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

fn path_with_index(path: &Path, index: usize) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn formatted_events_escape_non_ascii_payloads() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().write(bytes)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(AsciiWriter(move || Capture(captured.clone())))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(device = "室🦌", "Transcript: hi 你好");
        });
        let bytes = output.lock().unwrap();
        assert!(bytes.is_ascii());
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains(r"Transcript: hi \u4f60\u597d"));
        assert!(text.contains(r"\u5ba4\ud83e\udd8c"));
    }

    #[test]
    fn rotates_and_keeps_limit() {
        let dir = std::env::temp_dir().join(format!("xiaozhi-log-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let writer = RotatingFile::new(dir.to_str().unwrap(), 2, 100).unwrap();
        for _ in 0..10 {
            writer.write_bytes(&[b'x'; 40]).unwrap();
        }
        let files: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(files.len(), 2, "{files:?}");
        for name in &files {
            let size = fs::metadata(dir.join(name)).unwrap().len();
            assert!(size <= 100, "{name} = {size}");
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
