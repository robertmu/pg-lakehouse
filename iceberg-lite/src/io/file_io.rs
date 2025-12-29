use crate::error::Result;
use crate::{Error, ErrorKind};
use bytes::Bytes;
use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use url::Url;

use super::{LocalStorage, MemoryStorage};

pub struct FileMetadata {
    pub size: u64,
}

pub trait Storage: Send + Sync + std::fmt::Debug {
    fn exists(&self, path: &str) -> Result<bool>;
    fn delete(&self, path: &str) -> Result<()>;
    fn remove_dir_all(&self, path: &str) -> Result<()>;
    fn metadata(&self, path: &str) -> Result<FileMetadata>;

    fn reader(&self, path: &str) -> Result<Box<dyn FileRead>>;
    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    fn initialize(&mut self, props: HashMap<String, String>) -> Result<()>;

    fn scheme(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Debug)]
pub struct FileIO {
    storage: Arc<dyn Storage>,
}

impl FileIO {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    pub fn memory() -> Self {
        Self::new(Arc::new(MemoryStorage::new()))
    }

    pub fn local() -> Self {
        Self::new(Arc::new(LocalStorage::default()))
    }

    pub fn from_scheme_with_props(
        scheme: &str,
        props: HashMap<String, String>,
    ) -> Result<Self> {
        let mut storage: Arc<dyn Storage> = match scheme {
            "memory" => Arc::new(MemoryStorage::new()),
            "file" | "" => Arc::new(LocalStorage::default()),
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Unsupported storage scheme: {}", scheme),
                ));
            }
        };

        // Initialize storage with props if any
        if !props.is_empty() {
            if let Some(s) = Arc::get_mut(&mut storage) {
                s.initialize(props)?;
            }
        }

        Ok(Self::new(storage))
    }

    pub fn from_path(path: impl AsRef<str>) -> Result<Self> {
        Self::from_path_with_props(path, HashMap::new())
    }

    pub fn from_path_with_props(
        path: impl AsRef<str>,
        props: HashMap<String, String>,
    ) -> Result<Self> {
        let url = Url::parse(path.as_ref())
            .map_err(Error::from)
            .or_else(|e| {
                Url::from_file_path(path.as_ref()).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Input is neither a valid url nor path",
                    )
                    .with_context("input", path.as_ref().to_string())
                    .with_source(e)
                })
            })?;

        Self::from_scheme_with_props(url.scheme(), props)
    }

    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        let path_str = path.as_ref().to_string();
        let relative_path_pos = self.prefix_len(&path_str);
        Ok(InputFile {
            op: self.storage.clone(),
            path: path_str,
            relative_path_pos,
        })
    }

    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        let path_str = path.as_ref().to_string();
        let relative_path_pos = self.prefix_len(&path_str);
        Ok(OutputFile {
            op: self.storage.clone(),
            path: path_str,
            relative_path_pos,
        })
    }

    pub fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        let path_str = path.as_ref();
        let relative_path_pos = self.prefix_len(path_str);
        self.storage.delete(&path_str[relative_path_pos..])
    }

    /// Check if a file exists at the given path.
    pub fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        let path_str = path.as_ref();
        let relative_path_pos = self.prefix_len(path_str);
        self.storage.exists(&path_str[relative_path_pos..])
    }

    /// Remove a directory and all its contents recursively.
    pub fn remove_dir_all(&self, path: impl AsRef<str>) -> Result<()> {
        let path_str = path.as_ref();
        let relative_path_pos = self.prefix_len(path_str);
        self.storage.remove_dir_all(&path_str[relative_path_pos..])
    }

    /// Returns the length of URL prefix (e.g., `scheme://`) to strip from the path.
    fn prefix_len(&self, path: &str) -> usize {
        let prefix = format!("{}://", self.storage.scheme());
        if path.starts_with(&prefix) {
            prefix.len()
        } else {
            0
        }
    }

    /// Get the underlying storage implementation.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }
}

pub trait FileRead: io::Read + io::Seek + Send + Sync {
    /// Read bytes from the specified range
    fn read_range(&self, range: Range<u64>) -> Result<Bytes>;
    /// Read all bytes from the file
    fn read_all(&self) -> Result<Bytes>;
    /// Clone this reader, similar to File::try_clone().
    /// Mutations of one reader may affect all readers sharing the same underlying resource.
    fn try_clone(&self) -> io::Result<Box<dyn FileRead>>;
}

impl FileRead for Box<dyn FileRead> {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        self.as_ref().read_range(range)
    }
    fn read_all(&self) -> Result<Bytes> {
        self.as_ref().read_all()
    }
    fn try_clone(&self) -> io::Result<Box<dyn FileRead>> {
        self.as_ref().try_clone()
    }
}

/// Input file is used for reading from files.
#[derive(Debug)]
pub struct InputFile {
    op: Arc<dyn Storage>,
    path: String,
    relative_path_pos: usize,
}

impl InputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub fn exists(&self) -> crate::Result<bool> {
        self.op.exists(&self.path[self.relative_path_pos..])
    }

    pub fn metadata(&self) -> crate::Result<FileMetadata> {
        self.op.metadata(&self.path[self.relative_path_pos..])
    }

    pub fn read(&self) -> crate::Result<Bytes> {
        let path = &self.path[self.relative_path_pos..];
        let reader = self.op.reader(path)?;
        reader.read_all()
    }

    pub fn reader(&self) -> crate::Result<Box<dyn FileRead>> {
        self.op.reader(&self.path[self.relative_path_pos..])
    }
}

/// Trait for writing to files.
///
/// This trait extends `std::io::Write` to provide compatibility with
/// standard Rust I/O operations while also supporting the `close` method
/// for proper resource cleanup.
pub trait FileWrite: std::io::Write + Send + Sync {
    /// Close the file writer and flush any remaining data.
    fn close(&mut self) -> Result<()>;
}

impl FileWrite for Box<dyn FileWrite> {
    fn close(&mut self) -> Result<()> {
        self.as_mut().close()
    }
}

#[derive(Debug)]
pub struct OutputFile {
    op: Arc<dyn Storage>,
    path: String,
    relative_path_pos: usize,
}

impl OutputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub fn exists(&self) -> Result<bool> {
        self.op.exists(&self.path[self.relative_path_pos..])
    }

    pub fn delete(&self) -> Result<()> {
        self.op.delete(&self.path[self.relative_path_pos..])
    }

    pub fn to_input_file(self) -> InputFile {
        InputFile {
            op: self.op,
            path: self.path,
            relative_path_pos: self.relative_path_pos,
        }
    }

    pub fn write(&self, bs: &[u8]) -> crate::Result<()> {
        use std::io::Write;
        let mut writer = self.writer()?;
        writer.write_all(bs)?;
        writer.close()
    }

    pub fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        self.op.writer(&self.path[self.relative_path_pos..])
    }
}
