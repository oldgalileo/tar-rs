use crate::{EntryType, Header};
use std::{convert::TryFrom, fs, io};

/// TODO
pub struct Metadata {
    /// TODO
    pub size: u64,
    /// TODO
    pub uid: u64,
    /// TODO
    pub gid: u64,
    /// TODO
    pub mode: u32,
    /// TODO
    pub mtime: u64,
    /// TODO
    pub entry_type: EntryType,
}

impl Metadata {}

impl TryFrom<&Header> for Metadata {
    type Error = io::Error;

    fn try_from(header: &Header) -> Result<Self, Self::Error> {
        Ok(Self {
            size: header.size()?,
            uid: header.uid()?,
            gid: header.gid()?,
            mode: header.mode()?,
            mtime: header.mtime()?,
            entry_type: header.entry_type(),
        })
    }
}

impl TryFrom<Header> for Metadata {
    type Error = io::Error;

    fn try_from(header: Header) -> Result<Self, Self::Error> {
        TryFrom::try_from(&header)
    }
}

impl From<&fs::Metadata> for Metadata {
    #[cfg(windows)]
    fn from(stat: fs::Metadata) -> Self {
        use std::os::windows::prelude::*;

        // The dates listed in tarballs are always seconds relative to
        // January 1, 1970. On Windows, however, the timestamps are returned as
        // dates relative to January 1, 1601 (in 100ns intervals), so we need to
        // add in some offset for those dates.
        let mtime = (stat.last_write_time() / (1_000_000_000 / 100)) - 11644473600;
        let mode = {
            const FILE_ATTRIBUTE_READONLY: u32 = 0x00000001;
            let readonly = stat.file_attributes() & FILE_ATTRIBUTE_READONLY;
            match (stat.is_dir(), readonly != 0) {
                (true, false) => 0o755,
                (true, true) => 0o555,
                (false, false) => 0o644,
                (false, true) => 0o444,
            }
        };

        let ft = stat.file_type();
        let entry_type = if ft.is_dir() {
            EntryType::dir()
        } else if ft.is_file() {
            EntryType::file()
        } else if ft.is_symlink() {
            EntryType::symlink()
        } else {
            EntryType::new(b' ')
        };

        Self {
            size: if stat.is_dir() || stat.file_type().is_symlink() {
                0
            } else {
                stat.len()
            },
            uid: 0,
            gid: 0,
            mode,
            mtime,
            entry_type,
        }
    }

    #[cfg(unix)]
    fn from(stat: &fs::Metadata) -> Self {
        use std::os::unix::prelude::*;

        Self {
            size: if stat.is_dir() || stat.file_type().is_symlink() {
                0
            } else {
                stat.len()
            },
            uid: stat.uid() as u64,
            gid: stat.gid() as u64,
            mode: stat.mode(),
            mtime: stat.mtime() as u64,
            entry_type: mode_to_entry_type(stat.mode()),
        }
    }
}

impl From<fs::Metadata> for Metadata {
    #[cfg(windows)]
    fn from(stat: fs::Metadata) -> Self {
        From::from(&stat)
    }

    #[cfg(unix)]
    fn from(stat: fs::Metadata) -> Self {
        From::from(&stat)
    }
}

#[cfg(unix)]
fn mode_to_entry_type(mode: u32) -> EntryType {
    match mode as libc::mode_t & libc::S_IFMT {
        libc::S_IFREG => EntryType::file(),
        libc::S_IFLNK => EntryType::symlink(),
        libc::S_IFCHR => EntryType::character_special(),
        libc::S_IFBLK => EntryType::block_special(),
        libc::S_IFDIR => EntryType::dir(),
        libc::S_IFIFO => EntryType::fifo(),
        _ => EntryType::new(b' '),
    }
}
