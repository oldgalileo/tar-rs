use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::str;

use crate::header::{path2bytes, HeaderMode};
use crate::meta::Metadata;
use crate::{other, EntryType, Header};

#[derive(Eq, Hash, PartialEq)]
/// TODO
pub struct LinkInfo {
    /// TODO
    pub device: u64,
    /// TODO
    pub inode: u64,
}

/// A structure for building archives
///
/// This structure has methods for building up an archive from scratch into any
/// arbitrary writer.
pub struct Builder<W: Write> {
    mode: HeaderMode,
    follow: bool,
    finished: bool,
    inode_map: HashMap<LinkInfo, OsString>,
    obj: Option<W>,
}

impl<W: Write> Builder<W> {
    /// Create a new archive builder with the underlying object as the
    /// destination of all data written. The builder will use
    /// `HeaderMode::Complete` by default.
    pub fn new(obj: W) -> Builder<W> {
        Builder {
            mode: HeaderMode::Complete,
            follow: true,
            finished: false,
            inode_map: HashMap::new(),
            obj: Some(obj),
        }
    }

    /// Changes the HeaderMode that will be used when reading fs Metadata for
    /// methods that implicitly read metadata for an input Path. Notably, this
    /// does _not_ apply to `append(Header)`.
    pub fn mode(&mut self, mode: HeaderMode) {
        self.mode = mode;
    }

    /// Follow symlinks, archiving the contents of the file they point to rather
    /// than adding a symlink to the archive. Defaults to true.
    pub fn follow_symlinks(&mut self, follow: bool) {
        self.follow = follow;
    }

    /// Gets shared reference to the underlying object.
    pub fn get_ref(&self) -> &W {
        self.obj.as_ref().unwrap()
    }

    /// Gets mutable reference to the underlying object.
    ///
    /// Note that care must be taken while writing to the underlying
    /// object. But, e.g. `get_mut().flush()` is claimed to be safe and
    /// useful in the situations when one needs to be ensured that
    /// tar entry was flushed to the disk.
    pub fn get_mut(&mut self) -> &mut W {
        self.obj.as_mut().unwrap()
    }

    /// Unwrap this archive, returning the underlying object.
    ///
    /// This function will finish writing the archive if the `finish` function
    /// hasn't yet been called, returning any I/O error which happens during
    /// that operation.
    pub fn into_inner(mut self) -> io::Result<W> {
        if !self.finished {
            self.finish()?;
        }
        Ok(self.obj.take().unwrap())
    }

    /// Adds a new entry to this archive.
    ///
    /// This function will append the header specified, followed by contents of
    /// the stream specified by `data`. To produce a valid archive the `size`
    /// field of `header` must be the same as the length of the stream that's
    /// being written. Additionally the checksum for the header should have been
    /// set via the `set_cksum` method.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all entries have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Errors
    ///
    /// This function will return an error for any intermittent I/O error which
    /// occurs when either reading or writing.
    ///
    /// # Examples
    ///
    /// ```
    /// use tar::{Builder, Header};
    ///
    /// let mut header = Header::new_gnu();
    /// header.set_path("foo").unwrap();
    /// header.set_size(4);
    /// header.set_cksum();
    ///
    /// let mut data: &[u8] = &[1, 2, 3, 4];
    ///
    /// let mut ar = Builder::new(Vec::new());
    /// ar.append(&header, data).unwrap();
    /// let data = ar.into_inner().unwrap();
    /// ```
    pub fn append<R: Read>(&mut self, header: &Header, mut data: R) -> io::Result<()> {
        append(self.get_mut(), header, &mut data)
    }

    /// Adds a new entry to this archive with the specified path.
    ///
    /// This function will set the specified path in the given header, which may
    /// require appending a GNU long-name extension entry to the archive first.
    /// The checksum for the header will be automatically updated via the
    /// `set_cksum` method after setting the path. No other metadata in the
    /// header will be modified.
    ///
    /// Then it will append the header, followed by contents of the stream
    /// specified by `data`. To produce a valid archive the `size` field of
    /// `header` must be the same as the length of the stream that's being
    /// written.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all entries have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Errors
    ///
    /// This function will return an error for any intermittent I/O error which
    /// occurs when either reading or writing.
    ///
    /// # Examples
    ///
    /// ```
    /// use tar::{Builder, Header};
    ///
    /// let mut header = Header::new_gnu();
    /// header.set_size(4);
    /// header.set_cksum();
    ///
    /// let mut data: &[u8] = &[1, 2, 3, 4];
    ///
    /// let mut ar = Builder::new(Vec::new());
    /// ar.append_data(&mut header, data, "really/long/path/to/foo").unwrap();
    /// let data = ar.into_inner().unwrap();
    /// ```
    pub fn append_data<P: AsRef<Path>, R: Read>(
        &mut self,
        header: &mut Header,
        data: R,
        dest: P,
    ) -> io::Result<()> {
        prepare_header_path(self.get_mut(), header, dest.as_ref())?;
        header.set_cksum();
        self.append(&header, data)
    }

    /// TODO: Document
    ///
    /// This shouldn't be used to add hardlinks, only symlinks
    pub fn append_link<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        path: P,
        name: Q,
    ) -> io::Result<()> {
        let stat = fs::symlink_metadata(&path)?;
        let target = fs::read_link(&path)?;
        self.append_link_with_metadata(target, name, &stat.into())
    }

    /// TODO: Document
    pub fn append_link_with_metadata<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        target: P,
        name: Q,
        meta: &Metadata,
    ) -> io::Result<()> {
        let mut header = Header::new_gnu();
        header.set_metadata_in_mode(meta, self.mode);
        self.append_link_with_header(target, name, &mut header)
    }

    /// TODO: Document
    pub fn append_link_with_header<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        target: P,
        name: Q,
        header: &mut Header,
    ) -> io::Result<()> {
        prepare_header_path(self.get_mut(), header, name.as_ref())?;
        prepare_header_link(
            self.get_mut(),
            header,
            target.as_ref(),
            EntryType::GNULongLink,
        )?;
        header.set_cksum();
        self.append(&header, std::io::empty())
    }

    /// Adds a file on the local filesystem to this archive.
    ///
    /// This function will open the file specified by `path` and insert the file
    /// into the archive with the appropriate metadata set, returning any I/O
    /// error which occurs while writing. The path name for the file inside of
    /// this archive will be the same as `path`, and it is required that the
    /// path is a relative path.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// ar.append_path("foo/bar.txt").unwrap();
    /// ```
    pub fn append_path<P: AsRef<Path>>(&mut self, path: P) -> io::Result<()> {
        self.append_path_with_name(&path, &path)
    }

    /// Adds a file on the local filesystem to this archive under another name.
    ///
    /// This function will open the file specified by `path` and insert the file
    /// into the archive as `name` with appropriate metadata set, returning any
    /// I/O error which occurs while writing. The path name for the file inside
    /// of this archive will be `name` is required to be a relative path.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Note if the `path` is a directory. This will just add an entry to the archive,
    /// rather than contents of the directory.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// // Insert the local file "foo/bar.txt" in the archive but with the name
    /// // "bar/foo.txt".
    /// ar.append_path_with_name("foo/bar.txt", "bar/foo.txt").unwrap();
    /// ```
    pub fn append_path_with_name<P: AsRef<Path>, N: AsRef<Path>>(
        &mut self,
        path: P,
        name: N,
    ) -> io::Result<()> {
        let mode = self.mode.clone();
        let follow = self.follow;

        let stat = if follow {
            fs::metadata(&path).map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "{} when getting metadata for {}",
                        err,
                        path.as_ref().display()
                    ),
                )
            })?
        } else {
            fs::symlink_metadata(&path).map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "{} when getting metadata for {}",
                        err,
                        path.as_ref().display()
                    ),
                )
            })?
        };
        if stat.is_file() {
            self.append_file(path, name)
        } else if stat.is_dir() {
            self.append_directory(path, name)
        } else if stat.is_symlink() {
            self.append_link(path, name)
        } else {
            #[cfg(unix)]
            {
                append_special(self.get_mut(), name, &stat, mode)
            }
            #[cfg(not(unix))]
            {
                Err(other(&format!("{} has unknown file type", path.display())))
            }
        }
    }

    /// Adds a file to this archive with the given path as the name of the file
    /// in the archive.
    ///
    /// This will use the metadata of `file` to populate a `Header`, and it will
    /// then append the file to the archive with the name `path`.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::File;
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// // Open the file at one location, but insert it into the archive with a
    /// // different name.
    /// ar.append_file("foo/bar/baz.txt", "bar/baz.txt").unwrap();
    /// ```
    pub fn append_file<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        src: P,
        dest: Q,
    ) -> io::Result<()> {
        let mode = self.mode.clone();
        let stat = fs::metadata(src.as_ref())?;
        #[cfg(unix)]
        {
            if !stat.is_file() {
                return append_special(self.get_mut(), dest, &stat, mode);
            }
        }
        if let Some(link_name) = self.check_for_hard_link(dest.as_ref(), &stat) {
            tracing::debug!(
                "got a hardlink! `{:?}` to `{:?}` with stat `{:?}`",
                src.as_ref(),
                link_name,
                stat
            );
            let mut header = Header::new_gnu();
            prepare_header_path(self.get_mut(), &mut header, dest.as_ref())?;
            header.set_metadata_in_mode(&stat.into(), self.mode);
            header.set_entry_type(EntryType::Link);

            tracing::debug!(
                "about to prepare header link with `{:?}` (starting from: `{:?}`",
                link_name,
                header
            );

            prepare_header_link(
                self.get_mut(),
                &mut header,
                link_name.as_ref(),
                EntryType::Link,
            )?;

            tracing::debug!(
                "finishing preparing header link w `{:?}` (type: `{:?}`) (finishing at: `{:?}`)",
                link_name,
                header,
                header.entry_type()
            );
            header.set_size(0);
            header.set_cksum();

            tracing::debug!("ading hardlink with header `{:?}`", header);
            tracing::debug!("-------");
            return self.append(&header, &mut io::empty());
        }

        self.append_data_with_metadata(&dest, &stat.into(), &mut fs::File::open(src)?)
    }

    /// TODO
    pub fn append_file_with_metadata<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        src: P,
        dest: Q,
        meta: &Metadata,
    ) -> io::Result<()> {
        let mode = self.mode.clone();
        let stat = std::fs::metadata(&src)?;
        #[cfg(unix)]
        {
            if !stat.is_file() {
                return append_special(self.get_mut(), dest, &stat, mode);
            }
        }
        if let Some(link_name) = self.check_for_hard_link(dest.as_ref(), &stat) {
            let mut header = Header::new_gnu();
            prepare_header_path(self.get_mut(), &mut header, dest.as_ref())?;
            header.set_metadata_in_mode(meta, self.mode);
            header.set_entry_type(EntryType::Link);

            tracing::debug!(
                "about to prepare header link with `{:?}` (starting from: `{:?}`",
                link_name,
                header
            );

            prepare_header_link(
                self.get_mut(),
                &mut header,
                link_name.as_ref(),
                EntryType::Link,
            )?;

            tracing::debug!(
                "finishing preparing header link w `{:?}` (type: `{:?}`) (finishing at: `{:?}`)",
                link_name,
                header,
                header.entry_type()
            );
            header.set_size(0);
            header.set_cksum();

            tracing::debug!("ading hardlink with header `{:?}`", header);
            tracing::debug!("-------");
            return self.append(&header, &mut io::empty());
        }

        let meta = Metadata {
            size: stat.len(),
            uid: meta.uid,
            gid: meta.gid,
            mode: meta.mode,
            mtime: meta.mtime,
            entry_type: Metadata::from(stat).entry_type,
        };
        self.append_data_with_metadata(&dest, &meta, &mut fs::File::open(src)?)
    }

    // Windows does not support using inode to check for hard link
    #[cfg(windows)]
    fn check_for_hard_link(&mut self, _: &Path, _: &fs::Metadata) -> Option<&Path> {
        None
    }

    /// TODO: Dcument
    #[cfg(any(unix, target_os = "redox"))]
    fn check_for_hard_link(&mut self, path: &Path, meta: &fs::Metadata) -> Option<PathBuf> {
        use std::{collections::hash_map::Entry, os::unix::prelude::MetadataExt};

        if meta.file_type().is_dir() || meta.nlink() <= 1 {
            return None;
        }
        let hl_info = LinkInfo {
            device: meta.dev(),
            inode: meta.ino(),
        };

        match self.inode_map.entry(hl_info) {
            Entry::Occupied(o) => {
                let name: &OsString = o.into_mut();
                tracing::debug!("NONONO hit existing hardlink! `{:?}` to `{:?}`", path, name);
                Some(PathBuf::from(name))
            }
            Entry::Vacant(v) => {
                let name = path.as_os_str().to_owned();
                tracing::debug!(
                    "YESYESYES found a new hardlink! `{:?}` to `{:?}`",
                    path,
                    name
                );
                v.insert(name);
                tracing::debug!("-------");
                None
            }
        }
    }

    /// TODO: Document
    pub fn append_directory<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        src: P,
        dest: Q,
    ) -> io::Result<()> {
        let stat = fs::metadata(src.as_ref())?;
        self.append_directory_with_metadata(dest, &stat.into())
    }

    /// TODO: Document
    pub fn append_directory_with_metadata<P: AsRef<Path>>(
        &mut self,
        dest: P,
        meta: &Metadata,
    ) -> io::Result<()> {
        self.append_data_with_metadata(dest, meta, &mut io::empty())
    }

    /// TODO: Document
    pub fn append_data_with_metadata<P: AsRef<Path>>(
        &mut self,
        dest: P,
        meta: &Metadata,
        reader: &mut dyn Read,
    ) -> io::Result<()> {
        let mut header = Header::new_gnu();
        prepare_header_path(self.get_mut(), &mut header, dest.as_ref())?;
        header.set_metadata_in_mode(&meta, self.mode);
        header.set_cksum();
        self.append(&mut header, reader)
    }

    /// Adds a directory and all of its contents (recursively) to this archive
    /// with the given path as the name of the directory in the archive.
    ///
    /// Note that this will not attempt to seek the archive to a valid position,
    /// so if the archive is in the middle of a read or some other similar
    /// operation then this may corrupt the archive.
    ///
    /// Also note that after all files have been written to an archive the
    /// `finish` function needs to be called to finish writing the archive.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs;
    /// use tar::Builder;
    ///
    /// let mut ar = Builder::new(Vec::new());
    ///
    /// // Use the directory at one location, but insert it into the archive
    /// // with a different name.
    /// ar.append_dir_all(".", "bardir").unwrap();
    /// ```
    pub fn append_dir_all<P, Q>(&mut self, path: P, src_path: Q) -> io::Result<()>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let mut stack = vec![(src_path.as_ref().to_path_buf(), true, false)];
        while let Some((src, is_dir, is_symlink)) = stack.pop() {
            let dest = path.as_ref().join(src.strip_prefix(&src_path).unwrap());

            if is_dir || (is_symlink && self.follow && src.is_dir()) {
                for entry in fs::read_dir(&src)? {
                    let entry = entry?;
                    let file_type = entry.file_type()?;
                    stack.push((entry.path(), file_type.is_dir(), file_type.is_symlink()));
                }
                if dest != Path::new("") {
                    self.append_directory(&src, &dest)?;
                }
            } else if !self.follow && is_symlink {
                self.append_link(&src, &dest)?;
            } else {
                self.append_file(&src, &dest)?;
            }
        }
        Ok(())
    }

    /// Finish writing this archive, emitting the termination sections.
    ///
    /// This function should only be called when the archive has been written
    /// entirely and if an I/O error happens the underlying object still needs
    /// to be acquired.
    ///
    /// In most situations the `into_inner` method should be preferred.
    pub fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.get_mut().write_all(&[0; 1024])
    }
}

fn append(mut dst: &mut dyn Write, header: &Header, mut data: &mut dyn Read) -> io::Result<()> {
    dst.write_all(header.as_bytes())?;
    let len = io::copy(&mut data, &mut dst)?;

    // Pad with zeros if necessary.
    let buf = [0; 512];
    let remaining = 512 - (len % 512);
    if remaining < 512 {
        dst.write_all(&buf[..remaining as usize])?;
    }

    Ok(())
}

#[cfg(unix)]
fn append_special<P: AsRef<Path>>(
    dst: &mut dyn Write,
    name: P,
    stat: &fs::Metadata,
    mode: HeaderMode,
) -> io::Result<()> {
    use ::std::os::unix::fs::{FileTypeExt, MetadataExt};

    let file_type = stat.file_type();
    let entry_type;
    if file_type.is_socket() {
        // sockets can't be archived
        return Err(other(&format!(
            "{}: socket can not be archived",
            name.as_ref().display()
        )));
    } else if file_type.is_fifo() {
        entry_type = EntryType::Fifo;
    } else if file_type.is_char_device() {
        entry_type = EntryType::Char;
    } else if file_type.is_block_device() {
        entry_type = EntryType::Block;
    } else {
        return Err(other(&format!(
            "{} has unknown file type",
            name.as_ref().display()
        )));
    }

    let mut header = Header::new_gnu();
    header.set_metadata_in_mode(&Metadata::from(stat), mode);
    prepare_header_path(dst, &mut header, name.as_ref())?;

    header.set_entry_type(entry_type);
    let dev_id = stat.rdev();
    let dev_major = ((dev_id >> 32) & 0xffff_f000) | ((dev_id >> 8) & 0x0000_0fff);
    let dev_minor = ((dev_id >> 12) & 0xffff_ff00) | ((dev_id) & 0x0000_00ff);
    header.set_device_major(dev_major as u32)?;
    header.set_device_minor(dev_minor as u32)?;

    header.set_cksum();
    dst.write_all(header.as_bytes())?;

    Ok(())
}

fn prepare_header(size: u64, entry_type: EntryType) -> Header {
    let mut header = Header::new_gnu();
    let name = b"././@LongLink";
    header.as_gnu_mut().unwrap().name[..name.len()].clone_from_slice(&name[..]);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    // + 1 to be compliant with GNU tar
    header.set_size(size + 1);
    header.set_entry_type(entry_type);
    header.set_cksum();
    header
}

/// TODO
pub fn prepare_header_path(
    dst: &mut dyn Write,
    header: &mut Header,
    path: &Path,
) -> io::Result<()> {
    // Try to encode the path directly in the header, but if it ends up not
    // working (probably because it's too long) then try to use the GNU-specific
    // long name extension by emitting an entry which indicates that it's the
    // filename.
    if let Err(e) = header.set_path(path) {
        let data = path2bytes(&path)?;
        let max = header.as_old().name.len();
        // Since `e` isn't specific enough to let us know the path is indeed too
        // long, verify it first before using the extension.
        if data.len() < max {
            return Err(e);
        }
        let header2 = prepare_header(data.len() as u64, EntryType::GNULongName);
        // null-terminated string
        let mut data2 = data.chain(io::repeat(0).take(1));
        append(dst, &header2, &mut data2)?;

        // Truncate the path to store in the header we're about to emit to
        // ensure we've got something at least mentioned. Note that we use
        // `str`-encoding to be compatible with Windows, but in general the
        // entry in the header itself shouldn't matter too much since extraction
        // doesn't look at it.
        let truncated = match str::from_utf8(&data[..max]) {
            Ok(s) => s,
            Err(e) => str::from_utf8(&data[..e.valid_up_to()]).unwrap(),
        };
        header.set_path(truncated)?;
    }
    Ok(())
}

/// TODO: Document
fn prepare_header_link(
    dst: &mut dyn Write,
    header: &mut Header,
    link_name: &Path,
    link_type: EntryType,
) -> io::Result<()> {
    // Same as previous function but for linkname
    if let Err(e) = header.set_link_name(&link_name) {
        let data = path2bytes(&link_name)?;
        if data.len() < header.as_old().linkname.len() {
            return Err(e);
        }

        match link_type {
            EntryType::Link | EntryType::Symlink | EntryType::GNULongLink => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("invalid tar link type {:?}", link_type),
                ))
            }
        };

        let header2 = prepare_header(data.len() as u64, link_type);
        let mut data2 = data.chain(io::repeat(0).take(1));
        append(dst, &header2, &mut data2)?;
    }
    if link_type == EntryType::Link {
        tracing::debug!("finished preparing header link: `{:?}`", header);
    }
    Ok(())
}

impl<W: Write> Drop for Builder<W> {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}
