use crate::{
    encoder::{TiffValue, TiffWriter},
    error::TiffResult,
    ifd::{BufferedEntry, Directory},
    tags::Tag,
    TiffKind,
};
use std::{
    io::{Seek, Write},
    marker::PhantomData,
    mem,
};

/// Low level interface to encode ifd directories.
///
/// You should call `finish` on this when you are finished with it.
/// Encoding can silently fail while this is dropping.
pub struct DirectoryEncoder<'a, W: 'a + Write + Seek, K: TiffKind> {
    pub writer: &'a mut TiffWriter<W>,
    dropped: bool,
    ifd_pointer_pos: u64,
    pub ifd: Directory<BufferedEntry>,
    sub_ifd: Option<Directory<BufferedEntry>>,
    _phantom: PhantomData<K>,
}

impl<'a, W: 'a + Write + Seek, K: TiffKind> DirectoryEncoder<'a, W, K> {
    pub fn new(writer: &'a mut TiffWriter<W>) -> TiffResult<Self> {
        // the previous word is the IFD offset position
        let ifd_pointer_pos = writer.offset() - mem::size_of::<K::OffsetType>() as u64;
        writer.pad_word_boundary()?; // TODO: Do we need to adjust this for BigTiff?
        Ok(DirectoryEncoder::<W, K> {
            writer,
            dropped: false,
            ifd_pointer_pos,
            ifd: Directory::new(),
            sub_ifd: None,
            _phantom: ::std::marker::PhantomData,
        })
    }

    /// Start writing to sub-IFD
    pub fn subdirectory_start(&mut self) {
        self.sub_ifd = Some(Directory::new());
    }

    /// Stop writing to sub-IFD and resume master IFD, returns offset of sub-IFD
    pub fn subirectory_close(&mut self) -> TiffResult<u64> {
        let offset = self.write_directory()?;
        K::write_offset(self.writer, 0)?;
        self.sub_ifd = None;
        Ok(offset)
    }

    /// Write a single ifd tag.
    pub fn write_tag<T: TiffValue>(&mut self, tag: Tag, value: T) -> TiffResult<()> {
        let mut bytes = Vec::with_capacity(value.bytes());
        {
            let mut writer = TiffWriter::new(&mut bytes);
            value.write(&mut writer)?;
        }

        let active_ifd = match &self.sub_ifd {
            None => &mut self.ifd,
            Some(_v) => self.sub_ifd.as_mut().unwrap(),
        };

        active_ifd.insert(
            tag,
            BufferedEntry {
                type_: value.is_type(),
                count: value.count().try_into()?,
                data: bytes,
            },
        );

        Ok(())
    }

    fn write_directory(&mut self) -> TiffResult<u64> {
        let active_ifd = match &self.sub_ifd {
            None => &mut self.ifd,
            Some(_v) => self.sub_ifd.as_mut().unwrap(),
        };

        // Start by writing out all values
        for &mut BufferedEntry {
            data: ref mut bytes,
            ..
        } in active_ifd.values_mut()
        {
            let data_bytes = K::OffsetType::BYTE_LEN as usize;

            if bytes.len() > data_bytes {
                let offset = self.writer.offset();
                self.writer.write_bytes(bytes)?;
                *bytes = vec![0; data_bytes];
                let mut writer = TiffWriter::new(bytes as &mut [u8]);
                K::write_offset(&mut writer, offset)?;
            } else {
                while bytes.len() < data_bytes {
                    bytes.push(0);
                }
            }
        }

        let offset = self.writer.offset();

        K::write_entry_count(self.writer, active_ifd.len())?;
        for (
            tag,
            BufferedEntry {
                type_: field_type,
                count,
                data: offset,
            },
        ) in active_ifd.iter()
        {
            self.writer.write_u16(tag.to_u16())?;
            self.writer.write_u16(field_type.to_u16())?;
            K::convert_offset(*count)?.write(self.writer)?;
            self.writer.write_bytes(&offset)?;
        }

        Ok(offset)
    }

    /// Write some data to the tiff file, the offset of the data is returned.
    ///
    /// This could be used to write tiff strips.
    pub fn write_data<T: TiffValue>(&mut self, value: T) -> TiffResult<u64> {
        let offset = self.writer.offset();
        value.write(self.writer)?;
        Ok(offset)
    }

    /// Provides the number of bytes written by the underlying TiffWriter during the last call.
    pub fn last_written(&self) -> u64 {
        self.writer.last_written()
    }

    pub fn finish_internal(&mut self) -> TiffResult<()> {
        if self.sub_ifd.is_some() {
            self.subirectory_close()?;
        }
        let ifd_pointer = self.write_directory()?;
        let curr_pos = self.writer.offset();

        self.writer.goto_offset(self.ifd_pointer_pos)?;
        K::write_offset(self.writer, ifd_pointer)?;
        self.writer.goto_offset(curr_pos)?;
        K::write_offset(self.writer, 0)?;

        self.dropped = true;

        Ok(())
    }

    /// Write out the ifd directory.
    pub fn finish(mut self) -> TiffResult<()> {
        self.finish_internal()
    }
}

impl<'a, W: Write + Seek, K: TiffKind> Drop for DirectoryEncoder<'a, W, K> {
    fn drop(&mut self) {
        if !self.dropped {
            let _ = self.finish_internal();
        }
    }
}
