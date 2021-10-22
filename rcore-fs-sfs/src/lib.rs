#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![feature(async_closure)]
extern crate alloc;
#[macro_use]
extern crate log;

use alloc::{
    collections::BTreeMap,
    string::String,
    sync::{Arc, Weak},
    vec,
    vec::Vec,
};
use core::any::Any;
use core::fmt::{Debug, Error, Formatter};
use core::mem::MaybeUninit;

// use core::future::Future;
// use core::pin::Pin;

use bitvec::prelude::*;
use spin::RwLock;

use rcore_fs::dev::Device;
use rcore_fs::dirty::Dirty;
use rcore_fs::util::*;
use rcore_fs::vfs::{self, FsError, INode, MMapArea, Metadata};

use async_trait::async_trait;
use alloc::boxed::Box;

pub use self::structs::*;

mod structs;
#[cfg(test)]
mod tests;

#[async_trait]
trait DeviceExt: Device {
    async fn read_block(&self, id: BlockId, offset: usize, buf: &mut [u8]) -> vfs::Result<()> {
        debug_assert!(offset + buf.len() <= BLKSIZE);
        info!("id {} offset {} buf.len {}", id, offset, buf.len());
        match self.read_at(id * BLKSIZE + offset, buf).await {
            Ok(len) if len == buf.len() => Ok(()),
            Ok(len) => panic!("read invalid len {}, expected len {} block {} offset {}", 
                len, buf.len(), id, offset),
            Err(e) => panic!("read device error {:?} block {} offset {}", e, id, offset),
        }
    }
    async fn write_block(&self, id: BlockId, offset: usize, buf: &[u8]) -> vfs::Result<()> {
        debug_assert!(offset + buf.len() <= BLKSIZE);
        match self.write_at(id * BLKSIZE + offset, buf).await {
            Ok(len) if len == buf.len() => Ok(()),
            Ok(len) => panic!("write invalid len {}, expected len {}, block {} offset {}", 
                len, buf.len(), id, offset),
            Err(e) => panic!("write device error {:?} block {} offset {}", e, id, offset),
        }
    }
    /// Load struct `T` from given block in device
    async fn load_struct<T: AsBuf + Send>(&self, id: BlockId) -> vfs::Result<T> {
        let mut s: T = unsafe { MaybeUninit::uninit().assume_init() };
        info!("start load struct");
        self.read_block(id, 0, s.as_buf_mut()).await?;
        info!("end load struct");
        Ok(s)
    }
}

impl DeviceExt for dyn Device {}

/// INode for SFS
pub struct INodeImpl {
    /// INode number
    id: INodeId,
    /// On-disk INode
    disk_inode: RwLock<Dirty<DiskINode>>,
    /// Reference to SFS, used by almost all operations
    fs: Arc<SimpleFileSystem>,
    /// Char/block device id (major, minor)
    /// e.g. crw-rw-rw- 1 root wheel 3, 2 May 13 16:40 /dev/null
    device_inode_id: usize,
}

impl Debug for INodeImpl {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "INode {{ id: {}, disk: {:?} }}",
            self.id, self.disk_inode
        )
    }
}

impl INodeImpl {
    /// Map file block id to disk block id
    async fn get_disk_block_id(&self, file_block_id: BlockId) -> vfs::Result<BlockId> {
        let disk_inode = self.disk_inode.read();
        match file_block_id {
            id if id >= disk_inode.blocks as BlockId => Err(FsError::InvalidParam),
            id if id < MAX_NBLOCK_DIRECT => Ok(disk_inode.direct[id] as BlockId),
            id if id < MAX_NBLOCK_INDIRECT => {
                let mut disk_block_id: u32 = 0;
                self.fs.device.read_block(
                    disk_inode.indirect as usize,
                    ENTRY_SIZE * (id - NDIRECT),
                    disk_block_id.as_buf_mut(),
                ).await?;
                Ok(disk_block_id as BlockId)
            }
            id if id < MAX_NBLOCK_DOUBLE_INDIRECT => {
                // double indirect
                let indirect_id = id - MAX_NBLOCK_INDIRECT;
                let mut indirect_block_id: u32 = 0;
                self.fs.device.read_block(
                    disk_inode.db_indirect as usize,
                    ENTRY_SIZE * (indirect_id / BLK_NENTRY),
                    indirect_block_id.as_buf_mut(),
                ).await?;
                assert!(indirect_block_id > 0);
                let mut disk_block_id: u32 = 0;
                self.fs.device.read_block(
                    indirect_block_id as usize,
                    ENTRY_SIZE * (indirect_id as usize % BLK_NENTRY),
                    disk_block_id.as_buf_mut(),
                ).await?;
                assert!(disk_block_id > 0);
                Ok(disk_block_id as BlockId)
            }
            _ => unimplemented!("triple indirect blocks is not supported"),
        }
    }
    async fn set_disk_block_id(&self, file_block_id: BlockId, disk_block_id: BlockId) -> vfs::Result<()> {
        match file_block_id {
            id if id >= self.disk_inode.read().blocks as BlockId => Err(FsError::InvalidParam),
            id if id < MAX_NBLOCK_DIRECT => {
                self.disk_inode.write().direct[id] = disk_block_id as u32;
                Ok(())
            }
            id if id < MAX_NBLOCK_INDIRECT => {
                let disk_block_id = disk_block_id as u32;
                self.fs.device.write_block(
                    self.disk_inode.read().indirect as usize,
                    ENTRY_SIZE * (id - NDIRECT),
                    disk_block_id.as_buf(),
                ).await?;
                Ok(())
            }
            id if id < MAX_NBLOCK_DOUBLE_INDIRECT => {
                // double indirect
                let indirect_id = id - MAX_NBLOCK_INDIRECT;
                let mut indirect_block_id: u32 = 0;
                self.fs.device.read_block(
                    self.disk_inode.read().db_indirect as usize,
                    ENTRY_SIZE * (indirect_id / BLK_NENTRY),
                    indirect_block_id.as_buf_mut(),
                ).await?;
                assert!(indirect_block_id > 0);
                let disk_block_id = disk_block_id as u32;
                self.fs.device.write_block(
                    indirect_block_id as usize,
                    ENTRY_SIZE * (indirect_id as usize % BLK_NENTRY),
                    disk_block_id.as_buf(),
                ).await?;
                Ok(())
            }
            _ => unimplemented!("triple indirect blocks is not supported"),
        }
    }
    /// Only for Dir
    async fn get_file_inode_and_entry_id(&self, name: &str) -> Option<(INodeId, usize)> {
        for i in 0..self.disk_inode.read().size as usize / DIRENT_SIZE {
            let entry = self.read_direntry(i as usize).await.unwrap();
            if entry.name.as_ref() == name {
                return Some((entry.id as INodeId, i as usize))
            }
        }
        None
    }
    async fn get_file_inode_id(&self, name: &str) -> Option<INodeId> {
        self.get_file_inode_and_entry_id(name).await
            .map(|(inode_id, _)| inode_id)
    }
    /// Init dir content. Insert 2 init entries.
    /// This do not init nlinks, please modify the nlinks in the invoker.
    async fn init_direntry(&self, parent: INodeId) -> vfs::Result<()> {
        // Insert entries: '.' '..'
        self._resize(DIRENT_SIZE * 2).await?;
        self.write_direntry(
            0,
            &DiskEntry {
                id: self.id as u32,
                name: Str256::from("."),
            },
        ).await?;
        self.write_direntry(
            1,
            &DiskEntry {
                id: parent as u32,
                name: Str256::from(".."),
            },
        ).await?;
        Ok(())
    }
    async fn read_direntry(&self, id: usize) -> vfs::Result<DiskEntry> {
        let mut direntry: DiskEntry = unsafe { MaybeUninit::uninit().assume_init() };
        self._read_at(DIRENT_SIZE * id, direntry.as_buf_mut()).await?;
        Ok(direntry)
    }
    async fn write_direntry(&self, id: usize, direntry: &DiskEntry) -> vfs::Result<()> {
        self._write_at(DIRENT_SIZE * id, direntry.as_buf()).await?;
        Ok(())
    }
    async fn append_direntry(&self, direntry: &DiskEntry) -> vfs::Result<()> {
        let size = self.disk_inode.read().size as usize;
        let dirent_count = size / DIRENT_SIZE;
        self._resize(size + DIRENT_SIZE).await?;
        self.write_direntry(dirent_count, direntry).await?;
        Ok(())
    }
    /// remove a direntry in middle of file and insert the last one here, useful for direntry remove
    /// should be only used in unlink
    async fn remove_direntry(&self, id: usize) -> vfs::Result<()> {
        let size = self.disk_inode.read().size as usize;
        let dirent_count = size / DIRENT_SIZE;
        debug_assert!(id < dirent_count);
        let last_dirent = self.read_direntry(dirent_count - 1).await?;
        self.write_direntry(id, &last_dirent).await?;
        self._resize(size - DIRENT_SIZE).await?;
        Ok(())
    }
    /// Resize content size, no matter what type it is.
    async fn _resize(&self, len: usize) -> vfs::Result<()> {
        if len > MAX_FILE_SIZE {
            return Err(FsError::InvalidParam);
        }
        let blocks = ((len + BLKSIZE - 1) / BLKSIZE) as u32;
        if blocks > MAX_NBLOCK_DOUBLE_INDIRECT as u32 {
            return Err(FsError::InvalidParam);
        }
        use core::cmp::Ordering;
        let old_blocks = self.disk_inode.read().blocks;
        match blocks.cmp(&old_blocks) {
            Ordering::Equal => {
                self.disk_inode.write().size = len as u32;
            }
            Ordering::Greater => {
                let mut disk_inode = self.disk_inode.write();
                disk_inode.blocks = blocks;
                // allocate indirect block if needed
                if old_blocks < MAX_NBLOCK_DIRECT as u32 && blocks >= MAX_NBLOCK_DIRECT as u32 {
                    disk_inode.indirect = self.fs.alloc_block().expect("no space") as u32;
                }
                // allocate double indirect block if needed
                if blocks >= MAX_NBLOCK_INDIRECT as u32 {
                    if disk_inode.db_indirect == 0 {
                        disk_inode.db_indirect = self.fs.alloc_block().expect("no space") as u32;
                    }
                    let indirect_begin = {
                        if (old_blocks as usize) < MAX_NBLOCK_INDIRECT {
                            0
                        } else {
                            (old_blocks as usize - MAX_NBLOCK_INDIRECT) / BLK_NENTRY + 1
                        }
                    };
                    let indirect_end = (blocks as usize - MAX_NBLOCK_INDIRECT) / BLK_NENTRY + 1;
                    for i in indirect_begin..indirect_end {
                        let indirect = self.fs.alloc_block().expect("no space") as u32;
                        self.fs.device.write_block(
                            disk_inode.db_indirect as usize,
                            ENTRY_SIZE * i,
                            indirect.as_buf(),
                        ).await?;
                    }
                }
                drop(disk_inode);
                // allocate extra blocks
                for i in old_blocks..blocks {
                    let disk_block_id = self.fs.alloc_block().expect("no space");
                    self.set_disk_block_id(i as usize, disk_block_id).await?;
                }
                // clean up
                let mut disk_inode = self.disk_inode.write();
                let old_size = disk_inode.size as usize;
                disk_inode.size = len as u32;
                drop(disk_inode);
                self._clean_at(old_size, len).await?;
            }
            Ordering::Less => {
                // free extra blocks
                for i in blocks..old_blocks {
                    let disk_block_id = self.get_disk_block_id(i as usize).await?;
                    self.fs.free_block(disk_block_id);
                }
                let mut disk_inode = self.disk_inode.write();
                // free indirect block if needed
                if blocks < MAX_NBLOCK_DIRECT as u32
                    && disk_inode.blocks >= MAX_NBLOCK_DIRECT as u32
                {
                    self.fs.free_block(disk_inode.indirect as usize);
                    disk_inode.indirect = 0;
                }
                // free double indirect block if needed
                if disk_inode.blocks >= MAX_NBLOCK_INDIRECT as u32 {
                    let indirect_begin = {
                        if (blocks as usize) < MAX_NBLOCK_INDIRECT {
                            0
                        } else {
                            (blocks as usize - MAX_NBLOCK_INDIRECT) / BLK_NENTRY + 1
                        }
                    };
                    let indirect_end =
                        (disk_inode.blocks as usize - MAX_NBLOCK_INDIRECT) / BLK_NENTRY + 1;
                    for i in indirect_begin..indirect_end {
                        let mut indirect: u32 = 0;
                        self.fs.device.read_block(
                            disk_inode.db_indirect as usize,
                            ENTRY_SIZE * i,
                            indirect.as_buf_mut(),
                        ).await?;
                        assert!(indirect > 0);
                        self.fs.free_block(indirect as usize);
                    }
                    if blocks < MAX_NBLOCK_INDIRECT as u32 {
                        assert!(disk_inode.db_indirect > 0);
                        self.fs.free_block(disk_inode.db_indirect as usize);
                        disk_inode.db_indirect = 0;
                    }
                }
                disk_inode.blocks = blocks;
                disk_inode.size = len as u32;
            }
        }
        Ok(())
    }

    // trait IoFuture : Future<Output = vfs::Result<()>>{
    //     fn init(device: &'a Arc<dyn Device>, range: &'a BlockRange, offset: usize);
    // }

    // impl Future for IoFuture {
    //     type Output = vfs::Result<()>;
    //     fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
    //         Poll::Ready(())
    //     }
    // }

    // type F = dyn FnMut(&Arc<dyn Device>, &BlockRange, usize) -> Box<dyn Future<Output = vfs::Result<()>>>;

    // Note: the _\w*_at method always return begin>size?0:begin<end?0:(min(size,end)-begin) when success
    /// Read/Write content, no matter what type it is
    // async fn _io_at<F>(&self, begin: usize, end: usize, f: F) -> vfs::Result<usize>
    // where 
    // F: FnMut(&Arc<dyn Device>, &BlockRange, usize) -> Box<Pin<dyn Future<Output = vfs::Result<()>>>>,
    // {
    //     let size = self.disk_inode.read().size as usize;
    //     let iter = BlockIter {
    //         begin: size.min(begin),
    //         end: size.min(end),
    //         block_size_log2: BLKSIZE_LOG2,
    //     };

    //     // For each block
    //     let mut buf_offset = 0usize;
    //     for mut range in iter {
    //         range.block = self.get_disk_block_id(range.block).await?;
    //         f(&self.fs.device, &range, buf_offset).await?;
    //         buf_offset += range.len();
    //     }
    //     Ok(buf_offset)
    // }
    /// Read content, no matter what type it is
    async fn _read_at(&self, offset: usize, buf: &mut [u8]) -> vfs::Result<usize> {
        let size = self.disk_inode.read().size as usize;
        let begin = offset;
        let end = offset + buf.len();
        let iter = BlockIter {
            begin: size.min(begin),
            end: size.min(end),
            block_size_log2: BLKSIZE_LOG2,
        };

        // For each block
        let mut buf_offset = 0usize;
        for mut range in iter {
            range.block = self.get_disk_block_id(range.block).await?;
            self.fs.device.read_block(range.block, range.begin, &mut buf[buf_offset..buf_offset + range.len()]).await?;
            buf_offset += range.len();
        }
        Ok(buf_offset)
        // self._io_at(offset, offset + buf.len(), 
        // Box::new(async move |device: &Arc<dyn Device>, range: &BlockRange, offset: usize| {
        //         device.read_block(range.block, range.begin, &mut buf[offset..offset + range.len()]).await
        //     })
        // ).await
    }
    /// Write content, no matter what type it is
    async fn _write_at(&self, offset: usize, buf: &[u8]) -> vfs::Result<usize> {
        let size = self.disk_inode.read().size as usize;
        let begin = offset;
        let end = offset + buf.len();
        let iter = BlockIter {
            begin: size.min(begin),
            end: size.min(end),
            block_size_log2: BLKSIZE_LOG2,
        };

        // For each block
        let mut buf_offset = 0usize;
        for mut range in iter {
            range.block = self.get_disk_block_id(range.block).await?;
            self.fs.device.write_block(range.block, range.begin, &buf[buf_offset..buf_offset + range.len()]).await?;
            buf_offset += range.len();
        }
        Ok(buf_offset)

        // self._io_at(offset, offset + buf.len(), Box::new(
        //     async move |device: &Arc<dyn Device>, range: &BlockRange, offset: usize| {
        //         device.write_block(range.block, range.begin, &buf[offset..offset + range.len()]).await
        //     })
        // ).await
    }
    /// Clean content, no matter what type it is
    async fn _clean_at(&self, begin: usize, end: usize) -> vfs::Result<usize> {
        static ZEROS: [u8; BLKSIZE] = [0; BLKSIZE];
        let size = self.disk_inode.read().size as usize;
        let iter = BlockIter {
            begin: size.min(begin),
            end: size.min(end),
            block_size_log2: BLKSIZE_LOG2,
        };

        // For each block
        let mut buf_offset = 0usize;
        for mut range in iter {
            range.block = self.get_disk_block_id(range.block).await?;
            self.fs.device.write_block(range.block, range.begin, &ZEROS[..range.len()]).await?;
            buf_offset += range.len();
        }
        Ok(buf_offset)

        // self._io_at(begin, end, Box::new(
        //     async move |device: &Arc<dyn Device>, range: &BlockRange, offset: usize| {
        //         device.write_block(range.block, range.begin, &ZEROS[..range.len()]).await
        //     })
        // ).await
    }
    fn nlinks_inc(&self) {
        self.disk_inode.write().nlinks += 1;
    }
    fn nlinks_dec(&self) {
        let mut disk_inode = self.disk_inode.write();
        assert!(disk_inode.nlinks > 0);
        disk_inode.nlinks -= 1;
    }

    pub async fn link_inodeimpl(&self, name: &str, other: &Arc<INodeImpl>) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }
        if !self.get_file_inode_id(name).await.is_none() {
            return Err(FsError::EntryExist);
        }
        let child = other;
        if !Arc::ptr_eq(&self.fs, &child.fs) {
            return Err(FsError::NotSameFs);
        }
        if child.metadata()?.type_ == vfs::FileType::Dir {
            return Err(FsError::IsDir);
        }
        let entry = DiskEntry {
            id: child.id as u32,
            name: Str256::from(name),
        };
        let disk_inode = self.disk_inode.write();
        let old_size = disk_inode.size as usize;
        self._resize(old_size + BLKSIZE).await?;
        self._write_at(old_size, entry.as_buf()).await.unwrap();
        child.nlinks_inc();
        Ok(())
    }
}

#[async_trait]
impl vfs::INode for INodeImpl {
    async fn read_at(&self, offset: usize, buf: &mut [u8]) -> vfs::Result<usize> {
        match self.disk_inode.read().type_ {
            FileType::File => self._read_at(offset, buf).await,
            FileType::SymLink => self._read_at(offset, buf).await,
            FileType::CharDevice => {
                let device_inodes = self.fs.device_inodes.read();
                let device_inode = device_inodes.get(&self.device_inode_id);
                match device_inode {
                    Some(device) => device.read_at(offset, buf).await,
                    None => Err(FsError::DeviceError),
                }
            }
            _ => Err(FsError::NotFile),
        }
    }
    async fn write_at(&self, offset: usize, buf: &[u8]) -> vfs::Result<usize> {
        let DiskINode { type_, size, .. } = **self.disk_inode.read();
        match type_ {
            FileType::File | FileType::SymLink => {
                let end_offset = offset + buf.len();
                if (size as usize) < end_offset {
                    self._resize(end_offset).await?;
                }
                self._write_at(offset, buf).await
            }
            FileType::CharDevice => {
                let device_inodes = self.fs.device_inodes.write();
                let device_inode = device_inodes.get(&self.device_inode_id);
                match device_inode {
                    Some(device) => device.write_at(offset, buf).await,
                    None => Err(FsError::DeviceError),
                }
            }
            _ => Err(FsError::NotFile),
        }
    }
    // fn poll(&self) -> vfs::Result<vfs::PollStatus> {
    //     Ok(vfs::PollStatus {
    //         read: true,
    //         write: true,
    //         error: false,
    //     })
    // }

    /// the size returned here is logical size(entry num for directory), not the disk space used.
    fn metadata(&self) -> vfs::Result<vfs::Metadata> {
        let disk_inode = self.disk_inode.read();
        Ok(vfs::Metadata {
            dev: 0,
            inode: self.id,
            size: match disk_inode.type_ {
                FileType::File | FileType::SymLink => disk_inode.size as usize,
                FileType::Dir => disk_inode.size as usize,
                FileType::CharDevice => 0,
                FileType::BlockDevice => 0,
                _ => panic!("Unknown file type"),
            },
            mode: 0o777,
            type_: vfs::FileType::from(disk_inode.type_.clone()),
            blocks: disk_inode.blocks as usize,
            atime: disk_inode.atime,
            mtime: disk_inode.mtime,
            ctime: disk_inode.ctime,
            nlinks: disk_inode.nlinks as usize,
            uid: 0,
            gid: 0,
            blk_size: BLKSIZE,
            rdev: self.device_inode_id,
        })
    }

    fn set_metadata(&self, metadata: &vfs::Metadata) -> vfs::Result<()> {
        let mut disk_inode = self.disk_inode.write();
        disk_inode.atime = metadata.atime;
        disk_inode.mtime = metadata.mtime;
        disk_inode.ctime = metadata.ctime;
        Ok(())
    }


    async fn flush(&self) {
        self.sync_all().await.expect("Failed to sync when dropping the SimpleFileSystem Inode");
        if self.disk_inode.read().nlinks <= 0 {
            self._resize(0).await.unwrap();
            self.disk_inode.write().sync();
            self.fs.free_block(self.id);
        }
    }
    
    async fn sync_all(&self) -> vfs::Result<()> {
        let mut disk_inode = self.disk_inode.write();
        if disk_inode.dirty() {
            self.fs
                .device
                .write_block(self.id, 0, disk_inode.as_buf()).await?;
            disk_inode.sync();
        }
        Ok(())
    }
    async fn sync_data(&self) -> vfs::Result<()> {
        self.sync_all().await
    }
    async fn resize(&self, len: usize) -> vfs::Result<()> {
        if self.disk_inode.read().type_ != FileType::File
            && self.disk_inode.read().type_ != FileType::SymLink
        {
            return Err(FsError::NotFile);
        }
        self._resize(len).await
    }
    async fn create2(
        &self,
        name: &str,
        type_: vfs::FileType,
        _mode: u32,
        data: usize,
    ) -> vfs::Result<Arc<dyn vfs::INode>> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }

        // Ensure the name is not exist
        if !self.get_file_inode_id(name).await.is_none() {
            return Err(FsError::EntryExist);
        }

        // Create new INode
        let inode = match type_ {
            vfs::FileType::File => self.fs.new_inode_file()?,
            vfs::FileType::SymLink => self.fs.new_inode_symlink()?,
            vfs::FileType::Dir => self.fs.new_inode_dir(self.id).await?,
            vfs::FileType::CharDevice => self.fs.new_inode_chardevice(data)?,
            _ => return Err(vfs::FsError::InvalidParam),
        };

        // Write new entry
        self.append_direntry(&DiskEntry {
            id: inode.id as u32,
            name: Str256::from(name),
        }).await?;
        inode.nlinks_inc();
        if type_ == vfs::FileType::Dir {
            inode.nlinks_inc(); //for .
            self.nlinks_inc(); //for ..
        }

        Ok(inode)
    }

    async fn link(&self, name: &str, other: &Arc<dyn INode>) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }
        if !self.get_file_inode_id(name).await.is_none() {
            return Err(FsError::EntryExist);
        }
        let child = other
            .downcast_ref::<INodeImpl>()
            .ok_or(FsError::NotSameFs)?;
        if !Arc::ptr_eq(&self.fs, &child.fs) {
            return Err(FsError::NotSameFs);
        }
        if child.metadata()?.type_ == vfs::FileType::Dir {
            return Err(FsError::IsDir);
        }
        self.append_direntry(&DiskEntry {
            id: child.id as u32,
            name: Str256::from(name),
        }).await?;
        child.nlinks_inc();
        Ok(())
    }
    async fn unlink(&self, name: &str) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }
        if name == "." {
            return Err(FsError::IsDir);
        }
        if name == ".." {
            return Err(FsError::IsDir);
        }

        let (inode_id, entry_id) = self
            .get_file_inode_and_entry_id(name).await
            .ok_or(FsError::EntryNotFound)?;
        let inode = self.fs.get_inode(inode_id).await;

        let type_ = inode.disk_inode.read().type_;
        if type_ == FileType::Dir {
            // only . and ..
            if inode.disk_inode.read().size as usize / DIRENT_SIZE > 2 {
                return Err(FsError::DirNotEmpty);
            }
        }
        inode.nlinks_dec();
        if type_ == FileType::Dir {
            inode.nlinks_dec(); //for .
            self.nlinks_dec(); //for ..
        }
        self.remove_direntry(entry_id).await?;

        Ok(())
    }
    async fn move_(&self, old_name: &str, target: &Arc<dyn INode>, new_name: &str) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }
        if old_name == "." {
            return Err(FsError::IsDir);
        }
        if old_name == ".." {
            return Err(FsError::IsDir);
        }

        let dest = target
            .downcast_ref::<INodeImpl>()
            .ok_or(FsError::NotSameFs)?;
        let dest_info = dest.metadata()?;
        if !Arc::ptr_eq(&self.fs, &dest.fs) {
            return Err(FsError::NotSameFs);
        }
        if dest_info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if dest_info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }
        if let Some((_, id)) = dest.get_file_inode_and_entry_id(new_name).await {
            dest.remove_direntry(id).await?;
        }

        let (inode_id, entry_id) = self
            .get_file_inode_and_entry_id(old_name).await
            .ok_or(FsError::EntryNotFound)?;
        if info.inode == dest_info.inode {
            // rename: in place modify name
            self.write_direntry(
                entry_id,
                &DiskEntry {
                    id: inode_id as u32,
                    name: Str256::from(new_name),
                },
            ).await?;
        } else {
            // move
            dest.append_direntry(&DiskEntry {
                id: inode_id as u32,
                name: Str256::from(new_name),
            }).await?;
            self.remove_direntry(entry_id).await?;

            let inode = self.fs.get_inode(inode_id).await;
            if inode.metadata()?.type_ == vfs::FileType::Dir {
                self.nlinks_dec();
                dest.nlinks_inc();
            }
        }
        Ok(())
    }
    async fn find(&self, name: &str) -> vfs::Result<Arc<dyn vfs::INode>> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        let inode_id = self.get_file_inode_id(name).await.ok_or(FsError::EntryNotFound)?;
        Ok(self.fs.get_inode(inode_id).await)
    }
    async fn get_entry(&self, id: usize) -> vfs::Result<String> {
        if self.disk_inode.read().type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if id >= self.disk_inode.read().size as usize / DIRENT_SIZE {
            return Err(FsError::EntryNotFound);
        };
        let entry = self.read_direntry(id).await?;
        Ok(String::from(entry.name.as_ref()))
    }

    async fn get_entry_with_metadata(&self, id: usize) -> vfs::Result<(Metadata, String)> {
        if self.disk_inode.read().type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if id >= self.disk_inode.read().size as usize / DIRENT_SIZE {
            return Err(FsError::EntryNotFound);
        };
        let entry = self.read_direntry(id).await?;
        Ok((
            self.fs.get_inode(entry.id as usize).await.metadata()?,
            String::from(entry.name.as_ref()),
        ))
    }

    // TODO: fix me
    fn io_control(&self, _cmd: u32, _data: usize) -> vfs::Result<usize> {
        if self.metadata().unwrap().type_ != vfs::FileType::CharDevice {
            return Err(FsError::IOCTLError);
        }
        Ok(0)
        // let device_inodes = self.fs.device_inodes.read();
        // let device_inode = device_inodes.get(&self.device_inode_id);
        // match device_inode {
        //     Some(x) => x.io_control(_cmd, _data),
        //     None => {
        //         warn!("cannot find corresponding device inode in call_inoctl");
        //         Err(FsError::IOCTLError)
        //     }
        // }
    }
    fn mmap(&self, _area: MMapArea) -> vfs::Result<()> {
        Err(FsError::NotSupported)
    }
    fn fs(&self) -> Arc<dyn vfs::FileSystem> {
        self.fs.clone()
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl Drop for INodeImpl {
    /// Auto sync when drop
    fn drop(&mut self) {
        // DO NOTHING. Should call flush() when drop.

        // self.sync_all().await
        //     .expect("Failed to sync when dropping the SimpleFileSystem Inode");
        // if self.disk_inode.read().nlinks <= 0 {
        //     self._resize(0).await.unwrap();
        //     self.disk_inode.write().sync().await;
        //     self.fs.free_block(self.id).await;
        // }
    }
}

/// async filesystem for sfs
///
/// ## 内部可变性
/// 为了方便协调外部及INode对SFS的访问，并为日后并行化做准备，
/// 将SFS设置为内部可变，即对外接口全部是&self，struct的全部field用RwLock包起来
/// 这样其内部各field均可独立访问
pub struct SimpleFileSystem {
    /// on-disk superblock
    super_block: RwLock<Dirty<SuperBlock>>,
    /// blocks in use are mared 0
    free_map: RwLock<Dirty<BitVec<Lsb0, u8>>>,
    /// inode list
    inodes: RwLock<BTreeMap<INodeId, Weak<INodeImpl>>>,
    /// device
    device: Arc<dyn Device>,
    /// Pointer to self, used by INodes
    self_ptr: Weak<SimpleFileSystem>,
    /// device inode
    device_inodes: RwLock<BTreeMap<usize, Arc<DeviceINode>>>,
}

impl SimpleFileSystem {
    /// Load SFS from device
    pub async fn open(device: Arc<dyn Device>) -> vfs::Result<Arc<Self>> {
        info!("load super block..");
        let super_block = device.load_struct::<SuperBlock>(BLKN_SUPER).await?;
        info!("load super block over");
        if !super_block.check() {
            return Err(FsError::WrongFs);
        }
        let mut freemap_disk = vec![0u8; BLKSIZE * super_block.freemap_blocks as usize];
        for i in 0..super_block.freemap_blocks as usize {
            device.read_block(
                BLKN_FREEMAP + i,
                0,
                &mut freemap_disk[i * BLKSIZE..(i + 1) * BLKSIZE],
            ).await?;
        }

        Ok(SimpleFileSystem {
            super_block: RwLock::new(Dirty::new(super_block)),
            free_map: RwLock::new(Dirty::new(BitVec::from_vec(freemap_disk))),
            inodes: RwLock::new(BTreeMap::new()),
            device,
            self_ptr: Weak::default(),
            device_inodes: RwLock::new(BTreeMap::new()),
        }
        .wrap())
    }
    /// Create a new SFS on blank disk
    pub async fn create(device: Arc<dyn Device>, space: usize) -> vfs::Result<Arc<Self>> {
        let blocks = (space + BLKSIZE - 1) / BLKSIZE;
        let freemap_blocks = (space + BLKBITS * BLKSIZE - 1) / BLKBITS / BLKSIZE;
        assert!(blocks >= 16, "space too small");

        let super_block = SuperBlock {
            magic: MAGIC,
            blocks: blocks as u32,
            unused_blocks: (blocks - BLKN_FREEMAP - freemap_blocks) as u32,
            info: Str32::from(DEFAULT_INFO),
            freemap_blocks: freemap_blocks as u32,
        };
        let free_map = {
            let mut bitset = BitVec::with_capacity(freemap_blocks * BLKBITS);
            bitset.extend(core::iter::repeat(false).take(freemap_blocks * BLKBITS));
            for i in (BLKN_FREEMAP + freemap_blocks)..blocks {
                bitset.set(i, true);
            }
            bitset
        };

        let sfs = SimpleFileSystem {
            super_block: RwLock::new(Dirty::new_dirty(super_block)),
            free_map: RwLock::new(Dirty::new_dirty(free_map)),
            inodes: RwLock::new(BTreeMap::new()),
            device,
            self_ptr: Weak::default(),
            device_inodes: RwLock::new(BTreeMap::new()),
        }
        .wrap();

        // Init root INode
        let root = sfs._new_inode(BLKN_ROOT, Dirty::new_dirty(DiskINode::new_dir()));
        root.init_direntry(BLKN_ROOT).await?;
        root.nlinks_inc(); //for .
        root.nlinks_inc(); //for ..(root's parent is itself)
        root.sync_all().await?;

        Ok(sfs)
    }
    /// Wrap pure SimpleFileSystem with Arc
    /// Used in constructors
    fn wrap(self) -> Arc<Self> {
        // Create an Arc, make a Weak from it, then put it into the struct.
        // It's a little tricky.
        let fs = Arc::new(self);
        let weak = Arc::downgrade(&fs);
        let ptr = Arc::into_raw(fs) as *mut Self;
        unsafe {
            (*ptr).self_ptr = weak;
        }
        unsafe { Arc::from_raw(ptr) }
    }

    /// Allocate a block, return block id
    fn alloc_block(&self) -> Option<usize> {
        let mut free_map = self.free_map.write();
        let id = free_map.alloc();
        if let Some(block_id) = id {
            let mut super_block = self.super_block.write();
            if super_block.unused_blocks == 0 {
                free_map.set(block_id, true);
                return None;
            }
            super_block.unused_blocks -= 1; // will not underflow
            trace!("alloc block {:#x}", block_id);
        } else {
            let super_block = self.super_block.read();
            panic!("{:?}", super_block)
        }
        id
    }
    /// Free a block
    fn free_block(&self, block_id: usize) {
        let mut free_map = self.free_map.write();
        assert!(!free_map[block_id]);
        free_map.set(block_id, true);
        self.super_block.write().unused_blocks += 1;
        trace!("free block {:#x}", block_id);
    }

    pub fn new_device_inode(&self, device_inode_id: usize, device_inode: Arc<DeviceINode>) {
        self.device_inodes
            .write()
            .insert(device_inode_id, device_inode);
    }

    /// Create a new INode struct, then insert it to self.inodes
    /// Private used for load or create INode
    fn _new_inode(&self, id: INodeId, disk_inode: Dirty<DiskINode>) -> Arc<INodeImpl> {
        let device_inode_id = disk_inode.device_inode_id;
        let inode = Arc::new(INodeImpl {
            id,
            disk_inode: RwLock::new(disk_inode),
            fs: self.self_ptr.upgrade().unwrap(),
            device_inode_id,
        });
        self.inodes.write().insert(id, Arc::downgrade(&inode));
        inode
    }

    /// Get inode by id. Load if not in memory.
    /// ** Must ensure it's a valid INode **
    async fn get_inode(&self, id: INodeId) -> Arc<INodeImpl> {
        assert!(!self.free_map.read()[id]);

        // In the BTreeSet and not weak.
        if let Some(inode) = self.inodes.read().get(&id) {
            if let Some(inode) = inode.upgrade() {
                return inode;
            }
        }
        // Load if not in set, or is weak ref.
        let disk_inode = Dirty::new(self.device.load_struct::<DiskINode>(id).await.unwrap());
        self._new_inode(id, disk_inode)
    }
    /// Create a new INode file
    fn new_inode_file(&self) -> vfs::Result<Arc<INodeImpl>> {
        let id = self.alloc_block().ok_or(FsError::NoDeviceSpace)?;
        let disk_inode = Dirty::new_dirty(DiskINode::new_file());
        Ok(self._new_inode(id, disk_inode))
    }
    /// Create a new INode symlink
    fn new_inode_symlink(&self) -> vfs::Result<Arc<INodeImpl>> {
        let id = self.alloc_block().ok_or(FsError::NoDeviceSpace)?;
        let disk_inode = Dirty::new_dirty(DiskINode::new_symlink());
        Ok(self._new_inode(id, disk_inode))
    }
    /// Create a new INode dir
    async fn new_inode_dir(&self, parent: INodeId) -> vfs::Result<Arc<INodeImpl>> {
        let id = self.alloc_block().ok_or(FsError::NoDeviceSpace)?;
        let disk_inode = Dirty::new_dirty(DiskINode::new_dir());
        let inode = self._new_inode(id, disk_inode);
        inode.init_direntry(parent).await?;
        Ok(inode)
    }
    /// Create a new INode chardevice
    pub fn new_inode_chardevice(&self, device_inode_id: usize) -> vfs::Result<Arc<INodeImpl>> {
        let id = self.alloc_block().ok_or(FsError::NoDeviceSpace)?;
        let disk_inode = Dirty::new_dirty(DiskINode::new_chardevice(device_inode_id));
        let new_inode = self._new_inode(id, disk_inode);
        Ok(new_inode)
    }
    async fn flush_weak_inodes(&self) {
        let mut inodes = self.inodes.write();
        let remove_ids: Vec<_> = inodes
            .iter()
            .filter(|(_, inode)| inode.upgrade().is_none())
            .map(|(&id, _)| id)
            .collect();
        for id in remove_ids.iter() {
            if let Some(inode) = inodes.remove(&id).map_or(None, |n| n.upgrade()) {
                inode.flush().await;
                // drop inode
            }
        }
    }
}

#[async_trait]
impl vfs::FileSystem for SimpleFileSystem {

    /// Write back super block if dirty
    async fn sync(&self) -> vfs::Result<()> {
        // order is important, see issue #18
        let mut free_map = self.free_map.write();
        let mut super_block = self.super_block.write();
        if super_block.dirty() {
            self.device
                .write_at(BLKSIZE * BLKN_SUPER, super_block.as_buf()).await?;
            super_block.sync();
        }
        if free_map.dirty() {
            let data = free_map.as_buf();
            for i in 0..super_block.freemap_blocks as usize {
                self.device.write_at(
                    BLKSIZE * (BLKN_FREEMAP + i),
                    &data[i * BLKSIZE..(i + 1) * BLKSIZE],
                ).await?;
            }
            free_map.sync();
        }
        self.flush_weak_inodes().await;
        for inode in self.inodes.read().values() {
            if let Some(inode) = inode.upgrade() {
                inode.sync_all().await?;
            }
        }
        self.device.sync().await?;
        Ok(())
    }

    async fn root_inode(&self) -> Arc<dyn vfs::INode> {
        self.get_inode(BLKN_ROOT).await
        // let root = self.get_inode(BLKN_ROOT);
        // root.create("dev", vfs::FileType::Dir, 0).expect("fail to create dev"); // what's mode?
        // return root;
    }

    fn info(&self) -> vfs::FsInfo {
        let sb = self.super_block.read();
        vfs::FsInfo {
            bsize: BLKSIZE,
            frsize: BLKSIZE,
            blocks: sb.blocks as usize,
            bfree: sb.unused_blocks as usize,
            bavail: sb.unused_blocks as usize,
            files: sb.blocks as usize,        // inaccurate
            ffree: sb.unused_blocks as usize, // inaccurate
            namemax: MAX_FNAME_LEN,
        }
    }
}

impl Drop for SimpleFileSystem {
    /// Auto sync when drop
    fn drop(&mut self) {
        // DO NOTHING. Should call flush in advance.

        // unimplemented!();
        // self.sync().await
        //     .expect("Failed to sync when dropping the SimpleFileSystem");
    }
}


trait BitsetAlloc {
    fn alloc(&mut self) -> Option<usize>;
}

impl BitsetAlloc for BitVec<Lsb0, u8> {
    fn alloc(&mut self) -> Option<usize> {
        // TODO: more efficient
        let id = (0..self.len()).find(|&i| self[i]);
        if let Some(id) = id {
            self.set(id, false);
        }
        id
    }
}

impl AsBuf for BitVec<Lsb0, u8> {
    fn as_buf(&self) -> &[u8] {
        self.as_raw_slice()
    }
    fn as_buf_mut(&mut self) -> &mut [u8] {
        self.as_mut_raw_slice()
    }
}

impl AsBuf for [u8; BLKSIZE] {}

impl From<FileType> for vfs::FileType {
    fn from(t: FileType) -> Self {
        match t {
            FileType::File => vfs::FileType::File,
            FileType::SymLink => vfs::FileType::SymLink,
            FileType::Dir => vfs::FileType::Dir,
            FileType::CharDevice => vfs::FileType::CharDevice,
            FileType::BlockDevice => vfs::FileType::BlockDevice,
            _ => panic!("unknown file type"),
        }
    }
}
