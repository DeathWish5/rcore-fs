#![cfg_attr(not(any(test, feature = "std")), no_std)]

extern crate alloc;

use alloc::{
    boxed::Box,
    collections::BTreeMap,
    string::{String, ToString},
    sync::{Arc, Weak},
};
use async_trait::async_trait;
use core::any::Any;
use rcore_fs::vfs::*;
use spin::RwLock;
pub mod special;

/// Device file system
///
/// The filesystem for all device files.
/// It should be mounted at /dev.
///
/// The file system is readonly from the root INode.
/// You can add or remove devices through `add()` and `remove()`.
pub struct DevFS {
    root: Arc<DevINode>,
}

#[async_trait]
impl FileSystem for DevFS {
    async fn sync(&self) -> Result<()> {
        Ok(())
    }

    async fn root_inode(&self) -> Arc<dyn INode> {
        self.root.clone()
    }

    fn info(&self) -> FsInfo {
        FsInfo {
            bsize: 0,
            frsize: 0,
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            ffree: 0,
            namemax: 0,
        }
    }
}

impl DevFS {
    pub fn new() -> Arc<Self> {
        let fs = Arc::new(Self {
            root: DevINode::new(),
        });
        *fs.root.fs.write() = Arc::downgrade(&fs);
        fs
    }

    pub fn root(&self) -> Arc<DevINode> {
        self.root.clone()
    }

    /// Generate a new inode id
    pub fn new_inode_id() -> usize {
        use core::sync::atomic::*;
        static ID: AtomicUsize = AtomicUsize::new(1);
        ID.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct DevINode {
    this: Weak<DevINode>,
    parent: Weak<DevINode>,
    fs: RwLock<Weak<DevFS>>,
    children: RwLock<BTreeMap<String, Arc<dyn INode>>>,
    inode_id: usize,
}

impl DevINode {
    fn new_with_parent(parent: Weak<DevINode>) -> Arc<Self> {
        Self {
            this: Weak::default(),
            parent,
            fs: RwLock::new(Weak::default()),
            children: RwLock::new(BTreeMap::new()),
            inode_id: DevFS::new_inode_id(),
        }
        .wrap()
    }

    fn new() -> Arc<Self> {
        Self::new_with_parent(Weak::default())
    }

    /// Wrap pure DevFS with Arc
    /// Used in constructors
    fn wrap(self) -> Arc<Self> {
        // Create an Arc, make a Weak from it, then put it into the struct.
        // It's a little tricky.
        let this = Arc::new(self);
        let weak = Arc::downgrade(&this);
        let ptr = Arc::into_raw(this) as *mut Self;
        unsafe {
            (*ptr).this = weak;
        }
        unsafe { Arc::from_raw(ptr) }
    }

    pub fn add_dir(&self, name: &str) -> Result<Arc<DevINode>> {
        let mut children = self.children.write();
        if children.contains_key(name) {
            return Err(FsError::EntryExist);
        }
        let dir = Self::new_with_parent(self.this.clone());
        *dir.fs.write() = self.fs.read().clone();
        children.insert(String::from(name), dir.clone());
        Ok(dir)
    }

    pub fn add(&self, name: &str, dev: Arc<dyn INode>) -> Result<()> {
        let mut children = self.children.write();
        if children.contains_key(name) {
            return Err(FsError::EntryExist);
        }
        children.insert(String::from(name), dev);
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<()> {
        let mut children = self.children.write();
        children.remove(name).ok_or(FsError::EntryNotFound)?;
        Ok(())
    }
}

#[async_trait]
impl INode for DevINode {
    async fn read_at(&self, _offset: usize, _buf: &mut [u8]) -> Result<usize> {
        Err(FsError::IsDir)
    }

    async fn write_at(&self, _offset: usize, _buf: &[u8]) -> Result<usize> {
        Err(FsError::IsDir)
    }

    fn poll(&self) -> Result<PollStatus> {
        Err(FsError::IsDir)
    }

    fn metadata(&self) -> Result<Metadata> {
        Ok(Metadata {
            dev: 0,
            inode: self.inode_id,
            size: self.children.read().len(),
            blk_size: 0,
            blocks: 0,
            atime: Timespec { sec: 0, nsec: 0 },
            mtime: Timespec { sec: 0, nsec: 0 },
            ctime: Timespec { sec: 0, nsec: 0 },
            type_: FileType::Dir,
            mode: 0o755,
            nlinks: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
        })
    }

    fn set_metadata(&self, _metadata: &Metadata) -> Result<()> {
        Err(FsError::NotSupported)
    }

    async fn sync_all(&self) -> Result<()> {
        Ok(())
    }

    async fn sync_data(&self) -> Result<()> {
        Ok(())
    }

    async fn resize(&self, _len: usize) -> Result<()> {
        Err(FsError::IsDir)
    }

    async fn create(&self, _name: &str, _type_: FileType, _mode: u32) -> Result<Arc<dyn INode>> {
        Err(FsError::NotSupported)
    }

    async fn link(&self, _name: &str, _other: &Arc<dyn INode>) -> Result<()> {
        Err(FsError::NotSupported)
    }

    async fn unlink(&self, _name: &str) -> Result<()> {
        Err(FsError::NotSupported)
    }

    async fn move_(
        &self,
        _old_name: &str,
        _target: &Arc<dyn INode>,
        _new_name: &str,
    ) -> Result<()> {
        Err(FsError::NotSupported)
    }

    async fn find(&self, name: &str) -> Result<Arc<dyn INode>> {
        match name {
            "." => Ok(self.this.upgrade().ok_or(FsError::EntryNotFound)?),
            ".." => Ok(self.parent.upgrade().ok_or(FsError::EntryNotFound)?),
            name => self
                .children
                .read()
                .get(name)
                .cloned()
                .ok_or(FsError::EntryNotFound),
        }
    }

    async fn get_entry(&self, id: usize) -> Result<String> {
        match id {
            0 => Ok(String::from(".")),
            1 => Ok(String::from("..")),
            i => {
                if let Some(s) = self.children.read().keys().nth(i - 2) {
                    Ok(s.to_string())
                } else {
                    Err(FsError::EntryNotFound)
                }
            }
        }
    }

    fn io_control(&self, _cmd: u32, _data: usize) -> Result<usize> {
        Err(FsError::NotSupported)
    }

    fn mmap(&self, _area: MMapArea) -> Result<()> {
        Err(FsError::NotSupported)
    }

    fn fs(&self) -> Arc<dyn FileSystem> {
        self.fs.read().upgrade().unwrap()
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}
