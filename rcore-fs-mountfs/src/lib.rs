#![cfg_attr(not(any(test, feature = "std")), no_std)]

extern crate alloc;
#[macro_use]
extern crate log;

use alloc::{
    collections::BTreeMap,
    string::String,
    sync::{Arc, Weak},
};
use async_trait::async_trait;
use async_recursion::async_recursion;
use core::any::Any;
use rcore_fs::vfs::*;
use spin::RwLock;
use alloc::boxed::Box;


#[cfg(test)]
mod tests;

/// The filesystem on which all the other filesystems are mounted
pub struct MountFS {
    /// The inner file system
    inner: Arc<dyn FileSystem>,
    /// All mounted children file systems
    mountpoints: RwLock<BTreeMap<INodeId, Arc<MountFS>>>,
    /// The mount point of this file system
    self_mountpoint: Option<Arc<MNode>>,
    /// Weak reference to self
    self_ref: Weak<MountFS>,
}

type INodeId = usize;

/// INode for `MountFS`
pub struct MNode {
    /// The inner INode
    pub inode: Arc<dyn INode>,
    /// Associated `MountFS`
    pub vfs: Arc<MountFS>,
    /// Weak reference to self
    self_ref: Weak<MNode>,
}

impl MountFS {
    /// Create a `MountFS` wrapper for file system `fs`
    pub fn new(fs: Arc<dyn FileSystem>) -> Arc<Self> {
        MountFS {
            inner: fs,
            mountpoints: RwLock::new(BTreeMap::new()),
            self_mountpoint: None,
            self_ref: Weak::default(),
        }
        .wrap()
    }

    /// Wrap pure `MountFS` with `Arc<..>`.
    /// Used in constructors.
    fn wrap(self) -> Arc<Self> {
        // Create an Arc, make a Weak from it, then put it into the struct.
        // It's a little tricky.
        let fs = Arc::new(self);
        let weak = Arc::downgrade(&fs);
        let ptr = Arc::into_raw(fs) as *mut Self;
        unsafe {
            (*ptr).self_ref = weak;
            Arc::from_raw(ptr)
        }
    }

    /// Strong type version of `root_inode`
    pub async fn root_inode(&self) -> Arc<MNode> {
        MNode {
            inode: self.inner.root_inode().await,
            vfs: self.self_ref.upgrade().unwrap(),
            self_ref: Weak::default(),
        }
        .wrap()
    }
}

impl MNode {
    /// Wrap pure `INode` with `Arc<..>`.
    /// Used in constructors.
    fn wrap(self) -> Arc<Self> {
        // Create an Arc, make a Weak from it, then put it into the struct.
        // It's a little tricky.
        let inode = Arc::new(self);
        let weak = Arc::downgrade(&inode);
        let ptr = Arc::into_raw(inode) as *mut Self;
        unsafe {
            (*ptr).self_ref = weak;
            Arc::from_raw(ptr)
        }
    }

    /// Mount file system `fs` at this INode
    pub fn mount(&self, fs: Arc<dyn FileSystem>) -> Result<Arc<MountFS>> {
        let new_fs = MountFS {
            inner: fs,
            mountpoints: RwLock::new(BTreeMap::new()),
            self_mountpoint: Some(self.self_ref.upgrade().unwrap()),
            self_ref: Weak::default(),
        }
        .wrap();
        let inode_id = self.inode.metadata()?.inode;
        self.vfs
            .mountpoints
            .write()
            .insert(inode_id, new_fs.clone());
        Ok(new_fs)
    }

    /// Get the root INode of the mounted fs at here.
    /// Return self if no mounted fs.
    async fn overlaid_inode(&self) -> Arc<MNode> {
        let inode_id = self.metadata().unwrap().inode;
        if let Some(sub_vfs) = self.vfs.mountpoints.read().get(&inode_id) {
            sub_vfs.root_inode().await
        } else {
            self.self_ref.upgrade().unwrap()
        }
    }

    /// Is the root INode of its FS?
    async fn is_root(&self) -> bool {
        self.inode.fs().root_inode().await.metadata().unwrap().inode
            == self.inode.metadata().unwrap().inode
    }

    /// Strong type version of `create()`
    pub async fn create(&self, name: &str, type_: FileType, mode: u32) -> Result<Arc<Self>> {
        Ok(MNode {
            inode: self.inode.create(name, type_, mode).await?,
            vfs: self.vfs.clone(),
            self_ref: Weak::default(),
        }
        .wrap())
    }

    #[async_recursion]
    /// Strong type version of `find()`
    pub async fn find(&self, root: bool, name: &str) -> Result<Arc<Self>> {
        match name {
            "" | "." => Ok(self.self_ref.upgrade().unwrap()),
            ".." => {
                // Going Up
                // We need to check these things:
                // 1. Is going forward allowed, considering the current root?
                // 2. Is going forward trespassing the filesystem border,
                //    thus requires falling back to parent of original_mountpoint?
                // TODO: check going up.
                if root {
                    Ok(self.self_ref.upgrade().unwrap())
                } else if self.is_root().await {
                    // Here is mountpoint.
                    match &self.vfs.self_mountpoint {
                        Some(inode) => inode.find(root, "..").await,
                        // root fs
                        None => Ok(self.self_ref.upgrade().unwrap()),
                    }
                } else {
                    // Not trespassing filesystem border. Parent and myself in the same filesystem.
                    Ok(MNode {
                        inode: self.inode.find(name).await?, // Going up is handled by the filesystem. A better API?
                        vfs: self.vfs.clone(),
                        self_ref: Weak::default(),
                    }
                    .wrap())
                }
            }
            _ => {
                // Going down may trespass the filesystem border.
                // An INode replacement is required here.
                Ok(MNode {
                    inode: self.overlaid_inode().await.inode.find(name).await?,
                    vfs: self.vfs.clone(),
                    self_ref: Weak::default(),
                }
                .wrap()
                .overlaid_inode().await)
            }
        }
    }

    /// If `child` is a child of `self`, return its name.
    pub async fn find_name_by_child(&self, child: &Arc<MNode>) -> Result<String> {
        for index in 0.. {
            let name = self.inode.get_entry(index).await?;
            match name.as_ref() {
                "." | ".." => {}
                _ => {
                    let queryback = self.find(false, &name).await?.overlaid_inode().await;
                    // TODO: mountpoint check!
                    debug!("checking name {}", name);
                    if Arc::ptr_eq(&queryback.vfs, &child.vfs)
                        && queryback.inode.metadata()?.inode == child.inode.metadata()?.inode
                    {
                        return Ok(name);
                    }
                }
            }
        }
        Err(FsError::EntryNotFound)
    }
}

#[async_trait]
impl FileSystem for MountFS {
    async fn sync(&self) -> Result<()> {
        self.inner.sync().await?;
        for mount_fs in self.mountpoints.read().values() {
            mount_fs.sync().await?;
        }
        Ok(())
    }

    async fn root_inode(&self) -> Arc<dyn INode> {
        self.root_inode().await
    }

    fn info(&self) -> FsInfo {
        self.inner.info()
    }
}

#[async_trait]
// unwrap `MNode` and forward methods to inner except `find()`
impl INode for MNode {
    async fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.inode.read_at(offset, buf).await
    }

    async fn write_at(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        self.inode.write_at(offset, buf).await
    }

    // fn poll(&self) -> Result<PollStatus> {
    //     self.inode.poll()
    // }

    fn metadata(&self) -> Result<Metadata> {
        self.inode.metadata()
    }

    fn set_metadata(&self, metadata: &Metadata) -> Result<()> {
        self.inode.set_metadata(metadata)
    }

    async fn sync_all(&self) -> Result<()> {
        self.inode.sync_all().await
    }

    async fn sync_data(&self) -> Result<()> {
        self.inode.sync_data().await
    }

    async fn resize(&self, len: usize) -> Result<()> {
        self.inode.resize(len).await
    }

    async fn create(&self, name: &str, type_: FileType, mode: u32) -> Result<Arc<dyn INode>> {
        Ok(self.create(name, type_, mode).await?)
    }

    async fn link(&self, name: &str, other: &Arc<dyn INode>) -> Result<()> {
        let other = &other
            .downcast_ref::<Self>()
            .ok_or(FsError::NotSameFs)?
            .inode;
        self.inode.link(name, other).await
    }

    async fn unlink(&self, name: &str) -> Result<()> {
        let inode_id = self.inode.find(name).await?.metadata()?.inode;
        // target INode is being mounted
        if self.vfs.mountpoints.read().contains_key(&inode_id) {
            return Err(FsError::Busy);
        }
        self.inode.unlink(name).await
    }

    async fn move_(&self, old_name: &str, target: &Arc<dyn INode>, new_name: &str) -> Result<()> {
        let target = &target
            .downcast_ref::<Self>()
            .ok_or(FsError::NotSameFs)?
            .inode;
        self.inode.move_(old_name, target, new_name).await
    }

    async fn find(&self, name: &str) -> Result<Arc<dyn INode>> {
        Ok(self.find(false, name).await?)
    }

    async fn get_entry(&self, id: usize) -> Result<String> {
        self.inode.get_entry(id).await
    }

    async fn get_entry_with_metadata(&self, id: usize) -> Result<(Metadata, String)> {
        self.inode.get_entry_with_metadata(id).await
    }

    fn io_control(&self, cmd: u32, data: usize) -> Result<usize> {
        self.inode.io_control(cmd, data)
    }

    fn mmap(&self, area: MMapArea) -> Result<()> {
        self.inode.mmap(area)
    }

    fn fs(&self) -> Arc<dyn FileSystem> {
        self.vfs.clone()
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}