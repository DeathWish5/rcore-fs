use super::*;

pub struct NullINode {
    inode_id: usize,
}

impl NullINode {
    pub fn new() -> Self {
        Self {
            inode_id: DevFS::new_inode_id(),
        }
    }
}

#[async_trait]
impl INode for NullINode {
    async fn read_at(&self, _offset: usize, _buf: &mut [u8]) -> Result<usize> {
        // read nothing
        Ok(0)
    }

    async fn write_at(&self, _offset: usize, buf: &[u8]) -> Result<usize> {
        // write to nothing
        Ok(buf.len())
    }

    fn poll(&self) -> Result<PollStatus> {
        Ok(PollStatus {
            read: true,
            write: true,
            error: false,
        })
    }

    fn metadata(&self) -> Result<Metadata> {
        Ok(Metadata {
            dev: 1,
            inode: self.inode_id,
            size: 0,
            blk_size: 0,
            blocks: 0,
            atime: Timespec { sec: 0, nsec: 0 },
            mtime: Timespec { sec: 0, nsec: 0 },
            ctime: Timespec { sec: 0, nsec: 0 },
            type_: FileType::CharDevice,
            mode: 0o666,
            nlinks: 1,
            uid: 0,
            gid: 0,
            rdev: make_rdev(1, 3),
        })
    }

    fn set_metadata(&self, _metadata: &Metadata) -> Result<()> {
        Ok(())
    }
    async fn sync_all(&self) -> Result<()> {
        Ok(())
    }
    async fn sync_data(&self) -> Result<()> {
        Ok(())
    }
    async fn resize(&self, _len: usize) -> Result<()> {
        Err(FsError::NotSupported)
    }
    async fn create(&self, _name: &str, _type_: FileType, _mode: u32) -> Result<Arc<dyn INode>> {
        Err(FsError::NotDir)
    }
    async fn unlink(&self, _name: &str) -> Result<()> {
        Err(FsError::NotDir)
    }
    async fn link(&self, _name: &str, _other: &Arc<dyn INode>) -> Result<()> {
        Err(FsError::NotDir)
    }
    async fn move_(
        &self,
        _old_name: &str,
        _target: &Arc<dyn INode>,
        _new_name: &str,
    ) -> Result<()> {
        Err(FsError::NotDir)
    }
    async fn find(&self, _name: &str) -> Result<Arc<dyn INode>> {
        Err(FsError::NotDir)
    }
    async fn get_entry(&self, _id: usize) -> Result<String> {
        Err(FsError::NotDir)
    }
    fn io_control(&self, _cmd: u32, _data: usize) -> Result<usize> {
        Err(FsError::NotSupported)
    }
    fn mmap(&self, _area: MMapArea) -> Result<()> {
        Err(FsError::NotSupported)
    }
    fn fs(&self) -> Arc<dyn FileSystem> {
        unimplemented!()
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}
