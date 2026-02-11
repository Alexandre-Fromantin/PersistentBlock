use std::{
    cmp::min,
    fs::{File, OpenOptions},
    io,
    ops::{Deref, DerefMut},
    path::PathBuf,
    ptr::NonNull,
};

use ahash::AHashMap;
use byteorder::{ByteOrder, LittleEndian};
use memmap2::MmapMut;

pub type BlockID = u32;
pub const BLOCK_SIZE_U64: u64 = 4096;
pub const BLOCK_SIZE_USIZE: usize = BLOCK_SIZE_U64 as usize;

struct FileMapped {
    file: File,
    map: MmapMut,
    file_len: u64,
}
impl FileMapped {
    fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let map = unsafe { MmapMut::map_mut(&file) }?;

        let file_len = file.metadata()?.len();

        Ok(Self {
            file,
            map,
            file_len,
        })
    }

    fn resize(&mut self, new_size: u64) -> io::Result<()> {
        self.file.set_len(new_size)?;

        self.map = unsafe { MmapMut::map_mut(&self.file) }?;

        Ok(())
    }
}

pub struct CommitPhase {
    data: FileMapped,
    journal: FileMapped,
    journal_capacity: u32,
    block_hashmap: AHashMap<BlockID, BlockID>,
}

impl CommitPhase {
    pub fn from_journal_phase(mut journal_phase: JournalPhase) -> io::Result<Self> {
        let mut journal_map_idx = (journal_phase.next_journal_block_id as usize) * BLOCK_SIZE_USIZE;
        for (data_map_block_id, journal_map_block_id) in journal_phase.block_hashmap.iter() {
            LittleEndian::write_u32(
                &mut journal_phase.journal.map[journal_map_idx..(journal_map_idx + 4)],
                *journal_map_block_id,
            );
            LittleEndian::write_u32(
                &mut journal_phase.journal.map[(journal_map_idx + 4)..(journal_map_idx + 8)],
                *data_map_block_id,
            );
            journal_map_idx += size_of::<u32>() * 2;
        }

        //write nb block
        LittleEndian::write_u32(
            &mut journal_phase.journal.map[journal_map_idx..(journal_map_idx + 4)],
            journal_phase.block_hashmap.len() as u32,
        );
        journal_phase.journal.map.flush()?;

        let length_used = journal_phase.block_hashmap.len() as u64
            * (BLOCK_SIZE_USIZE + size_of::<u32>() * 2) as u64
            + size_of::<u32>() as u64;
        journal_phase.journal.resize(length_used)?;

        Ok(Self {
            data: journal_phase.data,
            journal: journal_phase.journal,
            journal_capacity: journal_phase.journal_capacity,
            block_hashmap: journal_phase.block_hashmap,
        })
    }
}

pub struct JournalPhase {
    data: FileMapped,
    journal: FileMapped,
    journal_capacity: u32,
    next_journal_block_id: BlockID,
    block_hashmap: AHashMap<BlockID, BlockID>,
}
impl JournalPhase {
    //on read -> if already in hash map -> read on journal block
    //           OR -> read on data map

    //on write -> if already in hash map -> write on journal block
    //           OR -> copy data block in journal block and write in journal block

    pub fn from_commit_phase(mut commit_phase: CommitPhase) -> io::Result<Self> {
        for (data_map_block_id, journal_map_block_id) in commit_phase.block_hashmap.iter() {
            let block_idx_in_data_map = (*data_map_block_id as usize) * BLOCK_SIZE_USIZE;
            let block_idx_in_journal_map = (*journal_map_block_id as usize) * BLOCK_SIZE_USIZE;

            commit_phase.data.map
                [block_idx_in_data_map..(block_idx_in_data_map + BLOCK_SIZE_USIZE)]
                .clone_from_slice(
                    &commit_phase.journal.map
                        [block_idx_in_journal_map..(block_idx_in_journal_map + BLOCK_SIZE_USIZE)],
                );
        }

        commit_phase.data.map.flush()?;

        commit_phase.block_hashmap.clear();
        Ok(Self {
            data: commit_phase.data,
            journal: commit_phase.journal,
            journal_capacity: commit_phase.journal_capacity,
            next_journal_block_id: 0,
            block_hashmap: commit_phase.block_hashmap,
        })
    }

    pub fn load_read_block(&mut self, block_id: BlockID) -> ReadBlock {
        let journal_block_id_opt = self.block_hashmap.get(&block_id);
        match journal_block_id_opt {
            Some(journal_block_id) => {
                let idx_in_journal_map = (*journal_block_id as usize) * BLOCK_SIZE_USIZE;
                ReadBlock {
                    data_map_block_id: block_id,
                    writable: true,
                    data: unsafe {
                        NonNull::new_unchecked(
                            &mut self.journal.map
                                [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE_USIZE)],
                        )
                    },
                }
            }
            None => {
                let idx_in_data_map = (block_id as usize) * BLOCK_SIZE_USIZE;
                ReadBlock {
                    data_map_block_id: block_id,
                    writable: false,
                    data: unsafe {
                        NonNull::new_unchecked(
                            &mut self.data.map
                                [idx_in_data_map..(idx_in_data_map + BLOCK_SIZE_USIZE)],
                        )
                    },
                }
            }
        }
    }

    pub fn load_write_block(&mut self, block_id: BlockID) -> io::Result<WriteBlock> {
        let journal_block_id_opt = self.block_hashmap.get(&block_id);
        match journal_block_id_opt {
            Some(journal_block_id) => {
                let idx_in_journal_map = (*journal_block_id as usize) * BLOCK_SIZE_USIZE;
                Ok(WriteBlock {
                    data: unsafe {
                        NonNull::new_unchecked(
                            &mut self.journal.map
                                [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE_USIZE)],
                        )
                    },
                })
            }
            None => {
                if self.journal_capacity <= self.block_hashmap.len() as u32 {
                    self.journal_capacity =
                        self.journal_capacity + min(self.journal_capacity / 4, 1); //capacity +25%
                    self.journal
                        .resize(self.journal_capacity as u64 * BLOCK_SIZE_U64)?;
                }
                self.block_hashmap
                    .insert(block_id, self.next_journal_block_id);

                let idx_in_data_map = (block_id as usize) * BLOCK_SIZE_USIZE;
                let idx_in_journal_map = (self.next_journal_block_id as usize) * BLOCK_SIZE_USIZE;
                self.journal.map[idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE_USIZE)]
                    .copy_from_slice(
                        &self.data.map[idx_in_data_map..(idx_in_data_map + BLOCK_SIZE_USIZE)],
                    );

                self.next_journal_block_id += 1;
                Ok(WriteBlock {
                    data: unsafe {
                        NonNull::new_unchecked(
                            &mut self.journal.map
                                [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE_USIZE)],
                        )
                    },
                })
            }
        }
    }

    pub fn load_write_block_from_read_block(
        &mut self,
        read_block: &mut ReadBlock,
    ) -> io::Result<WriteBlock> {
        if read_block.writable {
            Ok(WriteBlock {
                data: read_block.data,
            })
        } else {
            if self.journal_capacity <= self.block_hashmap.len() as u32 {
                self.journal_capacity = self.journal_capacity + min(self.journal_capacity / 4, 1); //capacity +25%
                self.journal
                    .resize(self.journal_capacity as u64 * BLOCK_SIZE_U64)?;
            }

            self.block_hashmap
                .insert(read_block.data_map_block_id, self.next_journal_block_id);

            let idx_in_data_map = (read_block.data_map_block_id as usize) * BLOCK_SIZE_USIZE;
            let idx_in_journal_map = (self.next_journal_block_id as usize) * BLOCK_SIZE_USIZE;
            self.journal.map[idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE_USIZE)]
                .copy_from_slice(
                    &self.data.map[idx_in_data_map..(idx_in_data_map + BLOCK_SIZE_USIZE)],
                );

            let journal_slice = unsafe {
                NonNull::new_unchecked(
                    &mut self.journal.map
                        [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE_USIZE)],
                )
            };
            read_block.data = journal_slice;
            read_block.writable = true;

            self.next_journal_block_id += 1;
            Ok(WriteBlock {
                data: journal_slice,
            })
        }
    }

    ///Does not work for size reduction
    /// TODO: manage size reduction
    pub fn resize(&mut self, new_number_of_block: u32) -> io::Result<()> {
        let new_size = new_number_of_block as u64 * BLOCK_SIZE_U64;
        if new_size < self.data.file_len {
            todo!("size reduction of data file, not implemented")
        }
        self.data.resize(new_size)
    }
}

pub struct ReadBlock {
    data_map_block_id: BlockID,
    writable: bool,
    data: NonNull<[u8]>,
}

impl Deref for ReadBlock {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.data.as_ref() }
    }
}

pub struct WriteBlock {
    data: NonNull<[u8]>,
}

impl Deref for WriteBlock {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.data.as_ref() }
    }
}
impl DerefMut for WriteBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.data.as_mut() }
    }
}
