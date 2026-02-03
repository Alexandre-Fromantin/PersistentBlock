use std::{
    cmp::min,
    collections::HashMap,
    fs::{File, OpenOptions},
    io,
    path::PathBuf,
};

use byteorder::{ByteOrder, LittleEndian};
use memmap2::MmapMut;

fn main() {}

type BlockID = u32;
const BLOCK_SIZE: usize = 4096;

struct FileMapped {
    file: File,
    map: MmapMut,
    file_len: u64,
}
impl FileMapped {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let map = unsafe { MmapMut::map_mut(&file) }?;

        let file_len = file.metadata()?.len();

        Ok(Self {
            file,
            map,
            file_len,
        })
    }

    pub fn resize(&mut self, new_size: u64) -> io::Result<()> {
        self.file.set_len(new_size)?;

        self.map = unsafe { MmapMut::map_mut(&self.file) }?;

        Ok(())
    }
}

struct MidPhase {
    data: FileMapped,
    journal: FileMapped,
    block_hashmap: HashMap<BlockID, BlockID>,
}

impl MidPhase {
    fn from_journal_phase(mut journal_phase: JournalPhase) -> io::Result<Self> {
        let mut journal_map_idx = (journal_phase.next_journal_block_id as usize) * 4096;
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

        ///write nb block
        LittleEndian::write_u32(
            &mut journal_phase.journal.map[journal_map_idx..(journal_map_idx + 4)],
            journal_phase.block_hashmap.len() as u32,
        );
        journal_phase.journal.map.flush()?;

        let length_used = journal_phase.block_hashmap.len() as u64
            * (4096 + size_of::<u32>() * 2) as u64
            + size_of::<u32>() as u64;
        journal_phase.journal.resize(length_used)?;

        Ok(Self {
            data: journal_phase.data,
            journal: journal_phase.journal,
            block_hashmap: journal_phase.block_hashmap,
        })
    }
}

struct JournalPhase {
    data: FileMapped,
    journal: FileMapped,
    journal_capacity: u32,
    next_journal_block_id: BlockID,
    block_hashmap: HashMap<BlockID, BlockID>,
}
impl<'a> JournalPhase {
    //on read -> if already in hash map -> read on journal block
    //           OR -> read on data map

    //on write -> if already in hash map -> write on journal block
    //           OR -> copy data block in journal block and write in journal block

    pub fn from_mid_phase(mut mid_phase: MidPhase) -> io::Result<Self> {
        for (data_map_block_id, journal_map_block_id) in mid_phase.block_hashmap.iter() {
            let block_idx_in_data_map = (*data_map_block_id as usize) * BLOCK_SIZE;
            let block_idx_in_journal_map = (*journal_map_block_id as usize) * BLOCK_SIZE;

            mid_phase.data.map[block_idx_in_data_map..(block_idx_in_data_map + BLOCK_SIZE)]
                .clone_from_slice(
                    &mid_phase.journal.map
                        [block_idx_in_journal_map..(block_idx_in_journal_map + BLOCK_SIZE)],
                );
        }

        mid_phase.data.map.flush()?;

        mid_phase.block_hashmap.clear();
        Ok(Self {
            data: mid_phase.data,
            journal: mid_phase.journal,
            next_journal_block_id: 0,
            block_hashmap: mid_phase.block_hashmap,
        })
    }

    pub fn load_read_block(&'a mut self, block_id: BlockID) -> ReadBlock<'a> {
        let journal_block_id_opt = self.block_hashmap.get(&block_id);
        match journal_block_id_opt {
            Some(journal_block_id) => {
                let idx_in_journal_map = (*journal_block_id as usize) * BLOCK_SIZE;
                ReadBlock {
                    data_map_block_id: block_id,
                    writable: true,
                    data: &mut self.journal.map
                        [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)],
                }
            }
            None => {
                let idx_in_data_map = (block_id as usize) * BLOCK_SIZE;
                ReadBlock {
                    data_map_block_id: block_id,
                    writable: false,
                    data: &mut self.data.map[idx_in_data_map..(idx_in_data_map + BLOCK_SIZE)],
                }
            }
        }
    }

    pub fn load_write_block(&'a mut self, block_id: BlockID) -> io::Result<WriteBlock<'a>> {
        let journal_block_id_opt = self.block_hashmap.get(&block_id);
        match journal_block_id_opt {
            Some(journal_block_id) => {
                let idx_in_journal_map = (*journal_block_id as usize) * BLOCK_SIZE;
                Ok(WriteBlock {
                    data: &mut self.journal.map
                        [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)],
                })
            }
            None => {
                if self.journal_capacity <= self.block_hashmap.len() as u32 {
                    self.journal_capacity =
                        self.journal_capacity + min(self.journal_capacity / 4, 1); //capacity +25%
                    self.journal.resize(self.journal_capacity as u64 * 4096)?;
                }
                self.block_hashmap
                    .insert(block_id, self.next_journal_block_id);

                let idx_in_data_map = (block_id as usize) * BLOCK_SIZE;
                let idx_in_journal_map = (self.next_journal_block_id as usize) * BLOCK_SIZE;
                self.journal.map[idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)]
                    .copy_from_slice(
                        &self.data.map[idx_in_data_map..(idx_in_data_map + BLOCK_SIZE)],
                    );

                self.next_journal_block_id += 1;
                Ok(WriteBlock {
                    data: &mut self.journal.map
                        [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)],
                })
            }
        }
    }

    pub fn load_write_block_from_read_block(
        &'a mut self,
        read_block: ReadBlock<'a>,
    ) -> io::Result<WriteBlock<'a>> {
        if read_block.writable {
            Ok(WriteBlock {
                data: read_block.data,
            })
        } else {
            if self.journal_capacity <= self.block_hashmap.len() as u32 {
                self.journal_capacity = self.journal_capacity + min(self.journal_capacity / 4, 1); //capacity +25%
                self.journal.resize(self.journal_capacity as u64 * 4096)?;
            }

            self.block_hashmap
                .insert(read_block.data_map_block_id, self.next_journal_block_id);

            let idx_in_data_map = (read_block.data_map_block_id as usize) * BLOCK_SIZE;
            let idx_in_journal_map = (self.next_journal_block_id as usize) * BLOCK_SIZE;
            self.journal.map[idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)]
                .copy_from_slice(&self.data.map[idx_in_data_map..(idx_in_data_map + BLOCK_SIZE)]);

            self.next_journal_block_id += 1;
            Ok(WriteBlock {
                data: &mut self.journal.map[idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)],
            })
        }
    }
}

struct ReadBlock<'a> {
    data_map_block_id: BlockID,
    writable: bool,
    data: &'a mut [u8],
}

struct WriteBlock<'a> {
    data: &'a mut [u8],
}
