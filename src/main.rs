use std::{collections::HashMap, fs::OpenOptions, io};

use byteorder::{ByteOrder, LittleEndian};
use memmap2::MmapMut;

//TODO: manage the file overflow

fn main() {
    let data_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open("data.bin")
        .unwrap();

    let journal_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open("journal.bin")
        .unwrap();

    let hashmap: HashMap<u32, u32> = HashMap::<u32, u32>::with_capacity(1000);

    let data_map = unsafe { MmapMut::map_mut(&data_file) }.unwrap();
    let journal_map = unsafe { MmapMut::map_mut(&journal_file) }.unwrap();
}

type BlockID = u32;
const BLOCK_SIZE: usize = 4096;

struct MidPhase {
    data_map: MmapMut,
    journal_map: MmapMut,
    block_hashmap: HashMap<BlockID, BlockID>,
}

impl MidPhase {
    fn from_journal_phase(mut journal_phase: JournalPhase) -> io::Result<Self> {
        let mut journal_map_idx = (journal_phase.next_journal_block_id as usize) * 4096;
        for (data_map_block_id, journal_map_block_id) in journal_phase.block_hashmap.iter() {
            LittleEndian::write_u32(
                &mut journal_phase.journal_map[journal_map_idx..(journal_map_idx + 4)],
                *journal_map_block_id,
            );
            LittleEndian::write_u32(
                &mut journal_phase.journal_map[(journal_map_idx + 4)..(journal_map_idx + 8)],
                *data_map_block_id,
            );
            journal_map_idx += size_of::<u32>() * 2;
        }

        ///write nb block
        LittleEndian::write_u32(
            &mut journal_phase.journal_map[journal_map_idx..(journal_map_idx + 4)],
            journal_phase.block_hashmap.len() as u32,
        );
        //TODO set a variable to get the last u32 of file(= nb_block) - Set the file length
        //                                                            - Set a config block(idx: 0)
        //                                                            - OR other ideas
        journal_phase.journal_map.flush()?;

        Ok(Self {
            data_map: journal_phase.data_map,
            journal_map: journal_phase.journal_map,
            block_hashmap: journal_phase.block_hashmap,
        })
    }
}

struct JournalPhase {
    data_map: MmapMut,
    journal_map: MmapMut,
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

            mid_phase.data_map[block_idx_in_data_map..(block_idx_in_data_map + BLOCK_SIZE)]
                .clone_from_slice(
                    &mid_phase.journal_map
                        [block_idx_in_journal_map..(block_idx_in_journal_map + BLOCK_SIZE)],
                );
        }

        mid_phase.data_map.flush()?;

        mid_phase.block_hashmap.clear();
        Ok(Self {
            data_map: mid_phase.data_map,
            journal_map: mid_phase.journal_map,
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
                    data: &mut self.journal_map
                        [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)],
                }
            }
            None => {
                let idx_in_data_map = (block_id as usize) * BLOCK_SIZE;
                ReadBlock {
                    data_map_block_id: block_id,
                    writable: false,
                    data: &mut self.data_map[idx_in_data_map..(idx_in_data_map + BLOCK_SIZE)],
                }
            }
        }
    }

    pub fn load_write_block(&'a mut self, block_id: BlockID) -> WriteBlock<'a> {
        let journal_block_id_opt = self.block_hashmap.get(&block_id);
        match journal_block_id_opt {
            Some(journal_block_id) => {
                let idx_in_journal_map = (*journal_block_id as usize) * BLOCK_SIZE;
                WriteBlock {
                    data: &mut self.journal_map
                        [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)],
                }
            }
            None => {
                self.block_hashmap
                    .insert(block_id, self.next_journal_block_id);

                let idx_in_data_map = (block_id as usize) * BLOCK_SIZE;
                let idx_in_journal_map = (self.next_journal_block_id as usize) * BLOCK_SIZE;
                self.journal_map[idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)]
                    .copy_from_slice(
                        &self.data_map[idx_in_data_map..(idx_in_data_map + BLOCK_SIZE)],
                    );

                self.next_journal_block_id += 1;
                WriteBlock {
                    data: &mut self.journal_map
                        [idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)],
                }
            }
        }
    }

    pub fn load_write_block_from_read_block(
        &'a mut self,
        read_block: ReadBlock<'a>,
    ) -> WriteBlock<'a> {
        if read_block.writable {
            WriteBlock {
                data: read_block.data,
            }
        } else {
            self.block_hashmap
                .insert(read_block.data_map_block_id, self.next_journal_block_id);

            let idx_in_data_map = (read_block.data_map_block_id as usize) * BLOCK_SIZE;
            let idx_in_journal_map = (self.next_journal_block_id as usize) * BLOCK_SIZE;
            self.journal_map[idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)]
                .copy_from_slice(&self.data_map[idx_in_data_map..(idx_in_data_map + BLOCK_SIZE)]);

            self.next_journal_block_id += 1;
            WriteBlock {
                data: &mut self.journal_map[idx_in_journal_map..(idx_in_journal_map + BLOCK_SIZE)],
            }
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
