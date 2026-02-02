use std::{collections::HashMap, fs::OpenOptions};

use memmap2::MmapMut;

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

/*struct Trace {
    filepath: O
}

impl Trace{

}*/

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

    fn load_read_block(&'a mut self, block_id: BlockID) -> ReadBlock<'a> {
        let journal_block_id_opt = self.block_hashmap.get(&block_id);
        match journal_block_id_opt {
            Some(journal_block_id) => {
                let idx_in_journal_map = (*journal_block_id as usize) * 4096;
                ReadBlock {
                    data_map_block_id: block_id,
                    writable: true,
                    data: &mut self.journal_map[idx_in_journal_map..(idx_in_journal_map + 4096)],
                }
            }
            None => {
                let idx_in_data_map = (block_id as usize) * 4096;
                ReadBlock {
                    data_map_block_id: block_id,
                    writable: false,
                    data: &mut self.data_map[idx_in_data_map..(idx_in_data_map + 4096)],
                }
            }
        }
    }

    fn load_write_block(&'a mut self, block_id: BlockID) -> WriteBlock<'a> {
        let journal_block_id_opt = self.block_hashmap.get(&block_id);
        match journal_block_id_opt {
            Some(journal_block_id) => {
                let idx_in_journal_map = (*journal_block_id as usize) * 4096;
                WriteBlock {
                    data: &mut self.journal_map[idx_in_journal_map..(idx_in_journal_map + 4096)],
                }
            }
            None => {
                self.block_hashmap
                    .insert(block_id, self.next_journal_block_id);

                let idx_in_data_map = (block_id as usize) * 4096;
                let idx_in_journal_map = (self.next_journal_block_id as usize) * 4096;
                self.journal_map[idx_in_journal_map..(idx_in_journal_map + 4096)]
                    .copy_from_slice(&self.data_map[idx_in_data_map..(idx_in_data_map + 4096)]);

                self.next_journal_block_id += 1;
                WriteBlock {
                    data: &mut self.journal_map[idx_in_journal_map..(idx_in_journal_map + 4096)],
                }
            }
        }
    }

    fn load_write_block_from_read_block(&'a mut self, read_block: ReadBlock<'a>) -> WriteBlock<'a> {
        if read_block.writable {
            WriteBlock {
                data: read_block.data,
            }
        } else {
            self.block_hashmap
                .insert(read_block.data_map_block_id, self.next_journal_block_id);

            let idx_in_data_map = (read_block.data_map_block_id as usize) * 4096;
            let idx_in_journal_map = (self.next_journal_block_id as usize) * 4096;
            self.journal_map[idx_in_journal_map..(idx_in_journal_map + 4096)]
                .copy_from_slice(&self.data_map[idx_in_data_map..(idx_in_data_map + 4096)]);

            self.next_journal_block_id += 1;
            WriteBlock {
                data: &mut self.journal_map[idx_in_journal_map..(idx_in_journal_map + 4096)],
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
