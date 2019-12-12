extern crate jemallocator;
extern crate quick_protobuf;

mod osm_pbf;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use crossbeam_channel::{bounded, unbounded};
use crossbeam_utils::thread;
use memmap::MmapOptions;
use osm_pbf::{Blob, BlobHeader, PrimitiveBlock};
use quick_protobuf::{BytesReader, MessageRead};
use std::fs::File;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

const WORK_BOUND: usize = 4000;
const THREAD_COUNT: i64 = 4;
const OUTPUT_BUF_SIZE: i32 = 10_485_760;

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    let filename = &args[1];
    match do_processing(filename) {
        Ok(result) => println!("{}", result),
        Err(err) => println!("{}", err),
    }
}

fn do_processing(filename: &std::ffi::OsStr) -> Result<String, String> {
    let file_handle = File::open(filename).or(Err("unable to open file"))?;
    let mmap = unsafe {
        MmapOptions::new()
            .map(&file_handle)
            .or(Err("unable to mmap"))?
    };
    let bytes = &mmap[..];
    let mut reader = BytesReader::from_bytes(&bytes);
    let mut sent_messages = 0;
    let (sender, receiver) = bounded::<Blob>(WORK_BOUND);
    let (return_sender, return_received) = unbounded::<u64>();

    thread::scope(|s| {
        for _ in 0..THREAD_COUNT {
            let cloned_receiver = receiver.clone();
            let cloned_return_sender = return_sender.clone();
            s.spawn(move |_| {
                let mut buffer = Vec::with_capacity(OUTPUT_BUF_SIZE as usize);
                loop {
                    match cloned_receiver.recv() {
                        Ok(blob) => {
                            if blob.raw_size.unwrap_or(0) > OUTPUT_BUF_SIZE {
                                panic!("got a blob with too large output size!");
                            }
                            let size = handle_block(&blob, &mut buffer);
                            buffer.clear();
                            cloned_return_sender
                                .send(size as u64)
                                .expect("failed to send size result");
                        }
                        Err(_e) => break,
                    }
                }
            });
        }

        loop {
            let header_size = match reader.read_fixed32(bytes).map(|value| {
                let mut buf = [0; 4];
                LittleEndian::write_u32(&mut buf, value);
                BigEndian::read_u32(&buf)
            }) {
                Ok(size) if size > 64 * 1024 => {
                    return Err("invalid data, blob too large".to_string())
                }
                Ok(size) => size,
                Err(_e) => break,
            } as usize;

            let blob_header = reader
                .read_message_by_len::<BlobHeader>(&bytes, header_size)
                .expect("failed to read blob header");

            let blob = reader
                .read_message_by_len::<Blob>(bytes, blob_header.datasize as usize)
                .expect("failed to read blob");

            if blob_header.type_pb == "OSMData" {
                sent_messages += 1;
                sender.send(blob).expect("failed to send blob");
            }
        }

        drop(sender);

        let mut received_messages = 0;
        let mut sizes = 0;
        while received_messages < sent_messages {
            sizes += return_received.recv().unwrap();
            received_messages += 1;
        }
        Ok(format!("jeee string tables total size {}", sizes))
    })
    .unwrap()
}

fn handle_block(blob: &Blob, buffer: &mut Vec<u8>) -> usize {
    let zlib_data_ref = blob.zlib_data.as_ref();
    let tried_block = if blob.raw.is_some() {
        let bytes = blob.raw.as_ref().unwrap();
        let mut reader = BytesReader::from_bytes(&bytes);
        Some(
            PrimitiveBlock::from_reader(&mut reader, &bytes)
                .expect("failed to read primitive block"),
        )
    } else if zlib_data_ref.is_some() {
        use flate2::{Decompress, FlushDecompress};

        let mut decompress = Decompress::new(true);

        /*let r = io::Cursor::new(zlib_data_ref.unwrap());
        let mut zr = ZlibDecoder::new(r);

        zr.read_to_end(buffer)
            .expect("failed to read to end gzipped"); */

        decompress
            .decompress_vec(&zlib_data_ref.unwrap(), buffer, FlushDecompress::Finish)
            .expect("error decompressing");
        let mut reader = BytesReader::from_bytes(&buffer);
        Some(
            PrimitiveBlock::from_reader(&mut reader, &buffer)
                .expect("failed to read gzipped primitive block"),
        )
    } else {
        None
    };
    let block = tried_block.unwrap();
    block.stringtable.s.len()
}
/*
fn parse_block(blob: &Blob) -> Result<&PrimitiveBlock, String> {
    if blob.raw.is_some() {
        let bytes = &blob.raw.unwrap();
        let mut reader = BytesReader::from_bytes(bytes);
        &PrimitiveBlock::from_reader(&mut reader, bytes)
    /*} else if blob.zlib_data.is_some() {
    use flate2::read::ZlibDecoder;
    let r = io::Cursor::new(blob.zlib_data.unwrap());
    let mut zr = ZlibDecoder::new(r);
    let mut reader = BytesReader::from_reader(zr);
    PrimitiveBlock::from_reader(reader, zr)*/
    } else {
        Err("unsupported data")
    }
}
*/
