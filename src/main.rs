extern crate jemallocator;
extern crate num_cpus;
extern crate quick_protobuf;

mod osm_pbf;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use crossbeam_channel::{bounded, unbounded};
use crossbeam_utils::thread;
use memmap::MmapOptions;
use osm_pbf::{Blob, BlobHeader, DenseInfo, DenseNodes, Info, Node, PrimitiveBlock, Relation, Way};
use quick_protobuf::{BytesReader, MessageRead};
use std::cmp::{max, min};
use std::fs::File;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

const WORK_BOUND: usize = 4000;
const MAX_COMPRESSED_BLOB_SIZE: i32 = 64 * 1024;
const MAX_DECOMPRESSED_BLOB_SIZE: i32 = 32 * 1024 * 1024;

#[derive(Debug)]
struct OsmStats {
    timestamp_min: i64,
    timestamp_max: i64,
    nodes: u64,
    ways: u64,
    relations: u64,
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    let filename = &args[1];
    match do_processing(filename, num_cpus::get()) {
        Ok(result) => println!("{}", result),
        Err(err) => println!("{}", err),
    }
}

fn do_processing(filename: &std::ffi::OsStr, thread_count: usize) -> Result<String, String> {
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
    let (return_sender, return_received) = unbounded::<OsmStats>();

    thread::scope(|s| {
        for _ in 0..thread_count {
            let cloned_receiver = receiver.clone();
            let cloned_return_sender = return_sender.clone();
            s.spawn(move |_| {
                let mut buffer = Vec::with_capacity(MAX_DECOMPRESSED_BLOB_SIZE as usize);
                let mut stats = empty_osm_stats();
                loop {
                    match cloned_receiver.recv() {
                        Ok(blob) => {
                            handle_block(&mut stats, &blob, &mut buffer);
                            buffer.clear();
                        }
                        Err(_e) => break,
                    }
                }
                cloned_return_sender
                    .send(stats)
                    .expect("failed to send size result");
            });
        }

        loop {
            let header_size = match reader.read_sfixed32(bytes).map(|value| {
                let mut buf = [0; 4];
                LittleEndian::write_i32(&mut buf, value);
                BigEndian::read_i32(&buf)
            }) {
                Ok(size) if size > MAX_COMPRESSED_BLOB_SIZE => {
                    return Err("invalid data, compressed blob too large".to_string())
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

            if blob.raw_size.unwrap_or(0) > MAX_DECOMPRESSED_BLOB_SIZE {
                return Err("invalid data, uncompressed blob too large".to_string());
            }

            if blob_header.type_pb == "OSMData" {
                sent_messages += 1;
                sender.send(blob).expect("failed to send blob");
            }
        }

        drop(sender);

        let mut received_messages = 0;
        let mut osm_stats = empty_osm_stats();
        while received_messages < thread_count {
            let worker_stats = return_received.recv().unwrap();
            osm_stats.nodes += worker_stats.nodes;
            osm_stats.ways += worker_stats.ways;
            osm_stats.relations += worker_stats.relations;
            osm_stats.timestamp_max = max(osm_stats.timestamp_max, worker_stats.timestamp_max);
            osm_stats.timestamp_min = min(osm_stats.timestamp_min, worker_stats.timestamp_min);
            received_messages += 1;
        }
        Ok(format!("{:#?}", osm_stats))
    })
    .unwrap()
}

fn handle_block(mut osm_stats: &mut OsmStats, blob: &Blob, buffer: &mut Vec<u8>) {
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
    handle_primitive_block(&mut osm_stats, &block);
}

fn handle_primitive_block(mut osm_stats: &mut OsmStats, block: &PrimitiveBlock) {
    let date_granularity = block.date_granularity as i64;
    for primitive in &block.primitivegroup {
        for node in &primitive.nodes {
            handle_node(&mut osm_stats, &node, date_granularity);
        }
        for way in &primitive.ways {
            handle_way(&mut osm_stats, &way, date_granularity);
        }
        for relation in &primitive.relations {
            handle_relation(&mut osm_stats, &relation, date_granularity);
        }
    }
}

fn handle_node(mut osm_stats: &mut OsmStats, node: &Node, date_granularity: i64) {
    osm_stats.nodes += 1;
    if let Some(info) = &node.info {
        handle_info(&mut osm_stats, &info, date_granularity)
    }
}

fn handle_way(mut osm_stats: &mut OsmStats, way: &Way, date_granularity: i64) {
    osm_stats.ways += 1;
    if let Some(info) = &way.info {
        handle_info(&mut osm_stats, &info, date_granularity)
    }
}

fn handle_relation(mut osm_stats: &mut OsmStats, relation: &Relation, date_granularity: i64) {
    osm_stats.relations += 1;
    if let Some(info) = &relation.info {
        handle_info(&mut osm_stats, &info, date_granularity)
    }
}

fn handle_info(osm_stats: &mut OsmStats, info: &Info, date_granularity: i64) {
    if let Some(timestamp) = info.timestamp {
        let millisec_stamp = timestamp * date_granularity;
        if millisec_stamp < osm_stats.timestamp_min {
            osm_stats.timestamp_min = millisec_stamp
        }
        if millisec_stamp > osm_stats.timestamp_max {
            osm_stats.timestamp_max = millisec_stamp
        }
    }
}

fn empty_osm_stats() -> OsmStats {
    OsmStats {
        nodes: 0,
        relations: 0,
        timestamp_max: std::i64::MIN,
        timestamp_min: std::i64::MAX,
        ways: 0,
    }
}
