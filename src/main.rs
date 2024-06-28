use serde_json::json;
use std::io::Read;

use std::collections::HashMap;

use std::time::Instant;

use serde_json;
use serde_json::Value;
use anyhow::Error;
use clap::Parser;
use std::path::PathBuf;
use crate::io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf};
use rayon::prelude::*;
use indicatif::{ProgressBar, ProgressStyle};

use dashmap::DashMap;
use tar::Archive;
use flate2::read::GzDecoder;


pub mod s3;
pub mod io;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ArgParser {
    #[arg(long, required=true, num_args=1..)]
    input: Vec<PathBuf>,

    #[arg(long, required=true)]
    output: PathBuf,

}


fn build_pbar(num_items: usize, units: &str) -> ProgressBar {
    let mut template = String::from(units);
    template.push_str(" {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]");
    let pbar = ProgressBar::new(num_items as u64)
        .with_style(
            ProgressStyle::with_template(&template).unwrap()
        );

    pbar.inc(0);
    pbar
}


/*====================================================
=                        WORKER FUNCTION             =
====================================================*/

fn count_tokens(path: &PathBuf, counter: &DashMap<usize, usize>) -> Result<(), Error> {

    let mut cur_counter: HashMap<usize, usize> = HashMap::new();
    let tar_data = read_pathbuf_to_mem(path).unwrap().into_inner();
    let mut archive = Archive::new(tar_data);

    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        // Read the contents of the file
        let mut compressed_contents = Vec::new();
        entry.read_to_end(&mut compressed_contents).unwrap();

        // Decompress the gzip data
        let mut gz = GzDecoder::new(&compressed_contents[..]);
        let mut json_str = String::new();
        gz.read_to_string(&mut json_str).unwrap();

        // Parse the JSON
        let json: Value = serde_json::from_str(&json_str).unwrap();        

        if let Value::Array(arr) = json {
            for tok in arr {
                *cur_counter.entry(tok.as_u64().unwrap() as usize).or_insert(0) += 1;
            }
        }
    }   


    // And then finally add to global_counter
    cur_counter.into_iter()
        .for_each(|(k,v)| {
            counter.entry(k)
                .and_modify(|count| *count += v)
                .or_insert(v);
        });

    Ok(()) 
}




/*==================================================
=                       MAIN BLOCK.                =
==================================================*/

fn main() {
    let start_main = Instant::now();
    let args = ArgParser::parse();

    let paths = expand_dirs(args.input.clone(), Some(&vec!["tar"])).unwrap();
    let pbar = build_pbar(paths.len(), "Paths");

    let counter : DashMap<usize, usize> = DashMap::new();

    paths.par_iter()
        .for_each(|p| {
            count_tokens(&p, &counter).unwrap();
            pbar.inc(1);
        });

    // And then finalize
    let total_tokens: usize = counter.iter().map(|r| r.value().clone()).sum::<usize>();
    let json_map: serde_json::Value = json!(counter.iter().map(|r| (r.key().clone(), r.value().clone())).collect::<std::collections::HashMap<_, _>>());
    let json_string = serde_json::to_string(&json_map).unwrap();
    let json_bytes = json_string.as_bytes();
    write_mem_to_pathbuf(json_bytes, &args.output).unwrap();

    println!("-------------------------");
    println!("Finishing counting in {:?} seconds", start_main.elapsed().as_secs());  
    println!("Saw {:?} tokens total", total_tokens);
}
