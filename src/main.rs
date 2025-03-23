use polars::prelude::*;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 3 {
        if args[1] == "csv" {
            let file = std::fs::File::open(&args[2]);
            match file {
                Ok(_f) => {
                    // READ PARQUET FILE
                    let local = std::env::current_dir().unwrap();
                    let parquetpath = format!("{}/{}", local.display(), &args[2]);
                    let parquetfile = std::fs::File::open(parquetpath).unwrap();
                    let mut df = ParquetReader::new(parquetfile).finish().unwrap();
                    // WRITE CSV FILE
                    let filename = &args[2][..args[2].len()-8];
                    let csvpath = format!("{}/{}.csv", local.display(), filename);
                    let csvfile = std::fs::File::create(csvpath).unwrap();
                    CsvWriter::new(csvfile).finish(&mut df).unwrap();
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                }
            }
        } else if args[1] == "parquet" {
            let file = std::fs::File::open(&args[2]);
            match file {
                Ok(_f) => {
                    // READ CSV FILE
                    let local = std::env::current_dir().unwrap();
                    let csvpath = format!("{}/{}", local.display(), &args[2]);
                    let csvfile = std::path::PathBuf::from(csvpath);
                    let mut df = CsvReadOptions::default().with_has_header(true).try_into_reader_with_file_path(Some(csvfile)).unwrap().finish().unwrap();
                    // WRITE PARQUET FILE
                    let filename = &args[2][..args[2].len()-4];
                    let parquetpath = format!("{}/{}.parquet", local.display(), filename);
                    let parquetfile = std::fs::File::create(parquetpath).unwrap();
                    ParquetWriter::new(parquetfile).with_compression(ParquetCompression::Snappy).finish(&mut df).unwrap();
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                }
            }
        } else {
            println!("Commands: csv, parquet");
            println!(" - parquet2csv csv <data.parquet>");
            println!(" - parquet2csv parquet <data.csv>");
        }
    } else {
        println!("Commands: csv, parquet");
        println!(" - parquet2csv csv <data.parquet>");
        println!(" - parquet2csv parquet <data.csv>");
    }
}