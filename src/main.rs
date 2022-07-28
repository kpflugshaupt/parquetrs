use std::env;
use std::fs::File;
use std::io::BufReader;

use anyhow::{Context, Result};
use parquet2::{metadata, read as parquet};

fn parquet_metadata(path: &str) -> Result<metadata::FileMetaData> {
    let mut reader = BufReader::new(
        File::open(&path).with_context(|| format!("could not open file '{}'", &path))?,
    );
    let metadata = parquet::read_metadata(&mut reader)
        .with_context(|| format!("could not read Parquet metadata from file '{}'", &path));
    metadata
}

fn main() -> Result<()> {
    // get Parquet file path from 1st argument
    let path = env::args()
        .skip(1)
        .next()
        .ok_or(anyhow::anyhow!("no path supplied"))?;

    // read metadata from file
    let metadata = parquet_metadata(&path)?;

    // Show some file attributes
    println!("File '{}' has {} rows", &path, metadata.num_rows);
    for column_metadata in metadata.row_groups[0].columns() {
        println!("---------");
        println!(
            "column: '{}'",
            column_metadata.descriptor().path_in_schema[0]
        );
        println!("type: {:#?}", column_metadata.descriptor().base_type);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fail_on_non_existent_file() {
        let md = parquet_metadata("this_path_does_not_exist");
        assert!(md.is_err());
        let error_txt = md.err().unwrap().to_string();
        assert_eq!(error_txt, "could not open file 'this_path_does_not_exist'");
    }

    #[test]
    fn fail_on_non_parquet_file() {
        let md = parquet_metadata("Cargo.toml");
        assert!(md.is_err());
        let error_txt = md.err().unwrap().to_string();
        assert_eq!(
            error_txt,
            "could not read Parquet metadata from file 'Cargo.toml'"
        );
    }

    #[test]
    fn succeed_on_valid_file() {
        let md = parquet_metadata("population_ratios.parquet");
        assert!(md.is_ok());
        let row_count = md.unwrap().num_rows;
        assert_eq!(row_count, 426);
    }
}
