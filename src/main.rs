use anyhow::{Context, Result};
use parquet2::read::read_metadata;
use std::env;
use std::fs::File;

fn parquet_metadata(path: &str) -> Result<parquet2::metadata::FileMetaData> {
    let mut reader =
        File::open(&path).with_context(|| format!("could not open file '{}'", &path))?;
    let metadata = read_metadata(&mut reader)
        .with_context(|| format!("could not read Parquet metadata from file '{}'", &path));
    metadata
}

fn main() -> Result<()> {
    // get Parquet file path from 1st argument
    let path = env::args()
        .skip(1)
        .next()
        .ok_or(anyhow::anyhow!("no path supplied"))?;

    let metadata = parquet_metadata(&path)?;
    println!("{:#?}", metadata);
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
