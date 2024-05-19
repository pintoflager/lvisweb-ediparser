mod download;
mod utils;
mod config;
mod db;
mod unzip;
mod files;
mod edi;
mod upload;
mod search;

use std::fs::{create_dir_all, read_dir, File};
use std::path::PathBuf;
use std::process::exit;
use log::{debug, error, info};

use download::bulk_download;
use config::Config;
use unzip::unzip_from;
use edi::{EdiType, DOWNLOAD_DIR_NAME};
use upload::read_uploads;

use crate::search::search_index_builder;


fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let config = match Config::new() {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to read config file: {}", e);
            std::process::exit(1);
        }
    };

    let (mut db_sellers, mut db_buyers) = match db::init(&config) {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to initialize database: {}", e);
            exit(1);
        }
    };

    // Start pulling EDI source files defined for each seller
    let mut downloads_dir = config.dir.to_owned();
    downloads_dir.push(DOWNLOAD_DIR_NAME);
    
    if let Err(e) = create_dir_all(&downloads_dir) {
        error!("Failed to create downloads dir: {}", e);
        exit(1);
    }

    // If we have content in downloads dir lets process that before downloading more
    let downloaded_files = match read_dir(&downloads_dir) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to read downloads dir: {}", e);
            exit(1);
        }
    };

    let mut archives = downloaded_files
        .into_iter().map(|e|e.unwrap().path())
        .collect::<Vec<PathBuf>>();

    // Empty dir means we have nothing left to process from previous runs, pull EDI content
    if archives.is_empty() {
        match bulk_download(&config, &downloads_dir) {
            Ok(v) => archives.extend(v),
            Err(e) => {
                error!("Failed to download zip archives: {}", e);
                exit(1);
            },
        }
    }

    let edi_files = match unzip_from(archives, &config) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to unzip downloaded files: {}", e);
            exit(1);
        }
    };

    // Keep file log for debugging
    let mut log_path = config.dir.to_owned();
    log_path.push("import.log");
    
    // Open log file for writing
    let mut log = File::create(&log_path).unwrap();
    

    // Process downloaded EDI files
    let mut build_search_index = false;

    for (path, filename) in edi_files {
        // Search index updating is pointless without new products.
        match EdiType::file_import(&path, &filename, &config, &mut db_sellers, &mut db_buyers, &mut log) {
            Ok(t) => match t {
                EdiType::Product(b) => {
                    if !build_search_index && b {
                        build_search_index = true;
                    }
                },
                _ => (),
            },
            Err(e) => {
                error!("Failed to process EDI file '{}' {:?}: {}", filename, path, e);
                exit(1)
            }
        }
    }

    // Read and prepare upload dir files
    let edi_files = match read_uploads(&config) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to process uploads: {}", e);
            exit(1);
        }
    };

    // Process uploaded EDI files
    for (path, name) in edi_files {
        match EdiType::file_import(&path, &name, &config, &mut db_sellers, &mut db_buyers, &mut log) {
            Ok(t) => match t {
                EdiType::Discount(b) => {
                    if b {
                        info!("Updated discounts of {} from uploads", name);
                    }
                },
                _ => (),
            },
            Err(e) => {
                error!("Failed to process EDI file '{}' {:?}: {}", name, path, e);
                exit(1)
            }
        }
    }

    // Build search indexes for each product group
    if build_search_index {
        debug!("Building search indexes...");

        if let Err(e) = search_index_builder(&config, &mut db_sellers) {
            error!("Failed to update search index: {}", e);
            exit(1)
        }
    }
}
