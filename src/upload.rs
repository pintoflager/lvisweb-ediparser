use std::path::PathBuf;
use std::fs::{create_dir_all, remove_file, read_dir};

use anyhow::{anyhow, bail, Result};
use log::{error, warn};
use rand::distributions::{Alphanumeric, DistString};

use crate::edi::{EDI_DIR_NAME, UPLOAD_DIR_NAME};
use crate::files::file_to_edi_utf8;
use crate::unzip::unzip_handler;
use crate::config::Config;


pub fn read_uploads(config: &Config) -> Result<Vec<(PathBuf, String)>> {
    // Create uploads dir in case it doesn't exist
    let mut uploads_dir = config.dir.to_owned();
    uploads_dir.push(UPLOAD_DIR_NAME);

    create_dir_all(&uploads_dir).map_err(|e|anyhow!("Failed to create uploads dir: {}", e))?;

    let mut edi_dir = config.dir.to_owned();
    edi_dir.push(EDI_DIR_NAME);

    let mut edi_files = vec![];

    for p in read_dir(uploads_dir)? {
        let n = p?;
        let mut path = n.path();
        let mut name: String = n.file_name().to_string_lossy().into();

        if path.is_dir() {
            warn!("Uploads dir has unexpected subdirectory '{}'", &name);
            continue;
        }

        // Handle uploaded zip files
        if name.ends_with(".zip") {
            match unzip_handler(&path, &edi_dir) {
                Ok(t) => {
                    if let Err(e) = remove_file(&path) {
                        bail!("Failed to delete obsolete zip archive {:?}: \
                            {}", path, e)
                    }
    
                    path = t.0;
                    name = t.1;
                },
                Err(e) => {
                    error!("Failed to unzip uploaded file {:?} ({}), skipping...", path, e);
    
                    if let Err(e) = remove_file(&path) {
                        bail!("Failed to delete non unzippable uploaded file {:?}: {}", path, e)
                    }
    
                    continue;
                }
            };
        }

        let randy = Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
        let rename = format!("{}-{}", randy, &name);
        
        match file_to_edi_utf8(&path, &edi_dir, Some(rename.to_owned())) {
            Ok(p) => edi_files.push((p, rename)),
            Err(e) => {
                warn!("Failed to convert source file '{}' ({:?}) to utf-8 \
                    format: {}", &name, path, e);

                if let Err(e) = remove_file(&path) {
                    bail!("Failed to delete non utf-8 convertable file {:?}: {}", path, e)
                }

                continue;
            }
        }
    }

    Ok(edi_files)
}
