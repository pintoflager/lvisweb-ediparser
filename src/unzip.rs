use zip::ZipArchive;
use log::{debug, error, info};
use anyhow::{anyhow, bail, Result};
use std::io::copy;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::fs::{create_dir_all, remove_file, set_permissions, File, Permissions};


use crate::edi::EDI_DIR_NAME;
use super::config::Config;
use super::files::file_to_edi_utf8;

pub fn unzip_from(archives: Vec<PathBuf>, config: &Config) -> Result<Vec<(PathBuf, String)>> {
    // Unzip and save files with randomized names into the sources dir.
    let mut edi_dir = config.dir.to_owned();
    edi_dir.push(EDI_DIR_NAME);

    create_dir_all(&edi_dir).map_err(|e|anyhow!("Failed to create edi dir: {}", e))?;
    let mut edi_files = vec![];

    for a in archives {
        let (f, n) = match unzip_handler(&a, &edi_dir) {
            Ok(t) => {
                if let Err(e) = remove_file(&a) {
                    bail!("Failed to delete obsolete zip archive {:?}: \
                        {}", a, e)
                }

                t
            },
            Err(e) => {
                error!("Failed to unzip file {:?} ({}), skipping...", a, e);

                if let Err(e) = remove_file(&a) {
                    bail!("Failed to delete non unzippable file {:?}: {}", a, e)
                }

                continue;
            }
        };

        match file_to_edi_utf8(&f, &edi_dir, None) {
            Ok(p) => edi_files.push((p, n)),
            Err(e) => {
                error!("Failed to convert source file '{}' ({:?}) \
                    to utf-8 format: {}", n, f, e);

                if let Err(e) = remove_file(&f) {
                    bail!("Failed to delete non utf-8 convertable file {:?}: {}", f, e)
                }

                continue;
            }
        }
    }

    Ok(edi_files)
}


pub fn unzip_handler(archive_file: &PathBuf, unzip_dir: &PathBuf) -> Result<(PathBuf, String)> {
    let file = File::open(archive_file)?;

    let mut extracted_file_path = unzip_dir.to_owned();
    let mut archive = ZipArchive::new(file)?;

    // We're expecting zip archives containing just one file.
    if archive.is_empty() || archive.len() > 1 {
        bail!("Zip archive {:?} has unexpected amount of files in it ({}). One \
            file expected.", archive_file, archive.len())
    }

    let mut archived_file = archive.by_index(0)?;
    let extracted_file_name = match archived_file.enclosed_name() {
        Some(p) => match p.to_str() {
            Some(s) => s.to_owned(),
            None => bail!("Unable to read zipped file name ({:?}) to string", p),
        },
        None => bail!("Unable to read file name from zip file {:?}", archive_file),
    };

    debug!("Trying to unzip archive containing EDI file {}...", &extracted_file_name);

    extracted_file_path.push(&extracted_file_name);

    // Directories. Should never be the case here but left as a reminder.
    if (*archived_file.name()).ends_with('/') {
        info!("File {} extracted to {:?}", 0, extracted_file_path);
        create_dir_all(&extracted_file_path)?;
    } 
    else {
        debug!(
            "File {} extracted to {:?} ({} bytes)",
            &extracted_file_name,
            &extracted_file_path,
            archived_file.size()
        );

        // Make sure target dir for unzip action exists
        if let Some(p) = extracted_file_path.parent() {
            if ! p.exists() {
                create_dir_all(p)?;
            }
        }

        let mut outfile = File::create(&extracted_file_path)?;
        copy(&mut archived_file, &mut outfile)?;
    }

    // Get and Set permissions
    set_permissions(&extracted_file_path, Permissions::from_mode(0o755))?;

    Ok((extracted_file_path, extracted_file_name))
}
