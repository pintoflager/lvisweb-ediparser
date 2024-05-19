// 3rd party libs
use std::fs::{File, write, create_dir_all, rename, remove_file, read_dir};
use std::io::{prelude::*, BufReader};
use std::path::PathBuf;
use log::debug;
use encoding::all::UTF_8;
use anyhow::{anyhow, bail, Result};
use encoding::{Encoding, DecoderTrap};
use encoding::all::ISO_8859_1;

use crate::config::Config;
use crate::edi::EDI_DIR_NAME;

use super::edi::{EdiOwnership, EdiHeader};


pub fn move_file(from: &PathBuf, target_dir: &PathBuf, subdir: &str, name: &str) {
    let mut path = target_dir.to_owned();
    path.push(subdir);

    if let Err(e) = create_dir_all(&path) {
        panic!("Failed to create supplier '{}' dir to {:?}: {}",
            subdir, path, e)
    }

    let mut file = path.to_owned();
    file.push(name);

    if let Err(e) = rename(from, &file) {
        panic!("Failed to move {:?} to supplier dir {:?}: {}", from, file, e)
    }
}

// Convert file to utf-8 and make sure it follows the same known pattern
pub fn file_to_edi_utf8(from: &PathBuf, to_dir: &PathBuf, new_name: Option<String>) -> Result<PathBuf> {
    // Try to decide where to save the file.
    let name = match new_name {
        Some(p) => p,
        None => match from.file_name() {
            Some(o) => o.to_string_lossy().into_owned(),
            None => bail!("Unable to read filename from path to utf-8 \
                convertable file")
        },
    };

    let mut to = to_dir.to_owned();
    to.push(name);

    // Read the full file into buffer and try to decode it to utf8.
    let mut buf = vec![];
    File::open(&from)?.read_to_end(&mut buf)?;

    if UTF_8.decode(&buf, DecoderTrap::Strict).is_ok() {
        debug!("File decodes as utf-8, surprising. Moving along...");

        if let Err(e) = rename(from, &to) {
            bail!("Failed to move utf-8 file: {}", e)
        }

        return edifile_cleanup(to)
    }

    match ISO_8859_1.decode(&buf, DecoderTrap::Strict) {
        Ok(s) => write(&to, s.as_bytes())?,
        Err(e) => bail!("Well fuck the decoder then: {}", e),
    }

    edifile_cleanup(to)
}

pub fn edi_file_imported(config: &Config, path: &PathBuf, ownership: EdiOwnership) -> Result<bool> {
    // Read title from the new file.
    let new_edi_file = File::open(path)?;
    let header = EdiHeader::read(path)?;
    
    // Expecting CONF_DIR/sellers/ID or CONF_DIR/sellers/ID/buyers/ID
    let (mut homedir, party) = match ownership {
        EdiOwnership::Seller => match header.seller {
            Some(s) => {
                let mut dir = s.party_dir(config)?;
                dir.push(&s.id);

                (dir, s)
            },
            None => bail!("Source file ownership set to seller but title says naaaay"),
        },
        EdiOwnership::Buyer => match header.buyer {
            Some(b) => match header.seller {
                Some(s) => {
                    let mut dir = s.party_dir(config)?;
                    dir.push(&s.id);
                    dir.push("buyers");
                    dir.push(&b.id);

                    (dir, b)
                },
                None => bail!("EDI file header has buyer reference but not seller"),
            },
            None => bail!("Source file ownership set to buyer but title says naaaay"),
        },
        _ => bail!("Please use this only for comparisons where you know the \
            ownership type of the file.")
    };

    // Add 'edi' tail to home path
    homedir.push(EDI_DIR_NAME);

    // If the directory doesn't exist we can skip the comparison
    if !homedir.is_dir() {
        return Ok(false)
    }
    
    // Search existing EDI files with a matching header
    let mut edi_files = vec![];

    for r in read_dir(&homedir)? {
        let e = r?;

        // Who the hell created a directory or symlink here...
        if !e.path().is_file() {
            continue;
        }

        let header = EdiHeader::read(&e.path())?;

        match ownership {
            EdiOwnership::Seller => if let Some(t) = header.seller {
                if t.eq(&party) {
                    edi_files.push(e.path());
                }
            },
            EdiOwnership::Buyer => if let Some(t) = header.buyer {
                if t.eq(&party) {
                    edi_files.push(e.path());
                }
            },
            _ => (),
        }
    }

    // If above loop gave us nothing we can skip the comparison
    if edi_files.is_empty() {
        return Ok(false)
    }
    
    // Loop through the files and compare them
    for f in edi_files {
        let f2 = File::open(f)?;
    
        // Compare filesize
        if new_edi_file.metadata()?.len() != f2.metadata()?.len() {
            continue;
        }
    
        // Read both files to bytes
        let f1 = BufReader::new(new_edi_file);
        let f2 = BufReader::new(f2);
    
        // Do a byte to byte comparison of the two files
        for (b1, b2) in f1.bytes().zip(f2.bytes()) {
            if b1? != b2? {
                continue;
            }
        }
    
        // Same file, get rid of the newcomer and notify skip
        remove_file(path)?;
    
        return Ok(true);
    }

    Ok(false)
}

fn edifile_cleanup(path: PathBuf) -> Result<PathBuf> {
    // Open the given file for reading
    let f = File::open(&path)?;
    let reader = BufReader::new(f);

    // Create new file for writing
    let mut new_path = path.to_owned();
    let name = format!("{}.clean", path.file_name().unwrap().to_str().unwrap());
    new_path.set_file_name(&name);

    let mut new_file = File::create(&new_path)?;


    // Skip headers with iterator.
    for l in reader.lines() {
        let s = l.map_err(|e| anyhow!("Unable to read line for cleanup from EDI file: {}", e))?;

        // Skip empty lines
        if !s.is_empty() {
            writeln!(new_file, "{}", s)?;
        }
    }

    // Validate EDI header
    EdiHeader::read(&new_path).map_err(|e|anyhow!("Cleaned up EDI file has invalid header: {}", e))?;
    
    std::fs::rename(&new_path, &path)
        .map_err(|e|anyhow!("Unable to copy example config file: {}", e))?;

    Ok(path)
}