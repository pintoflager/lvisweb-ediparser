mod header;
mod products;
mod prices;
mod discounts;

use std::fs::File;
use std::io::Write;
use std::{fs::remove_file, path::PathBuf, str::Chars};
use anyhow::{anyhow, bail, Result};
use log::{debug, error, info, warn};
use rusqlite::Connection;
use serde::{Serialize, Deserialize};

pub use header::{EdiOwnership, EdiHeader};
pub use discounts::{is_discount_file, discounts_writer};

use crate::config::Config;
use crate::db::{query_discount_groups, query_price_groups};
use crate::files::{move_file, edi_file_imported};
use self::prices::{is_price_file, prices_writer};
use self::products::{is_product_file, products_writer};

pub const EDI_DIR_NAME: &str = "edi";
pub const UPLOAD_DIR_NAME: &str = "uploads";
pub const DOWNLOAD_DIR_NAME: &str = "downloads";


#[derive(Debug, Clone, Serialize, Deserialize)]
struct EdiDate {
    #[serde(rename = "y")]
    year: String,
    #[serde(rename = "m")]
    month: String,
    #[serde(rename = "d")]
    day: String,
}

impl EdiDate {
    fn new() -> Self {
        Self { year: String::new(), month: String::new(), day: String::new() }
    }
    fn from_string(val: String) -> Result<Self> {
        if val.len() != 8 {
            bail!("Date value should be in format 'yyyymmdd'. String 8 chars \
                long that is.")
        }
        
        let (y, f) = match val.len() > 4 {
            true => val.split_at(4),
            false => bail!("Unable to split years from date '{}' string", val),
        };
        let (m, d) = f.split_at(2);

        Ok(Self { year: y.to_string() , month: m.to_string(), day: d.to_string() })
    }
}

pub enum EdiLine {
    Buyer(String),
    Seller(String),
    Entry(String)
}

impl EdiLine {
    pub fn line_read(read: Result<String, std::io::Error>, i: usize, reqlen: usize)
    -> Result<(Option<Self>, Vec<String>)> {
        let s = match read {
            Ok(s) => s,
            Err(e) => bail!("Line {}, Unable to read as text string: {}", i + 1, e),
        };
    
        // Skip empty lines silently (should not be any, but still)
        if s.is_empty() || s.trim().is_empty() { return Ok((None, vec![])) }
    
        // Collect buyer
        if i == 0 { return Ok((Some(Self::Buyer(s)), vec![])) }
    
        // ..and seller
        if i == 1 { return Ok((Some(Self::Seller(s)), vec![])) }
    
        let linelen = s.chars().count();
        let mut warnings = vec![];
    
        if linelen < reqlen {
            warnings.push(format!(
                "Line: {}, length {} is smaller than the expected length {}", i + 1, s.len(), reqlen
            ));
    
            return Ok((Some(Self::Entry(s)), warnings))
        }
    
        // Give one more opportunity
        if linelen > reqlen {
            let cl = s.trim();
    
            return match cl.chars().count() > reqlen {
                true => {
                    error!("{}", &cl);
                    warnings.push(format!(
                        "Skipping line {}, length {} is greater than expexted {} ({})", i + 1,
                        cl.len(), reqlen, cl
                    ));
                    
                    Ok((None, warnings))
                },
                false => Ok((Some(Self::Entry(cl.to_string())), vec![])),
            }
        }
    
        Ok((Some(Self::Entry(s)), vec![]))
    }
}

pub enum EdiType {
    Invalid,
    Product(bool),
    Price(bool),
    Discount(bool)
}

impl EdiType {
    /// Reads EDI file and imports its lines into the database.
    /// Generates also JSON version of EDI data.
    pub fn file_import(edifile_path: &PathBuf, edifile_name: &String, config: &Config, db_sellers: &mut Connection,
        db_buyers: &mut Connection, log: &mut File)
    -> Result<Self> {
        let d = chrono::Utc::now();
        let dmy = format!("{} import started on: {}", edifile_name, d.format("%d.%m.%y %H:%M:%S"));
        
        writeln!(log, "{}", dmy).unwrap();

        // Products EDI file
        if is_product_file(edifile_path).unwrap() {
            match edi_file_imported(config, &edifile_path, EdiOwnership::Seller) {
                Ok(b) => match b {
                    true => {
                        info!("Skipping rewriting for up to date product source file {:?}", &edifile_path);
    
                        return Ok(Self::Product(false))
                    },
                    false => {
                        info!("Running product update from source file {:?}", &edifile_path);
                    }
                },
                Err(e) => bail!("Failed to compare new and latest product \
                    source files: {}", e)
            }
    
            // Collect separate list for each supported language
            let mut supplier_dir = PathBuf::new();
            for c in config.lang_codes.iter() {
                match products_writer(config, &edifile_path, c, db_sellers, log) {
                    Ok(d) => { supplier_dir = d; },
                    Err(e) => {
                        warn!("Failed to write products from {:?} in \
                            lang {}: {}", edifile_path, c, e);
                    }
                };
            }
    
            move_file(&edifile_path, &supplier_dir, EDI_DIR_NAME, edifile_name);
    
            return Ok(Self::Product(true))
        }
    
        // Prices EDI file
        if is_price_file(&edifile_path).unwrap() {
            match edi_file_imported(config, &edifile_path, EdiOwnership::Seller) {
                Ok(b) => if b {
                    info!("Skipping rewriting for up to date price source \
                        file {:?}", &edifile_path);
    
                    return Ok(Self::Price(false))
                },
                Err(e) => bail!("Failed to compare new and latest price \
                    source files: {}", e)
            }
    
            let supplier_dir = match prices_writer(config, &edifile_path, db_sellers, log) {
                Ok(d) => d,
                Err(e) => bail!("Failed to write prices: {}", e),
            };
        
            move_file(&edifile_path, &supplier_dir, EDI_DIR_NAME, edifile_name);

            return Ok(Self::Price(true))
        }

        // Discount EDI file
        if is_discount_file(&edifile_path).unwrap() {
            match edi_file_imported(config, &edifile_path, EdiOwnership::Buyer) {
                Ok(b) => if b {
                    info!("Skipping rewriting for up to date discount source \
                        file {:?}", &edifile_path);
    
                    return Ok(Self::Discount(false))
                },
                Err(e) => bail!("Failed to compare new and latest discount \
                    source files: {}", e)
            }

            // Query discount and price groups from database for possible discount file processing
            let discount_groups = match query_discount_groups(&db_sellers) {
                Ok(v) => v,
                Err(e) => bail!("Failed to query discount groups: {}", e),
            };

            let price_groups = match query_price_groups(&db_sellers) {
                Ok(v) => v,
                Err(e) => bail!("Failed to query price groups: {}", e),
            };

            debug!("Opening discounts file {:?}...", &edifile_path);

            let buyer_dir = discounts_writer(
                config, &edifile_path, db_buyers, &discount_groups, &price_groups, log
            ).map_err(|e|anyhow!("Failed to write discounts: {}", e))?;

            // Discount EDI file should be named as the discounts.txt
            move_file(&edifile_path, &buyer_dir, EDI_DIR_NAME, "discounts.txt");
            
            return Ok(Self::Discount(true))
        }
    
        // Don't leave obsolete files hanging around. If we're still here then this
        // is a clusterfuck situation
        if edifile_path.is_file() {
            error!("Deleting obsolete file from {:?}. File was not recognized as \
                product nor price EDI file", &edifile_path);
            
            if let Err(e) = remove_file(&edifile_path) {
                bail!("Forgot how to delete a file: {}", e)
            }
        }
    
        Ok(Self::Invalid)
    }
}

pub fn edi_line_iter(pointer: usize, chars: &Chars<'_>, take_next: &usize) -> Result<(String, usize)> {
    let mut value = vec![];

    for (i, c) in chars.to_owned().enumerate() {
        if i < pointer {
            continue;
        }

        value.push(c);

        if value.len() == *take_next {
            let s = String::from_iter(value);

            return Ok((s.trim().to_string(), pointer + take_next))
        }
    }

    let failing = String::from_iter(chars.to_owned());

    bail!("Failed to extract [{}-{}] from line '{}'", pointer, take_next, failing)
}

pub fn str_as_f64(int: &str, des: &str, val: &String) -> Result<f64> {
    let mut d: f64 = match int.parse() {
        Ok(f) => f,
        Err(e) => bail!("Failed to read integers ({}) from \
            string '{}' as number: {}", int, val, e),
    };

    let dd: f64 = match (format!("0.{}", des)).parse() {
        Ok(f) => f,
        Err(e) => bail!("Failed to read decimals ({}) from \
            string '{}' as number: {}", des, val, e),
    };

    // Count up
    d += dd;

    Ok(d)
}

pub fn import_warning_logger(log: &mut File, path: &PathBuf, warnings: Vec<String>) -> Result<()> {
    if !warnings.is_empty() {
        writeln!(log, "File {:?} produced {} warnings:", path, warnings.len())?;

        warn!("File {:?} produced {} warnings. All warnings are logged.", path, warnings.len());
    }

    for w in warnings {
        writeln!(log, "Warning: {}", w)?;
    }

    Ok(())
}