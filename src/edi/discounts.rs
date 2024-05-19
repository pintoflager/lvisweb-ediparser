use log::error;
use serde::Serialize;
use anyhow::{bail, Result};
use std::fs::{File, write, create_dir_all};
use std::io::{prelude::*, BufReader};
use rand::distributions::{Alphanumeric, DistString};
use std::path::PathBuf;
use rusqlite::{Connection, params};

use crate::config::Config;

use super::header::EdiParty;
use super::{edi_line_iter, import_warning_logger, str_as_f64, EdiLine};

const SEQ_DISC_REQLEN: usize = 92;
const EXPL_SEQ_DISC: [usize; 7] = [
    1, 6, 25, 40, 2, 9, 9
];

// Tietuetunnus 	A 	1 	1 	R
// Aleryhmä 	    A 	6 	2
// Tunnus 	        A 	25 	8 	 
// Nimi 	        A 	40 	33 	 
// Laji 	        A 	2 	73 	 
//   	01 = alennus, 	  	  	 
//   	02 = pakkausalennus - kumulatiivinen 	  	  	 
//   	03 = pakkausalennus - ei kumulatiivinen 	  	  	 
// Prosentti1 	N 	9 (2 des) 	75 	 
// Prosentti2 	N 	9 (2 des) 	84
#[derive(Debug, Serialize)]
struct Discount {
    #[serde(rename = "disc")]
    discount_group: String, // Alennusryhmä 6 A *
    id: String,
    name: String,
    #[serde(rename = "group")]
    price_group: String,
    pc1: f64,
    pc2: f64,
}

impl Discount {
    fn new() -> Self {
        Self{
            discount_group: String::new(),
            id: String::new(),
            name: String::new(),
            price_group: String::new(),
            pc1: 0.0f64,
            pc2: 0.0f64
        }
    }
    fn from_line(line: String) -> Result<Self> {
        let mut disc = Self::new();
        let chars = line.chars();
        let mut pointer = 0;

        for (j, v) in EXPL_SEQ_DISC.iter().enumerate() {
            if j == 0 {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;

                if val.chars().count() != 1 || v.ne(&1) {
                    bail!("Trying to extract row id from pointer with invalid length.")
                }

                if val.ne("R") {
                    bail!("Row identifier is fixed 'R', found '{}'", val)
                }

                pointer = p;
                continue;
            }

            // Strings.
            if [1, 2, 3, 4].contains(&j) {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;
                
                match j {
                    1 => { disc.discount_group = val },
                    2 => { disc.id = val },
                    3 => { disc.name = val },
                    4 => { disc.price_group = val },
                    _ => (),
                }
                pointer = p;

                continue;
            }

            // Discounts
            if [5, 6].contains(&j) {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;
                let (int, des) = match val.len() > 7 {
                    true => val.split_at(7),
                    false => bail!("Unable to split decimals from '{}' string", val),
                };
                
                let d = str_as_f64(int, des, &val)?;

                match j {
                    5 => { disc.pc1 = d },
                    6 => { disc.pc2 = d },
                    _ => ()
                }

                pointer = p;

                continue;
            }

            bail!("missing index '{}' in line parser", j);
        }

        Ok(disc)
    }
}

pub fn is_discount_file(path: &PathBuf) -> Result<bool> {
    // To prevent stupid developer errors
    let mut total = 0;
    
    for v in EXPL_SEQ_DISC.iter() {
        total += v
    }

    if total != SEQ_DISC_REQLEN {
        bail!("Discount file decoder has developer level issues.")
    }
    
    let uft8_file = File::open(path)?;
    let reader = BufReader::new(uft8_file);

    // Skip headers with iterator.
    for (i, l) in reader.lines().skip(2).enumerate() {
        let s = match l {
            Ok(s) => s,
            Err(_) => bail!("unable to read line number {} from {:?}", i, path),
        };

        return match Discount::from_line(s) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    bail!("What the hell 3")
}

pub fn discounts_writer(config: &Config, path: &PathBuf, db_conn: &mut Connection,
    discount_groups: &Vec<String>, price_groups: &Vec<String>, log: &mut File)
-> Result<PathBuf> {
    // Open utf8 encoded file and read it line by line.
    let uft8_file = File::open(path)?;
    let reader = BufReader::new(uft8_file);

    let mut seller_dir = PathBuf::new();
    let mut discounts = vec![];
    let mut buyer_id = String::new();
    let mut seller_id = String::new();

    let mut warnings = vec![];

    for (i, l) in reader.lines().enumerate() {
        let line = match EdiLine::line_read(l, i, SEQ_DISC_REQLEN)? {
            (Some(l), w) => {
                warnings.extend(w);
                l
            },
            (None, w) => {
                warnings.extend(w);
                continue
            },
        };

        match line {
            EdiLine::Buyer(s) => match EdiParty::create(config, s) {
                Ok((_, i)) => {
                    buyer_id = i;
                },
                Err(e) => bail!("Failed to read buyer from header: {}", e),
            },
            EdiLine::Seller(s) => match EdiParty::create(config, s) {
             Ok((d, i)) => {
                    seller_dir = d;
                    seller_id = i;
                },
                Err(e) => bail!("Failed to create seller dir: {}", e),
            },
            EdiLine::Entry(s) => match Discount::from_line(s) {
                Ok(d) => {
                    match discount_groups.contains(&d.discount_group) {
                        true => match price_groups.contains(&d.price_group) {
                            true => discounts.push(d),
                            false => warnings.push(format!("[{}]: Ignoring as price group \
                                '{}' was not found", &d.discount_group, &d.price_group))
                        },
                        false => warnings.push(format!("[{}]: Ignoring as discount group \
                            was not found", &d.discount_group))
                    }
                },
                Err(e) => eprintln!("price read error '{}', line: {}", e, i + 1),
            }
        }
    }

    // Don't use buyer id as identifier as it comes from the supplier, can collide
    // and is considered to be somewhat private.
    let id_randy = Alphanumeric.sample_string(&mut rand::thread_rng(), 20);
    let bid = format!("{}{}", &buyer_id, &seller_id);

    // Print unique warnings from decoder.
    warnings.sort();
    warnings.dedup();

    if let Err(e) = import_warning_logger(log, path, warnings) {
        error!("Failed to write {:?} warnings to log: {}", path, e);
    }

    // See if supplier is valid
    if ! seller_dir.is_dir() {
        bail!("Unknown supplier {}", seller_id)
    }

    // Create buyer on the database
    if config.import.sqlite {
        let ctx = db_conn.transaction()?;

        ctx.execute(
            "insert or ignore into buyers (id, uuid, buyer_id, vat_percent) \
            values (?1, ?2, ?3, ?4)",
            params![&bid, &id_randy, &buyer_id, config.vat_percent]
        )?;

        for d in discounts.iter() {
            // Add buyers' product discounts per seller and discount group
            let did = format!("{}{}", &bid, &d.discount_group);

            ctx.execute(
                "insert into discounts (id, buyer_id, seller_id, discount_group, price_group, percent_1, percent_2) \
                    values (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                    on conflict (id) do update set price_group=excluded.price_group, \
                    percent_1=excluded.percent_1, percent_2=excluded.percent_2",
                params!(&did, &bid, &seller_id, &d.discount_group, &d.price_group, &d.pc1, &d.pc2)
            )?;
        }

        ctx.commit()?;
    }

    // Create buyer directory which is needed for imported EDI files at least.
    let mut buyer_dir = seller_dir.canonicalize()?;
    buyer_dir.push("buyers");
    buyer_dir.push(&buyer_id);

    // Buyer files under their respective seller.
    if config.import.json {
        let json = serde_json::to_string::<Vec<Discount>>(&discounts)?;
        let mut discounts_file_path = buyer_dir.to_owned();
        discounts_file_path.push("discounts");

        if let Err(e) = create_dir_all(&discounts_file_path) {
            bail!("Failed to create buyer discounts dir {:?}: {}", discounts_file_path, e)
        }

        discounts_file_path.push(&seller_id);
        discounts_file_path.set_extension("json");

        write(&discounts_file_path, json.as_bytes())?;
    }

    Ok(buyer_dir)
}