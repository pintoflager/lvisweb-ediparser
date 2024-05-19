use std::collections::HashMap;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use anyhow::{anyhow, bail, Result};
use std::fs::{File, write, create_dir_all, read_to_string};
use std::io::{prelude::*, BufReader};
use log::{debug, error};
use rusqlite::{Connection, params};

use crate::config::Config;
use crate::utils::Category;
use super::header::EdiParty;
use super::{edi_line_iter, import_warning_logger, str_as_f64, EdiDate, EdiLine};

const SEQ_PRICE_REQLEN: usize = 100;
const EXPL_SEQ_PRICE: [usize; 19] = [
    1, 1, 9, 2, 9, 8, 6, 3, 4, 9, 5, 9, 5, 9, 5, 3, 9, 1, 2
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Price {
    #[serde(skip)]
    category: Category, 
    #[serde(skip)]
    identifier: String, // Tuotenumero 9 A
    #[serde(rename = "group")]
    price_group: String, // Hintalaji 2 A 01 = ohjehinta alv 0%
    price: f64,// Hinta 9(N2) ovh sentteinä
    date: EdiDate, // Voimaantulopvm 8 vvvvkkpp
    #[serde(rename = "disc")]
    discount_group: String, // Alennusryhmä 6 A *
    unit: String, // Yksikkö 3 A
    #[serde(rename = "incl")]
    units_incl: i64, // Hinnoitteluyksikkö 4 N Esim. 1, 10, 100, 100 = kuinka monta perusyksikköä hinta sisältää, meillä aina 1
    #[serde(rename = "p1", skip_serializing_if = "Option::is_none")]
    packaging_1: Option<f64>, // Pakkauskoko 1 9(2) *
    #[serde(rename = "p1d", skip_serializing_if = "Option::is_none")]
    packaging_1_discount: Option<f64>, // Pakkauskoko 1 alennus % 5(2)* ei käytössä
    #[serde(rename = "p2", skip_serializing_if = "Option::is_none")]
    packaging_2: Option<f64>,
    #[serde(rename = "p2d", skip_serializing_if = "Option::is_none")]
    packaging_2_discount: Option<f64>,
    #[serde(rename = "p3", skip_serializing_if = "Option::is_none")]
    packaging_3: Option<f64>,
    #[serde(rename = "p3d", skip_serializing_if = "Option::is_none")]
    packaging_3_discount: Option<f64>,
    #[serde(rename = "i", skip_serializing_if = "Option::is_none")]
    usage_unit: Option<String>, // Käyttöyksikkö 3 A *
    #[serde(rename = "ix")]
    usables_in_unit: f64, // Käyttöyksikkökerroin 9(N4) Oletusarvo 10000 (=1)
    #[serde(rename = "stock")]
    stock_item: Option<bool>,
    #[serde(rename = "delay", skip_serializing_if = "Option::is_none")]
    delivery_in_weeks: Option<i32> // Tukkurin hankinta-aika, vko 2 *
}

impl Price {
    fn new() -> Self {
        Self {
            category: Category::Unset,
            identifier: String::new(),
            price_group: String::new(),
            price: 0.0f64,
            date: EdiDate::new(),
            discount_group: String::new(),
            unit: String::new(),
            units_incl: 0i64,
            packaging_1: None,
            packaging_1_discount: None,
            packaging_2: None,
            packaging_2_discount: None,
            packaging_3: None,
            packaging_3_discount: None,
            usage_unit: None,
            usables_in_unit: 0.0f64,
            stock_item: None,
            delivery_in_weeks: None,
        }
    }
    fn from_line(line: String) -> Result<(Self, Vec<String>)> {
        let mut price = Self::new();
        let chars = line.chars();
        let mut pointer = 0;
        let mut warnings = vec![];

        for (j, v) in EXPL_SEQ_PRICE.iter().enumerate() {
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

            // Special cases.
            let handled = match j {
                1 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    price.category = Category::from_edi_str(val.as_str())?;
                    Some(p)
                },
                4 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    let (int, des) = match val.len() > 7 {
                        true => val.split_at(7),
                        false => bail!("Unable to split decimals from '{}' string", val),
                    };
                    
                    // Price in eur _cents_
                    price.price = str_as_f64(int, des, &val)?;
                    // let eurs = price.price / 100.0;

                    Some(p)
                },
                5 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    price.date = EdiDate::from_string(val)?;
                    Some(p)
                },
                8 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    let int: i64 = match val.parse() {
                        Ok(f) => f,
                        Err(e) => bail!("Failed to read '{}' as \
                            number: {}", val, e),
                    };

                    if int > 0 {
                        price.units_incl = int;
                    }

                    Some(p)
                },
                16 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    let (int, des) = match val.len() > 5 {
                        true => val.split_at(5),
                        false => bail!("Unable to split decimals from '{}' string", val),
                    };
                    
                    let d = str_as_f64(int, des, &val)?;
                    
                    price.usables_in_unit = d;
                    Some(p)
                },
                17 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    if val.eq("E") {
                        price.stock_item = Some(false);
                    }
                    Some(p)
                },
                18 => {
                    // Last chunk and optional, some source files seem to ignore
                    // this completely. They should not, but hey nothing is perfect.
                    let (val, p) = match edi_line_iter(pointer, &chars, v) {
                        Ok(t) => t,
                        Err(_) => {
                            warnings.push("Optional last value in price catalog \
                                ignored. Should be '00' for empty.".to_string());
                            
                            break;
                        }
                    };
                    
                    if ! val.is_empty() && val.ne("00") {
                        let int: i32 = match val.parse() {
                            Ok(f) => f,
                            Err(e) => bail!("Failed to read '{}' as \
                                number: {}", val, e),
                        };

                        if int > 0 {
                            price.delivery_in_weeks = Some(int);
                        }
                    }

                    Some(p)
                },
                _ => None,
            };

            if let Some(p) = handled {
                pointer = p;
                continue;
            }

            // Strings, required ones.
            if [2, 3, 6, 7].contains(&j) {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;
                
                if val.is_empty() {
                    match j {
                        2 => bail!("Product identifier in price is an empty string."),
                        3 => bail!("Price group is an empty string."),
                        6 => bail!("Product discount group in price is an empty string."),
                        7 => bail!("Price unit is an empty string."),
                        _ => (),
                    }
                }

                match j {
                    2 => { price.identifier = val },
                    3 => { price.price_group = val },
                    6 => { price.discount_group = val },
                    7 => { price.unit = val },
                    _ => (),
                }
                pointer = p;

                continue;
            }

            // Optional strings
            if [15].contains(&j) {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;
                pointer = p;
                
                if val.is_empty() {
                    continue;
                }

                match j {
                    15 => { price.usage_unit = Some(val) },
                    _ => (),
                }

                continue;
            }

            // Optional floating point numbers.
            if [9, 10, 11, 12, 13, 14].contains(&j) {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;
                pointer = p;
                
                if val.is_empty() {
                    continue;
                }

                let (int, des) = match j {
                    9 => match val.len() > 7 {
                        true => val.split_at(7),
                        false => continue,
                    },
                    10 => match val.len() > 3 {
                        true => val.split_at(3),
                        false => continue,
                    },
                    11 => match val.len() > 7 {
                        true => val.split_at(7),
                        false => continue,
                    },
                    12 => match val.len() > 3 {
                        true => val.split_at(3),
                        false => continue,
                    },
                    13 => match val.len() > 7 {
                        true => val.split_at(7),
                        false => continue,
                    },
                    14 => match val.len() > 3 {
                        true => val.split_at(3),
                        false => continue,
                    },
                    _ => bail!("Stupid developer issue on optional number fields"),
                };

                let d = str_as_f64(int, des, &val)?;

                // Ignore zero results as these are optional fields.
                let zero = 0.0f64;

                if d.eq(&zero) {
                    continue;
                }

                match j {
                    9 => { price.packaging_1 = Some(d) },
                    10 => { price.packaging_1_discount = Some(d) },
                    11 => { price.packaging_2 = Some(d) },
                    12 => { price.packaging_2_discount = Some(d) },
                    13 => { price.packaging_3 = Some(d) },
                    14 => { price.packaging_3_discount = Some(d) },
                    _ => (),
                }

                continue;
            }

            bail!("missing index '{}' in line parser", j);
        }

        Ok((price, warnings))
    }
}

pub fn is_price_file(path: &PathBuf) -> Result<bool> {
    // To prevent stupid developer errors
    let mut total = 0;
    
    for v in EXPL_SEQ_PRICE.iter() {
        total += v
    }

    if total != SEQ_PRICE_REQLEN {
        bail!("Price file decoder has developer level issues.")
    }
    
    let uft8_file = File::open(path)?;
    let reader = BufReader::new(uft8_file);

    // Skip headers with iterator.
    for (i, l) in reader.lines().skip(2).enumerate() {
        let s = match l {
            Ok(s) => s,
            Err(_) => bail!("unable to read line number {} from {:?}", i, path),
        };

        return match Price::from_line(s) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    bail!("What the hell 2")
}

pub fn prices_writer(config: &Config, path: &PathBuf, db_conn: &mut Connection, log: &mut File)
-> Result<PathBuf> {
    // Open utf8 encoded file and read it line by line.
    let uft8_file = File::open(path)?;
    let reader = BufReader::new(uft8_file);
    let file_suffix = "json";

    let mut supplier_dir = PathBuf::new();
    let mut id = String::new();
    let mut prices = HashMap::new();

    let mut warnings = vec![];
    let ctx = db_conn.transaction()?;

    for (i, l) in reader.lines().enumerate() {
        let line = match EdiLine::line_read(l, i, SEQ_PRICE_REQLEN)? {
            (Some(l), w) => {
                warnings.push(w);
                l
            },
            (None, w) => {
                warnings.push(w);
                continue
            },
        };

        match line {
            EdiLine::Buyer(_) => continue,
            EdiLine::Seller(s) => {
                match EdiParty::create(config, s) {
                    Ok((d, i)) => {
                        supplier_dir = d;
                        id = i;
                    },
                    Err(e) => bail!("Failed to create seller dir: {}", e),
                }

                let sc = match config.seller.iter().find(|s| s.id.eq(&id)) {
                    Some(c) => c,
                    None => {
                        error!("Unable to find config for seller ID {}, \
                            skipping seller...", &id);
                        continue;
                    }
                };

                if config.import.sqlite {
                    ctx.execute(
                        "insert or ignore into sellers (id, name) values (?1, ?2)",
                        [&id, &sc.name]
                    )?;
                }
                
                // Take existing values to categories and update to those instead
                // of overwriting the whole crap.
                if config.import.json {
                    let mut prices_dir = supplier_dir.to_owned();
                    prices_dir.push("prices");

                    for (k, v) in Category::mapper() {
                        let name = format!("{}.{}", k, &file_suffix);
                        let mut extf = prices_dir.to_owned();
                        extf.push(name);

                        if extf.is_file() {
                            let s = read_to_string(extf)?;
                            let pri = serde_json::from_str::<HashMap<String, Price>>(&s)?;

                            prices.insert(v, pri);
                        }
                    }
                }
            },
            EdiLine::Entry(s) => match Price::from_line(s) {
                Ok((p, w)) => {
                    warnings.push(w);
    
                    match prices.get_mut(&p.category) {
                        Some(m) => {
                            m.insert(
                                p.identifier.to_owned(),
                                p
                            );
                        },
                        None => {
                            let mut map = HashMap::new();
                            map.insert(p.identifier.to_owned(), p.to_owned());
                            prices.insert(p.category.to_owned(), map);
                        }
                    }
                },
                Err(e) => debug!("price read error '{}', line: {}", e, i + 1),
            }
        }
    }

    // Print unique warnings from decoder.
    let mut warnings = warnings.concat();
    warnings.sort();
    warnings.dedup();

    if let Err(e) = import_warning_logger(log, path, warnings) {
        error!("Failed to write {:?} warnings to log: {}", path, e);
    }

    // Needed if json files are written.
    let mut prices_dir = supplier_dir.to_owned();
    prices_dir.push("prices");
    
    if config.import.json {
        create_dir_all(&prices_dir).map_err(|e|anyhow!(
            "Failed to create supplier prices dir {:?}: {}", prices_dir, e
        ))?;
    }

    if config.import.sqlite {
        // SQLite add missing units
        let mut units = prices.iter()
            .flat_map(|(_, m)|
                m.iter().map(|(_, p)| p.unit.to_owned())
            ).collect::<Vec<String>>();
        
        units.sort();
        units.dedup();

        for u in units.iter() {
            ctx.execute(
                "insert or ignore into units (id) values (?1)",
                [u]
            )?;
        }

        // SQLite add missing discount groups
        let mut discgr = prices.iter()
            .flat_map(|(_, m)|
                m.iter()
                .map(|(_, p)| p.discount_group.to_owned())
            ).collect::<Vec<String>>();

        discgr.sort();
        discgr.dedup();

        for u in discgr.iter() {
            ctx.execute(
                "insert or ignore into discount_groups (id) values (?1)",
                [u]
            )?;
        }

        // SQLite add missing price groups
        let mut pricegr = prices.iter()
            .flat_map(|(_, m)|
                m.iter()
                .map(|(_, p)| p.price_group.to_owned())
            ).collect::<Vec<String>>();

        pricegr.sort();
        pricegr.dedup();

        for u in pricegr.iter() {
            ctx.execute(
                "insert or ignore into price_groups (id) values (?1)",
                [u]
            )?;
        }
    }

    ctx.commit()?;

    //  TODO: Or should it be... ?
    // match config.import.sqlite {
    //     true => ctx.commit()?,
    //     false => ctx.rollback()?,
    // };

    for (k, v) in prices {
        let tx = db_conn.transaction()?;

        for p in v.values() {
            if config.import.sqlite {
                let pid = p.identifier.to_owned();
                let prid = format!("{}{}", &id, &pid);

                tx.execute(
                    &format!("insert into prices_{} (id, product_id, price_group, price, \
                    date, discount_group, unit, units_incl, packaging_1, \
                    packaging_1_discount, packaging_2, packaging_2_discount, packaging_3, \
                    packaging_3_discount, usage_unit, usables_in_unit, stock_item, \
                    delivery_in_weeks) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, \
                    ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18) on conflict (id) do update \
                    set price_group=excluded.price_group, price=excluded.price, \
                    date=excluded.date, discount_group=excluded.discount_group, \
                    unit=excluded.unit, units_incl=excluded.units_incl, \
                    packaging_1=excluded.packaging_1, packaging_1_discount=excluded.packaging_1_discount, \
                    packaging_2=excluded.packaging_2, packaging_2_discount=excluded.packaging_2_discount, \
                    packaging_3=excluded.packaging_3, packaging_3_discount=excluded.packaging_3_discount, \
                    usage_unit=excluded.usage_unit, usables_in_unit=excluded.usables_in_unit, \
                    stock_item=excluded.stock_item, \
                    delivery_in_weeks=excluded.delivery_in_weeks", k),
                    params!(
                        &prid, &pid, &p.price_group, &p.price, &format!(
                            "{}-{}-{} 00:00:00.000", &p.date.year,
                            &p.date.month,
                            &p.date.day),
                        &p.discount_group, &p.unit, &p.units_incl,
                        &p.packaging_1, &p.packaging_1_discount,
                        &p.packaging_2, &p.packaging_2_discount, &p.packaging_3,
                        &p.packaging_3_discount, &p.usage_unit, &p.usables_in_unit,
                        p.stock_item.unwrap_or(true), &p.delivery_in_weeks
                    )
                ).map_err(|e|anyhow!("Price add failure: {}", e))?;
            }
        }

        tx.commit()?;
        
        if config.import.json {
            let json = serde_json::to_string(&v)?;
            let name = format!("{}.{}", k, &file_suffix);
            let mut file = prices_dir.to_owned();
            file.push(name);
            
            write(&file, json.as_bytes())?;
        }
    }

    Ok(supplier_dir)
}
