use std::{collections::HashMap, path::PathBuf};
use serde::{Serialize, Deserialize};
use anyhow::{anyhow, bail, Result};
use log::{debug, error};
use std::fs::{File, write, create_dir_all, read_to_string};
use std::io::{prelude::*, BufReader};
use rusqlite::{Connection, params};

use crate::config::Config;
use crate::edi::header::EdiParty;
use crate::edi::{import_warning_logger, str_as_f64};
use crate::utils::{Category, Lang, Operation};
use super::{EdiDate, EdiLine, edi_line_iter};

const SEQ_PROD_REQLEN: usize = 232;
const EXPL_SEQ_PRODUCT: [usize; 27] = [
    1, 1, 9, 1, 3, 8, 35, 35, 20, 7, 6, 3, 7, 7, 9, 9, 5, 9, 5,
    9, 5, 3, 2, 1, 20, 3, 9
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Product {
    #[serde(skip)]
    category: Category, 
    #[serde(skip)]
    identifier: String, // Tuotenumero 9 A
    #[serde(rename = "op")]
    operation: Operation,
    #[serde(skip)]
    lang: Lang,
    date: EdiDate, // Voimaantulopvm 8 vvvvkkpp
    name: String, // Tuotteen nimi 35 A Tuotekuvaus 1
    #[serde(rename = "name2")]
    description: String, // Nimen jatke 35 A Tuotekuvaus 2
    #[serde(rename = "tag", skip_serializing_if = "Option::is_none")]
    search_tags: Option<String>, // Hakumerkki 20 A *
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    search_code: Option<String>, // Pikakoodi 7 A * ei käytössä
    #[serde(rename = "disc", skip_serializing_if = "Option::is_none")]
    discount_group: Option<String>, // Alennusryhmä 6 A *
    unit: String, // Yksikkö 3 A
    #[serde(rename = "weight", skip_serializing_if = "Option::is_none")]
    unit_weight: Option<f64>, // Yksikön paino, kg 7(3) *
    #[serde(rename = "vol", skip_serializing_if = "Option::is_none")]
    unit_volume: Option<f64>, // Yksikön tilavuus, ltr 7(3) *
    #[serde(rename = "pkg", skip_serializing_if = "Option::is_none")]
    typical_packaging: Option<i64>, // Yleisimmin käytetty pakkauskoko 9 *
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
    #[serde(rename = "tax", skip_serializing_if = "Option::is_none")]
    tax_class: Option<String>,// Veroluokka 3 A * ei käytössä
    #[serde(rename = "delay", skip_serializing_if = "Option::is_none")]
    delivery_in_weeks: Option<i32>, // Tukkurin hankinta-aika, vko 2 *
    #[serde(rename = "stock")]
    stock_item: Option<bool>,
    #[serde(rename = "ean", skip_serializing_if = "Option::is_none")]
    ean_code: Option<String>, // EAN-koodi 20 A * ei käytössä
    #[serde(rename = "i", skip_serializing_if = "Option::is_none")]
    usage_unit: Option<String>, // Käyttöyksikkö 3 A *
    #[serde(rename = "ix")]
    usables_in_unit: f64 // Käyttöyksikkökerroin 9(N4) Oletusarvo 10000 (=1)
}

impl Product {
    fn new() -> Self {
        Self {
            category: Category::Unset,
            identifier: String::new(),
            operation: Operation::Empty,
            lang: Lang::default(),
            date: EdiDate::new(),
            name: String::new(),
            description: String::new(),
            search_tags: None,
            search_code: None,
            discount_group: None,
            unit: String::new(),
            unit_weight: None,
            unit_volume: None,
            typical_packaging: None,
            packaging_1: None,
            packaging_1_discount: None,
            packaging_2: None,
            packaging_2_discount: None,
            packaging_3: None,
            packaging_3_discount: None,
            tax_class: None,
            delivery_in_weeks: None,
            stock_item: None,
            ean_code: None,
            usage_unit: None,
            usables_in_unit: 0.0f64
        }
    }
    fn from_line(line: String, lang_filter: Option<&Lang>) -> Result<(Self, Vec<String>)> {
        let mut product = Self::new();
        let mut warnings = vec![];
        let chars = line.chars();
        let mut pointer = 0;

        for (j, v) in EXPL_SEQ_PRODUCT.iter().enumerate() {
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
                    product.category = Category::from_edi_str(val.as_str())?;
                    Some(p)
                },
                3 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    let op = match Operation::from_str(val.as_str()) {
                        Ok(o) => o,
                        Err(e) => bail!("Product with ID: {} fails for bad \
                            operation: {}", product.identifier, e)
                    };
                    product.operation = op;
                    Some(p)
                },
                4 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    let l = match Lang::from_name(&val) {
                        Ok(l) => l,
                        Err(e) => bail!("Product has invalid language {val}: {}", e),
                    };

                    if let Some(f) = lang_filter {
                        if l.ne(f) {
                            bail!("Language filter set to '{}' and product lang is '{}'",
                                f, l)
                        }
                    }

                    product.lang = l;
                    Some(p)
                },
                5 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    product.date = EdiDate::from_string(val)?;
                    Some(p)
                },
                14 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    if ! val.is_empty() {
                        let int: i64 = match val.parse() {
                            Ok(f) => f,
                            Err(e) => bail!("Failed to read '{}' as \
                                number: {}", val, e),
                        };
                        product.typical_packaging = Some(int);
                    }

                    Some(p)
                },
                22 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    
                    if ! val.is_empty() {
                        let int: i32 = match val.parse() {
                            Ok(f) => f,
                            Err(e) => bail!("Failed to read '{}' as \
                                number: {}", val, e),
                        };
    
                        if int > 0 {
                            product.delivery_in_weeks = Some(int);
                        }
                    }

                    Some(p)
                },
                23 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    
                    if val.eq("E") {
                        product.stock_item = Some(false);
                    }
                    Some(p)
                },
                26 => {
                    let (val, p) = edi_line_iter(pointer, &chars, v)?;
                    let (int, des) = match val.len() > 5 {
                        true => val.split_at(5),
                        false => bail!("Unable to split decimals from '{}' string", val),
                    };
                    
                    let d = str_as_f64(int, des, &val)?;
                    
                    product.usables_in_unit = d;
                    Some(p)
                },
                _ => None,
            };

            if let Some(p) = handled {
                pointer = p;
                continue;
            }

            // String types, required fields.
            if [2, 6, 7, 11].contains(&j) {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;
                
                if val.is_empty() {
                    match j {
                        2 => bail!("Product identifier is an empty string"),
                        6 => bail!("Product name is an empty string"),
                        7 => warnings.push(format!(
                            "[{}]: Product description is an empty string", product.identifier)
                        ),
                        11 => bail!("Product unit is an empty string"),
                        _ => (),
                    }
                }

                match j {
                    2 => { product.identifier = val },
                    6 => { product.name = val },
                    7 => { product.description = val },
                    11 => { product.unit = val },
                    _ => (),
                }
                pointer = p;

                continue;
            }

            // String types, optional.
            if [8, 9, 10, 21, 24, 25].contains(&j) {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;
                pointer = p;
                
                if val.is_empty() {
                    continue;
                }

                match j {
                    8 => { product.search_tags = Some(val) },
                    9 => { product.search_code = Some(val) },
                    10 => { product.discount_group = Some(val) },
                    21 => { product.tax_class = Some(val) },
                    24 => { product.ean_code = Some(val) },
                    25 => { product.usage_unit = Some(val) },
                    _ => (),
                }

                continue;              
            }

            // Optional floating point numbers.
            if [12, 13, 15, 16, 17, 18, 19, 20].contains(&j) {
                let (val, p) = edi_line_iter(pointer, &chars, v)?;
                pointer = p;
                
                if val.is_empty() {
                    continue;
                }

                let (int, des) = match j {
                    12 => match val.len() > 4 {
                        true => val.split_at(4),
                        false => continue,
                    },
                    13 => match val.len() > 4 {
                        true => val.split_at(4),
                        false => continue,
                    },
                    15 => match val.len() > 7 {
                        true => val.split_at(7),
                        false => continue,
                    },
                    16 => match val.len() > 3 {
                        true => val.split_at(3),
                        false => continue,
                    },
                    17 => match val.len() > 7 {
                        true => val.split_at(7),
                        false => continue,
                    },
                    18 => match val.len() > 3 {
                        true => val.split_at(3),
                        false => continue,
                    },
                    19 => match val.len() > 7 {
                        true => val.split_at(7),
                        false => continue,
                    },
                    20 => match val.len() > 3 {
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
                    12 => { product.unit_weight = Some(d) },
                    13 => { product.unit_volume = Some(d) },
                    15 => { product.packaging_1 = Some(d) },
                    16 => { product.packaging_1_discount = Some(d) },
                    17 => { product.packaging_2 = Some(d) },
                    18 => { product.packaging_2_discount = Some(d) },
                    19 => { product.packaging_3 = Some(d) },
                    20 => { product.packaging_3_discount = Some(d) },
                    _ => (),
                }

                continue;
            }

            bail!("missing index '{}' in line parser", j);
        }

        Ok((product, warnings))
    }
}

pub fn is_product_file(path: &PathBuf) -> Result<bool> {
    // To prevent stupid developer errors
    let mut total = 0;
    
    for v in EXPL_SEQ_PRODUCT.iter() {
        total += v
    }

    if total != SEQ_PROD_REQLEN {
        bail!("Product file decoder has developer level issues.")
    }
    
    let uft8_file = File::open(path)?;
    let reader = BufReader::new(uft8_file);

    // Skip headers with iterator.
    for (i, l) in reader.lines().skip(2).enumerate() {
        let s = match l {
            Ok(s) => s,
            Err(_) => bail!("unable to read line number {} from {:?}", i, path),
        };

        return match Product::from_line(s, None) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    bail!("What the hell")
}

pub fn products_writer(config: &Config, path: &PathBuf, lang_filter: &Lang, db_conn: &mut Connection, log: &mut File)
-> Result<PathBuf> {
    // Open utf8 encoded file and read it line by line.
    let uft8_file = match File::open(path) {
        Ok(f) => f,
        Err(e) => bail!("Failed to open products source file (utf-8) \
            for reading: {}", e)
    };

    debug!("Adding products with language code: {}", lang_filter);

    let reader = BufReader::new(uft8_file);
    let file_suffix = format!("{}.json", lang_filter.to_name());

    let mut supplier_dir = PathBuf::new();
    let mut seller_id = String::new();
    let mut categorized_products = HashMap::new();

    let mut warnings = vec![];
    let ctx = db_conn.transaction()?;

    for (i, l) in reader.lines().enumerate() {
        let line = match EdiLine::line_read(l, i, SEQ_PROD_REQLEN)? {
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
            EdiLine::Buyer(_) => continue,
            EdiLine::Seller(s) => {
                match EdiParty::create(config, s) {
                    Ok((d, i)) => {
                        supplier_dir = d;
                        seller_id = i;
                    },
                    Err(e) => bail!("Failed to create seller dir: {}", e),
                }

                let sc = match config.seller.iter().find(|s| s.id.eq(&seller_id)) {
                    Some(c) => c,
                    None => {
                        warnings.push(format!("Unable to find config for seller ID {}, \
                            skipping seller...", &seller_id));
                        continue;
                    }
                };

                if config.import.sqlite {
                    ctx.execute(
                        "insert or ignore into sellers (id, name) values (?1, ?2)",
                        [&seller_id, &sc.name]
                    )?;
                }

                // Take existing values to categories and update to those instead
                // of overwriting the whole crap. This is for the json file. DB
                // does insert or update.
                if config.import.json {
                    let mut products_dir = supplier_dir.to_owned();
                    products_dir.push("products");

                    for (k, v) in Category::mapper() {
                        let name = format!("{}.{}", k, &file_suffix);
                        let mut extf = products_dir.to_owned();
                        extf.push(name);

                        if extf.is_file() {
                            let s = read_to_string(extf)?;
                            let prod = serde_json::from_str::<HashMap<String, Product>>(&s)?;

                            categorized_products.insert(v, prod);
                        }
                    }
                }
            },
            EdiLine::Entry(s) => match Product::from_line(s, Some(lang_filter)) {
                Ok((p, w)) => {
                    warnings.extend(w);

                    match categorized_products.get_mut(&p.category) {
                        Some(m) => {
                            m.insert(
                                p.identifier.to_owned(),
                                p
                            );
                        },
                        None => {
                            let mut map = HashMap::new();
                            map.insert(p.identifier.to_owned(), p.to_owned());
                            categorized_products.insert(p.category.to_owned(), map);
                        }
                    }
                },
                Err(e) => warnings.push(format!("Product read: {}", e)),
            }
        }
    }

    warnings.sort();
    warnings.dedup();

    if let Err(e) = import_warning_logger(log, path, warnings) {
        error!("Failed to write {:?} warnings to log: {}", path, e);
    }

    // Needed if json files are written.
    let mut products_dir = supplier_dir.to_owned();
    products_dir.push("products");
    
    if config.import.json {
        create_dir_all(&products_dir).map_err(|e|anyhow!(
            "Failed to create supplier products dir {:?}: {}", products_dir, e
        ))?;
    }

    if config.import.sqlite {
        // SQLite add missing units
        let mut units = categorized_products.iter()
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

        // SQLite add missing languages
        for (k, l) in Lang::mapper() {
            let lang_found = categorized_products.iter().any(|(_, m)|
                m.iter().any(|(_, p)| p.lang.eq(&l)));

            if ! lang_found {
                continue;
            }
        
            let resp = ctx.execute(
                "insert or ignore into languages (id, name) values (?1, ?2)",
                params!(l.to_index(), k)
            );

            if let Err(e) = resp {
                bail!("Lang write to DB error: {}", e)
            }
        }

        // SQLite add missing discount groups
        let mut discgr = categorized_products.iter()
            .flat_map(|(_, m)|
                m.iter()
                .filter(|(_, p)| p.discount_group.is_some())
                .map(|(_, p)| p.discount_group.as_ref().unwrap().to_owned())
            ).collect::<Vec<String>>();

        discgr.sort();
        discgr.dedup();

        for u in discgr.iter() {
            ctx.execute(
                "insert or ignore into discount_groups (id) values (?1)",
                [u]
            )?;
        }

        // ctx.commit()?;
        
        // // Create generic product if not present yet. New transaction.
        // let ctx = db_conn.transaction()?;

        for p in categorized_products.values().flat_map(|v|v.values()) {
            let category = p.category.to_name();
            
            ctx.execute(
                "insert into products (id, category, tax_class) \
                values (?1, ?2, ?3) on conflict (id) do update set \
                tax_class=excluded.tax_class",
                params!(&p.identifier, category, &p.tax_class)
            ).map_err(|e|anyhow!("Generic product write to DB error: {}", e))?;
        }
    }
    
    ctx.commit()?;
    
    for (k, v) in categorized_products {
        let tx = db_conn.transaction()?;

        for p in v.values() {
            if config.import.sqlite {
                let eid = format!("{}{}", &seller_id, &p.identifier);
                let lix = lang_filter.to_index();
                let tid = format!("{}{}", &eid, lix);

                // Create translation for seller product
                tx.execute(
                    &format!("insert into product_{}_t (id, lang, name, \
                    description, tags, code) values (?1, ?2, ?3, ?4, ?5, ?6) \
                    on conflict (id) do update set name=excluded.name, \
                    description=excluded.description, tags=excluded.tags, \
                    code=excluded.code", k),
                    params!(&tid, &p.lang.to_index(), &p.name, &p.description,
                        &p.search_tags, &p.search_code)
                ).map_err(|e|anyhow!("Product translation DB write error: {}", e))?;

                // Create seller product with references to translation and generic product
                tx.execute(
                    &format!("insert into products_{} (id, product_id, seller_id, \
                    operation, date, discount_group, unit, unit_weight, \
                    unit_volume, typical_packaging, packaging_1, packaging_1_discount, \
                    packaging_2, packaging_2_discount, packaging_3, packaging_3_discount, \
                    delivery_in_weeks, stock_item, ean_code, usage_unit, usables_in_unit) \
                    values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, \
                    ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21) on conflict (id) do update \
                    set operation=excluded.operation, date=excluded.date, \
                    discount_group=excluded.discount_group, \
                    unit=excluded.unit, unit_weight=excluded.unit_weight, \
                    unit_volume=excluded.unit_volume, typical_packaging=excluded.typical_packaging, \
                    packaging_1=excluded.packaging_1, packaging_1_discount=excluded.packaging_1_discount, \
                    packaging_2=excluded.packaging_2, packaging_2_discount=excluded.packaging_2_discount, \
                    packaging_3=excluded.packaging_3, packaging_3_discount=excluded.packaging_3_discount, \
                    delivery_in_weeks=excluded.delivery_in_weeks, \
                    stock_item=excluded.stock_item, ean_code=excluded.ean_code, \
                    usage_unit=excluded.usage_unit, usables_in_unit=excluded.usables_in_unit", k),
                    params!(
                        &eid, &p.identifier, &seller_id, p.operation.to_name(),
                        &format!(
                            "{}-{}-{} 00:00:00.000", &p.date.year,
                            &p.date.month,
                            &p.date.day
                        ), &p.discount_group, &p.unit, &p.unit_weight, &p.unit_volume,
                        &p.typical_packaging, &p.packaging_1, &p.packaging_1_discount,
                        &p.packaging_2, &p.packaging_2_discount, &p.packaging_3,
                        &p.packaging_3_discount, &p.delivery_in_weeks, p.stock_item.unwrap_or(true),
                        &p.ean_code, &p.usage_unit, &p.usables_in_unit
                    )
                ).map_err(|e|anyhow!("Product DB entry failure: {}", e))?;
            }
        }

        tx.commit()?;

        // Json file, simplified format
        if config.import.json {
            let json = serde_json::to_string(&v)?;
            let name = format!("{}.{}", k, &file_suffix);
            let mut file = products_dir.to_owned();

            file.push(name);
            
            write(
                &file,
                json.as_bytes()
            )?;
        }
    }

    Ok(supplier_dir)
}
