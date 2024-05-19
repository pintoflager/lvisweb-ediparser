use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::{anyhow, bail, Result};

use crate::config::Config;

use super::edi_line_iter;


const SEQ_TITLE_REQLEN: usize = 23;
const EXPL_SEQ_TITLE: [usize; 4] = [1, 2, 17, 3];

#[derive(Debug, PartialEq, Eq)]
pub enum EdiOwnership {
    Seller,
    Buyer,
    Shared
}

impl EdiOwnership {
    fn to_path(&self) -> Result<&'static str> {
        match self {
            Self::Seller => Ok("sellers"),
            Self::Buyer => Ok("buyers"),
            Self::Shared => bail!("Can't resolve paths to shared ownership."),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct EdiParty {
    pub owner: EdiOwnership,
    pub id: String,
    pub code: String
}

impl EdiParty {
    fn new() -> Self {
        Self { owner: EdiOwnership::Shared, id: String::new(), code: String::new() }
    }
    fn from_line(line: String) -> Result<Self> {
        let mut party = Self::new();
        let chars = line.chars();
        let mut pointer = 0;
    
        for (j, v) in EXPL_SEQ_TITLE.iter().enumerate() {
            let (val, p) = edi_line_iter(pointer, &chars, v)?;
            
            match j {
                0 => {
                    if val.chars().count() != 1 || v.ne(&1) {
                        bail!("Trying to extract party id from pointer with invalid length.")
                    }
    
                    if val.ne("O") {
                        bail!("Party identifier is fixed 'O', found '{}'", val)
                    }
    
                    pointer = p;
                    continue;
                },
                1 => {
                    party.owner = match val.eq("SE") {
                        true => EdiOwnership::Seller,
                        false => match val.eq("BY") {
                            true => EdiOwnership::Buyer,
                            false => bail!("Invalid party identifier. Owner should be BY or SE"),
                        }
                    };
                },
                2 => { party.id = val; },
                3 => { party.code = val; },
                _ => ()

            }

            pointer = p;
        }

        Ok(party)
    }
    pub fn is_seller(&self) -> bool {
        self.owner.eq(&EdiOwnership::Seller)
    }
    pub fn is_buyer(&self) -> bool {
        self.owner.eq(&EdiOwnership::Buyer)
    }
    pub fn party_dir(&self, config: &Config) -> Result<PathBuf> {
        let mut base = config.dir.to_owned();

        match self.is_seller() {
            true => base.push(EdiOwnership::Seller.to_path()?),
            false => match self.is_buyer() {
                true => base.push(EdiOwnership::Buyer.to_path()?),
                false => bail!("Unable to read title as seller nor buyer."),
            }
        }

        Ok(base)
    }
    pub fn create(config: &Config, line: String) -> Result<(PathBuf, String)> {
        let party = match Self::from_line(line) {
            Ok(t) => t,
            Err(e) => bail!("Failed to read string as EDI header: {}", e),
        };
        
        let mut path = party.party_dir(config)?;
        path.push(&party.id);
    
        // Buyers don't get their dir created here. Each buyer is a customer of the seller,
        // so somebody with customer relationship with each provider (seller) would have
        // multiple buyer directories.
        if party.is_seller() {
            create_dir_all(&path).map_err(|e|anyhow!(
                "Failed to create 'seller' home dir for ID {}: {}", &party.id, e
            ))?;
        }
        
        Ok((path, party.id))
    }
}

pub struct EdiHeader {
    pub seller: Option<EdiParty>,
    pub buyer: Option<EdiParty>
}

impl EdiHeader {
    fn new() -> Self {
        Self { seller: None, buyer: None }
    }
    pub fn read(path: &PathBuf) -> Result<Self> {
        // To prevent stupid developer errors
        let mut total = 0;
        
        for v in EXPL_SEQ_TITLE.iter() {
            total += v
        }
    
        if total != SEQ_TITLE_REQLEN {
            bail!("Header decoder has developer level issues.")
        }
        
        let uft8_file = File::open(path)?;
        let reader = BufReader::new(uft8_file);
        let mut head = Self::new();
    
        // Read only header, two lines that is.
        for (i, l) in reader.lines().take(2).enumerate() {
            let s = match l {
                Ok(s) => s,
                Err(_) => bail!("unable to read header line {} from {:?}", i, path),
            };
    
            match EdiParty::from_line(s) {
                Ok(t) => match t.is_buyer() {
                    true => head.buyer = Some(t),
                    false => head.seller = Some(t),
                },
                Err(e) => bail!("Failed to read header line: {}", e),
            }
        }
    
        Ok(head)
    }
}
