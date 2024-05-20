use std::fs::create_dir_all;
use std::{fs::read_to_string, path::PathBuf};
use std::env;

use anyhow::{anyhow, bail, Result};
use log::warn;
use serde::Deserialize;

use super::utils::Lang;


#[derive(Debug, Clone, Deserialize)]
pub struct Seller {
    pub id: String,
    pub name: String,
    pub lv: Option<Vec<Vec<String>>>,
    pub iv: Option<Vec<Vec<String>>>,
    pub sa: Option<Vec<Vec<String>>>,
    pub te: Option<Vec<Vec<String>>>,
    pub ky: Option<Vec<Vec<String>>>
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImportTargets {
    pub json: bool,
    pub sqlite: bool,
    pub search: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub vat_percent: f64,
    pub lang_codes: Vec<Lang>,
    pub import: ImportTargets,
    pub seller: Vec<Seller>,
    #[serde(skip)]
    pub dir: PathBuf,
}

impl Config {
    pub fn new() -> Result<Self> {
        let dir = match env::args().nth(1) {
            Some(p) => {
                let path = match p.eq("example") {
                    true => {
                        create_dir_all("./example/tester/uploads").map_err(|e|
                            anyhow!("Unable to create example uploads directory: {}", e)
                        )?;

                        let to = PathBuf::from("./example/tester/config.toml");

                        if !to.is_file() {
                            std::fs::copy("./example/config.example.toml", to)
                                .map_err(|e|anyhow!("Unable to copy example config file: {}", e))?;
                        }

                        let to = PathBuf::from("./example/tester/uploads/abcd12345.txt");

                        if !to.is_file() {
                            std::fs::copy("./example/discount.example.txt", to)
                                .map_err(|e|anyhow!("Unable to copy example discount file: {}", e))?;
                        }

                        PathBuf::from("./example/tester")
                    },
                    false => PathBuf::from(p),
                };


                match path.is_dir() {
                    true => path,
                    false => {
                        let mut pwd = PathBuf::from(".");
                        pwd.push(path);
                        pwd
                    }
                }
            },
            None => {
                warn!("No command arguments given. Using current directory as default.");    
                PathBuf::from(".")
            }
        };

        // Make sure config file is in the given directory
        let mut config_file = dir.to_owned();
        config_file.push("config.toml");

        if ! config_file.is_file() {
            bail!("Unable to find config file from {:?}", config_file)
        }

        let s = read_to_string(config_file).map_err(|e|
            anyhow!("Unable to read config file to string: {}", e)
        )?;
    
        let mut config = toml::from_str::<Self>(&s).map_err(|e|
            anyhow!("Unable to read config file as toml: {}", e)
        )?;

        if config.import.search && !config.import.sqlite {
            bail!("Search index importing requires sqlite import to be enabled.")
        }

        config.dir = dir;

        Ok(config)
    }
}
