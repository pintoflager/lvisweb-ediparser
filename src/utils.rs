use serde::{Serialize, Deserialize};
use std::fmt;
use anyhow::{Result, bail};

#[derive(Debug, Serialize, PartialEq, Clone, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Category {
    #[default]
    Unset,
    #[serde(rename = "lv")]
    WaterAndHeating,
    #[serde(rename = "iv")]
    Ventilation,
    #[serde(rename = "sa")]
    Electricity,
    #[serde(rename = "te")]
    Industrial,
    #[serde(rename = "ky")]
    Refrigeration
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_name())
    }
}

impl Category {
    pub fn from_edi_str(val: &str) -> Result<Self> {
        let c = match val {
            "L" => Category::WaterAndHeating,
            "I" => Category::Ventilation,
            "S" => Category::Electricity,
            "P" => Category::Industrial,
            "K" => Category::Refrigeration,
            x => bail!("Invalid EDI category '{}' provided", x),
        };

        Ok(c)
    }
    pub fn to_name(&self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::WaterAndHeating => "lv",
            Self::Ventilation => "iv",
            Self::Electricity => "sa",
            Self::Industrial => "te",
            Self::Refrigeration => "ky",
        }
    }
    pub fn mapper() -> [(&'static str, Self); 5] {
        [
            (Self::WaterAndHeating.to_name(), Self::WaterAndHeating),
            (Self::Ventilation.to_name(), Self::Ventilation),
            (Self::Electricity.to_name(), Self::Electricity),
            (Self::Industrial.to_name(), Self::Industrial),
            (Self::Refrigeration.to_name(), Self::Refrigeration),
        ]
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Lang {
    #[default]
    Fin,
    Swe,
    Eng,
    Nor,
}

impl fmt::Display for Lang {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_name())
    }
}

impl Lang {
    pub fn to_index(&self) -> usize {
        match self {
            Self::Fin => 1,
            Self::Swe => 2,
            Self::Eng => 3,
            Self::Nor => 4

        }
    }
    pub fn to_name(&self) -> &'static str {
        match self {
            Self::Fin => "fin",
            Self::Swe => "swe",
            Self::Eng => "eng",
            Self::Nor => "nor"

        }
    }
    pub fn from_name<T>(val: T) -> Result<Self> where T: AsRef<str> {
        for (k, v) in Self::mapper() {
            if k.eq(&val.as_ref().to_lowercase()) {
                return Ok(v)
            }
        }

        let names = Self::mapper().into_iter()
            .map(|(k, _)| k)
            .collect::<Vec<&'static str>>();

        bail!("Invalid language name {} provided. Expected one of: [{}]",
            val.as_ref(), names.join(", "))
    }
    pub fn mapper() -> [(&'static str, Self); 4] {
        [
            (Self::Fin.to_name(), Self::Fin),
            (Self::Swe.to_name(), Self::Swe),
            (Self::Eng.to_name(), Self::Eng),
            (Self::Nor.to_name(), Self::Nor)
        ]
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum Operation {
    #[serde(rename = "a")]
    Added,
    #[serde(rename = "m")]
    Modified,
    #[serde(rename = "d")]
    Destroyed,
    #[default]
    Empty,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Added => "add",
            Self::Modified => "mod",
            Self::Destroyed => "del",
            Self::Empty => "-"

        };

        write!(f, "{}", name)
    }
}

impl Operation {
    pub fn from_str(val: &str) -> Result<Self> {
        let c = match val {
            "1" => Self::Added,
            "2" => Self::Modified,
            "3" => Self::Destroyed,
            x => bail!("Operation has to be number between 1 and 3. Found '{}'", x),
        };

        Ok(c)
    }
    pub fn to_name(&self) -> &'static str {
        match  &self {
            Self::Added => "add",
            Self::Destroyed => "del",
            Self::Modified => "mod",
            _ => panic!("Can't name an empty operation.")
        }
    }
}
