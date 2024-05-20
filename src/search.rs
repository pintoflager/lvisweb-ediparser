use std::collections::HashMap;
use anyhow::{anyhow, Result};
use log::debug;
use rusqlite::{Connection, params};
use serde::Serialize;

use crate::utils::Category;
use super::config::Config;

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct DbProductSearch {
    pub lang: i8,
    pub seller_id: String,
    pub product_id: String,
    pub body: String
}

pub fn search_index_builder(conf: &Config, db_conn: &mut Connection) -> Result<()> {
    // Get sellers who are still active on the config
    let active_sellers = conf.seller.iter()
        .map(|c| (c.id.to_owned(), c.name.to_owned()))
        .collect::<HashMap<String, String>>();

    let active_ids = active_sellers.to_owned()
        .into_keys()
        .collect::<Vec<String>>()
        .join(", ");

    for (k, v) in Category::mapper() {
        // TODO: does not delete products that were removed from catalog though.
        // Products of obsolete suppliers should be deleted
        db_conn.execute(
            &format!("delete from search_{} where seller_id not in ({})", k, active_ids),
            []
        ).map_err(|e|anyhow!("Failed to delete obsolete {} search rows: {}", k, e))?;

        // Load current search index for category
        let index_rows = query_search_index(&db_conn, k)?;
        
        // Connection to DB of current category
        let translation_rows = query_search_index_translations(&db_conn, k, &active_sellers)?;
        
        // Loop products from catalog and run insert or update on the
        // search index
        let tx = db_conn.transaction()?;

        for i in translation_rows {
            // Update if we have changes, insert if missing
            match index_rows.iter().find(|s|
                s.seller_id.eq(&i.seller_id) && s.product_id.eq(&i.product_id)
            ) {
                Some(s) => if s.ne(&i) {
                    tx.execute(
                        &format!("update search_{} set body = ?3 \
                            where search.seller_id = ?1 and search.product_id = ?2", k),
                        params!(&s.seller_id, &s.product_id, &i.body)
                    ).map_err(|e|anyhow!("Search index DB row update error: {}", e))?;
                },
                None => {
                    tx.execute(
                        &format!("insert into search_{} (seller_id, product_id, lang, \
                            body) values (?1, ?2, ?3, ?4)", k),
                        params!(&i.seller_id, &i.product_id, &i.lang, &i.body)
                    ).map_err(|e|anyhow!("Search index DB write error: {}", e))?;
                }
            }
        }

        tx.commit()?;
        
        debug!("Optimizing {} search indexes...", k);

        db_conn.execute(
            &format!("insert into search_{}(search_{}) VALUES('optimize')", k, k), []
        ).map_err(|e|anyhow!("Failed to optimize search index for {}: {}", v, e))?;
    }

    Ok(())
}

fn query_search_index<T>(db_conn: &Connection, table: T) -> Result<Vec<DbProductSearch>>
where T: AsRef<str> {
    let mut stm = db_conn.prepare(
        &format!("select * from search_{}", table.as_ref())
    )?;

    stm.query_map([], |r| {
        Ok(DbProductSearch {
            lang: r.get(0)?,
            seller_id: r.get(1)?,
            product_id: r.get(2)?,
            body: r.get(3)?,
        })
    }).and_then(Iterator::collect)
    .map_err(|e|anyhow!("Failed to query search index: {}", e))
}

fn query_search_index_translations<T>(db_conn: &Connection, table: T, active_sellers: &HashMap<String, String>)
-> Result<Vec<DbProductSearch>>
where T: AsRef<str> {
    let mut stm = db_conn.prepare(
        &format!("select substr(id, 0, 13), substr(id, 13, 7), \
        lang, name, description, tags, code \
        from product_{}_t", table.as_ref()
    ))?;

    stm.query_map([], |r| {
        let seller_id: String = r.get(0)?;
        let mut name = r.get_ref(3)?.as_str()?.to_string();

        // Prefix seller name to product name
        if let Some(n) = active_sellers.get(&seller_id) {
            name = name + ", " + n;
        }
        
        // Should be present, could be an empty string though.
        let mut body = match r.get_ref(4)?.as_str() {
            Ok(s) => match s.is_empty() {
                true => name,
                false => name + ", " + s
            },
            Err(_) => name,
        };

        // Tags are optional, append to description
        if let Ok(o) = r.get_ref(5)?.as_str_or_null() {
            if let Some(s) = o {
                body = body + ", " + s;
            }
        }

        // Search code is optional, append to description
        if let Ok(o) = r.get_ref(6)?.as_str_or_null() {
            if let Some(s) = o {
                body = body + ", " + s;
            }
        }

        Ok(DbProductSearch {
            lang: r.get(2)?,
            seller_id,
            product_id: r.get(1)?,
            body
        })
    }).and_then(Iterator::collect)
    .map_err(|e|anyhow!("Failed to query search translations: {}", e))
}
