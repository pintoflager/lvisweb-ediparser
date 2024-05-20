use rusqlite::{Connection, Result};
use log::warn;

use super::utils::Category;
use super::config::Config;


pub fn init(config: &Config) -> Result<(Connection, Connection)> {
    // Sellers DB
    let mut path = config.dir.to_owned();
    path.push("sellers.db");
    
    let sellers = Connection::open(path)?;

    // Create sellers table
    sellers.execute(
        "create table if not exists sellers (
            id text primary key,
            name text not null unique
        )",
        [],
    )?;

    // Create unit types table
    sellers.execute(
        "create table if not exists units (
            id text primary key
        )",
        [],
    )?;

    // Create languages table
    sellers.execute(
        "create table if not exists languages (
            id integer primary key,
            name text not null unique
        )",
        [],
    )?;

    // Create discount groups table
    sellers.execute(
        "create table if not exists discount_groups (
            id text primary key
        )",
        [],
    )?;

    // Create price groups table
    sellers.execute(
        "create table if not exists price_groups (
            id text primary key
        )",
        [],
    )?;

    // Generic products table
    sellers.execute(
        "create table if not exists products (
            id text primary key,
            category text not null,
            tax_class text null
        )",
        [],
    )?;


    // Tables for each product category
    for (k, v) in Category::mapper().into_iter() {
        // Create translations table
        sellers.execute(
            &format!("create table if not exists product_{k}_t (
                id text primary key,
                lang integer not null,
                name text not null,
                description text not null,
                tags text null,
                code text null
            )"),
            [],
        )?;

        // Create full-text search index table to DB
        if config.import.search {
            let result = sellers.execute(
                &format!("create virtual table search_{k} using fts5 (
                    lang UNINDEXED,
                    seller_id UNINDEXED,
                    product_id,
                    body,
                    tokenize='trigram'
                )"),
                [],
            );

            // Virtual table doesn't have 'if not exists' untill newer versions of sqlite
            // so if this fails lets just presume it was a duplicate error (^^)
            if let Err(e) = result {
                match e.sqlite_error() {
                    Some(c) => warn!("Create DB search table for {} \
                        failed with error code ({}), most likely table already exists.",
                        v, c.extended_code),
                    None => warn!("Create DB search table failed without \
                        error code. How strange is that...")
                }
            }
        }

        // SQLite create product entries table
        sellers.execute(
            &format!("create table if not exists products_{} (
                id text primary key,
                product_id text not null,
                seller_id text not null,
                operation text not null,
                date text not null,
                discount_group text not null,
                unit text not null,
                unit_weight real null,
                unit_volume real null,
                typical_packaging integer null,
                packaging_1 real null,
                packaging_1_discount real null,
                packaging_2 real null,
                packaging_2_discount real null,
                packaging_3 real null,
                packaging_3_discount real null,
                delivery_in_weeks integer null,
                stock_item integer not null,
                ean_code text null,
                usage_unit text null,
                usables_in_unit real not null
            )", k),
            [],
        )?;

        // SQLite create prices table
        sellers.execute(
            &format!("create table if not exists prices_{} (
                id text primary key,
                product_id text not null,
                price_group text not null,
                price real not null,
                date text not null,
                discount_group text not null,
                unit text not null,
                units_incl integer not null,
                packaging_1 real null,
                packaging_1_discount real null,
                packaging_2 real null,
                packaging_2_discount real null,
                packaging_3 real null,
                packaging_3_discount real null,
                usage_unit text null,
                usables_in_unit real not null,
                stock_item integer not null,
                delivery_in_weeks integer null
            )", k),
            [],
        )?;
    }

    // Buyers DB
    let mut path = config.dir.to_owned();
    path.push("buyers.db");
    
    let buyers = Connection::open(path)?;

    // Create buyers table
    buyers.execute(
        "create table if not exists buyers (
            id text primary key,
            uuid text not null unique,
            buyer_id text not null,
            vat_percent real not null,
            name text null
        )",
        [],
    )?;

    // Discounts table
    buyers.execute(
        "create table if not exists discounts (
            id text primary key,
            buyer_id text not null,
            seller_id text not null,
            discount_group text not null,
            price_group text not null,
            percent_1 real not null,
            percent_2 real not null
        )",
        [],
    )?;

    Ok((sellers, buyers))
}

pub fn query_price_groups(conn: &Connection) -> Result<Vec<String>> {
    let mut stm = conn.prepare("select id from price_groups")?;
    
    stm.query_map([], |r| {
        Ok(r.get(0)?)
    }).and_then(Iterator::collect)
}

pub fn query_discount_groups(conn: &Connection) -> Result<Vec<String>> {
    // Load all discount groups so we can compare if discount is needed or not
    let mut stm = conn.prepare("select id from discount_groups")?;
    
    stm.query_map([], |r| {
        Ok(r.get(0)?)
    }).and_then(Iterator::collect)
}
