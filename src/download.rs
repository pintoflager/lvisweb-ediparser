use ureq::Agent;
use std::thread;
use std::time::Duration;
use std::io::Read;
use log::{debug, error, info};
use anyhow::{Result, bail};
use std::path::PathBuf;
use rand::distributions::{Alphanumeric, DistString};
use std::fs::{create_dir_all, write};

use super::config::{Config, Seller};

pub fn bulk_download(config: &Config, target_dir: &PathBuf) -> Result<Vec<PathBuf>> {
    let urls = &mut config.seller.iter()
        .flat_map(|s|url_collect(s))
        .collect::<Vec<Vec<String>>>();
    
    create_dir_all(&target_dir)?;
    
    thread::scope(|s| {
        let handles = urls.iter()
            .map(|v|s.spawn(move || {
                let agent: Agent = ureq::AgentBuilder::new()
                .timeout_read(Duration::from_secs(30))
                .timeout_write(Duration::from_secs(60))
                .build();
            
                // If first url fails try the next one and so on
                let (response, url) = try_urls(agent, v)?;
                
                if !response.has("Content-Length") {
                    panic!("Url {} is missing content length header", url)
                }

                let len: usize = response.header("Content-Length")
                    .unwrap()
                    .parse()
                    .expect("Failed to parse content length from Content-Length header");
                
                let mut buf: Vec<u8> = Vec::with_capacity(len);
                response.into_reader().read_to_end(&mut buf).expect("Failed to read response bytes");

                let mut target_file = target_dir.to_owned();
                let randy = Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
                let target_name = match url.split('/').last() {
                    Some(s) => format!("{}-{}", randy, s),
                    None => randy,
                };
                target_file.push(target_name);

                write(&target_file, buf.as_slice()).expect(
                    "Failed to write downloaded content to file"
                );

                info!("Downloaded {} to {}", url, target_file.display());

                Ok::<PathBuf, String>(target_file)
            }))
            .collect::<Vec<_>>();


        let mut results = vec![];

        for h in handles {
            match h.join() {
                Ok(r) => match r {
                    Ok(p) => results.push(p),
                    Err(e) => {
                        error!("Download error: {}", e);
                        continue
                    },
                },
                Err(e) => bail!("Threads are tangled: {}",
                    e.downcast::<String>().unwrap()),
            }
        }

        Ok(results)
    })
}

fn try_urls(agent: Agent, urls: &Vec<String>) -> Result<(ureq::Response, String), String> {
    for u in urls {
        debug!("Trying to download from {}...", &u);

        let call = agent.get(u)
            .call()
            .map_err(|e|format!("Failed to get content from url {}: {}", u, e));

        match call {
            Ok(r) => return Ok((r, u.to_owned())),
            Err(e) => error!("Http call error: {}", e),
        }
    }

    Err(String::from("Failed to download from any of the provided urls"))
}

fn url_collect(seller: &Seller) -> Vec<Vec<String>> {
    // Some urls have tokens. At the moment only {mmyy} for 2 digit month
    // and year token is used.
    let mut urls = vec![];

    if let Some(ref v) = seller.lv { url_ext(&mut urls, v) }
    if let Some(ref v) = seller.iv { url_ext(&mut urls, v) }
    if let Some(ref v) = seller.sa { url_ext(&mut urls, v) }
    if let Some(ref v) = seller.te { url_ext(&mut urls, v) }
    if let Some(ref v) = seller.ky { url_ext(&mut urls, v) }

    urls
}

fn url_ext(urls: &mut Vec<Vec<String>>, ext: &Vec<Vec<String>>) {
    let d = chrono::Utc::now();
    let mmyy = format!("{}", d.format("%m%y"));

    for v in ext {
        let mut add = vec![];

        for u in v {
            add.push(u.replace("{mmyy}", &mmyy));
        }

        urls.push(add);   
    }
}
