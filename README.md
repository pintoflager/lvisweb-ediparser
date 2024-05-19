# LVISweb EDI parser
EDI stands for electronic data interchange and in this specific case we're dealing with product and price catalog information related to HVAC stuff, pipes and fittings, electrical components, wires, automation, so on, so on.

In Finland multiple big wholesalers in the industry have standardized their product and price catalogs, their communication related to orders, quotes and invoices into a EDI format. To best of my understanding they have formed a mutually owned company "LVISnet" to maintain this EDI standard + messaging between the parties.

Many of these companies span across scandinavia so this EDI parser might even work in other countries as well.

I don't know the ins and outs of this system. I wrote whats available here with the help of partial documentation that was scattered all over the internet, mostly on the websites of the wholesalers involved. If you're interested read more through [this link](https://opuscapita.com/fi/lvisnet/) I found by using a well established search engine.

[Discounts EDI](/example/discount.example.toml) file the wholesalers seem to provide free of charge for their customers (resellers) but resellers who'd like to have access to EDI messaging (invoices, orders, quotes, ..) have to pay monthly to LVISnet. And of cource purchase a licence for some commercial ERP software that knows how to handle these messages.

# What is it used for
Basic concept is following:
- Wholesalers provide updated EDI product and price catalog on a regular basis, free to download for everybody
- Each reseller may receive their personal discounts file in EDI format (and new version of it if discounts change)
- Combining information from these three sources reseller can build a personal product-price catalog
- Reseller adds profit margin and IVA (if applicable) and here we have the price of a toilet seat Average Bob pays when plumber is called

The resellers can have up to date information on the purchase price of identical items sold by different wholesalers they do business with. Comparing prices is easier and doesn't require repetitive calls or emails to multiple suppliers.

# What does it do
Compiled binary downloads EDI sources for products and prices from urls defined in `config.toml` then it decompresses the received (zip) archives, validates that the archived file is valid utf-8 (tries to convert if it's not) and reads through the file line by line updating successfully extracted data into a `SQLite` database and / or categorized `JSON` files.

# How to use
First of all this was never intented to be used in Windows environment. **Only tested on Linux**.

[Rust](https://www.rust-lang.org/tools/install) along with its package manager cargo, has to be installed.

Just clone this project, `cd` into it and execute:
```bash
cargo run example
```
Which does following:
- creates a `tester` directory under [example](/example) directory
- copies and renames [config.example.toml](/example/config.example.toml) into it as `config.toml`
- adds a fake [discount.example.txt](/example/discount.example.txt) into `uploads` directory
- pulls all EDI files defined in `config.toml` from the web, unzips and converts them to utf-8 format
- iterates all EDI files line by line importing valid items into
    - `SQLite` databases `sellers.db` and `buyers.db`
    - `JSON` files grouped under product category subdirectories

If all went smoothly you'll end up with following `example/listener` dir contents
```bash
ls -la example/tester
drwxr-xr-x 6 lvisweb lvisweb      4096 19. 5. 15:47 .
drwxr-xr-x 3 lvisweb lvisweb      4096 19. 5. 15:42 ..
-rw-r--r-- 1 lvisweb lvisweb    421888 19. 5. 15:47 buyers.db
-rw-r--r-- 1 lvisweb lvisweb      4558 19. 5. 15:42 config.toml
drwxr-xr-x 2 lvisweb lvisweb      4096 19. 5. 15:42 downloads
drwxr-xr-x 2 lvisweb lvisweb      4096 19. 5. 15:47 edi
-rw-r--r-- 1 lvisweb lvisweb   3761849 19. 5. 15:47 import.log
drwxr-xr-x 7 lvisweb lvisweb      4096 19. 5. 15:46 sellers
-rw-r--r-- 1 lvisweb lvisweb 595034112 19. 5. 15:47 sellers.db
drwxr-xr-x 2 lvisweb lvisweb      4096 19. 5. 15:46 uploads
```

If you want to see `JSON` files created as well, modify `config.toml` import value to:
```toml
[import]
json = true
sqlite = true
```

Then delete `sellers` directory, copy example discount file to uploads dir again (optional) and execute
```bash
cargo run example
```

Now you should see more bloated `example/listener/...` dir:
```bash
ls -la example/tester/sellers/003710712079/products
drwxr-xr-x 2 lvisweb lvisweb     4096 19. 5. 16:00 .
drwxr-xr-x 5 lvisweb lvisweb     4096 19. 5. 15:58 ..
-rw-r--r-- 1 lvisweb lvisweb  7070214 19. 5. 16:00 iv.fin.json
-rw-r--r-- 1 lvisweb lvisweb  3931861 19. 5. 16:00 ky.fin.json
-rw-r--r-- 1 lvisweb lvisweb 30498977 19. 5. 16:00 lv.fin.json
-rw-r--r-- 1 lvisweb lvisweb 33353577 19. 5. 16:00 sa.fin.json
```

# Query something
Following example gives you discount percent and price on a product `3125463` from a wholesaler `003718191538` using our example discounts. Single result is returned since only one discount file was uploaded. Other suppliers have the same product but below query ignores them since discounted price cannot be queried.

Open sqlite client:
```bash
sqlite3 example/tester/sellers.db
```

Attach buyers DB:
```sql
attach 'example/tester/buyers.db' as buyers;
```

```sql
select main.product_iv_t.name, main.product_iv_t.description, main.products_iv.discount_group, 
    main.prices_iv.price, main.prices_iv.unit, main.sellers.name, buyers.discounts.percent_1, buyers.buyers.vat_percent
from main.products_iv
inner join main.products on main.products_iv.product_id = main.products.id
inner join main.prices_iv on main.prices_iv.id = main.products_iv.seller_id || main.products.id
inner join main.sellers on main.sellers.id = main.products_iv.seller_id
inner join main.product_iv_t on main.product_iv_t.id = main.products_iv.id || '1'
inner join buyers.buyers on buyers.buyers.buyer_id = '1234567'
inner join buyers.discounts on buyers.discounts.id = buyers.buyers.buyer_id || main.products_iv.seller_id || main.products_iv.discount_group
    and buyers.discounts.price_group = main.prices_iv.price_group
where main.products.id = '8629087'
order by main.sellers.name;
```

Should return:  
```
TULOILMALAATIKKO|ALVE 400-200-160-B|I8631B|256.6|KPL|Ahlsell Oy|55.0|24.0
```
...where `I8631B` is the discount group matching the requested product number `8629087`  
`TULOILMALAATIKKO|ALVE 400-200-160-B|`**I8631B**`|256.6|KPL|Ahlsell Oy|55.0|24.0`

...which you can verify to be correct by searching discount group `I8631B` from `discount.example.txt`:  
```
RI8631B8629007 -8629048         P��TELAITTEET SWEGON LB                 01000005500000000000
```
Read it like this:  
`R`**I8631B**`8629007 -8629048         P��TELAITTEET SWEGON LB                 0100000`**55**`00000000000`

...so now we know buyer `1234567` of vendor `003710712079` has discount percent of `55.0` for the queried product `TULOILMALAATIKKO ALVE 400-200-160-B`  
...which leaves us pondering the unit price, `|I8631B|`**256.6**`|KPL|`  
...and to know the unit price we count `(100.0 - `**55.0**`) / 100.0 * `**256,6**  
...to get result `115,47` which might even be correct.

If you want to see which vendors had the product `8629087` run sql query:
```sql
select main.product_iv_t.name, main.product_iv_t.description, main.products_iv.discount_group, main.sellers.name
from main.products_iv
inner join main.products on main.products_iv.product_id = main.products.id
inner join main.sellers on main.sellers.id = main.products_iv.seller_id
inner join main.product_iv_t on main.product_iv_t.id = main.products_iv.id || '1'
where product_id = '8629087';
```

Expected result looks something like this:
```
TULOILMALAATIKKO|ALVE 400-200-160-B|I8631B|Ahlsell Oy
TASAUSLAATIKKO SWEGON|ALV 400-200-160B WALL|031102|Dahl Oy
TULOILMALAATIKKO SWEGON|ALVe 400-200-160-B WALL|AT5|Onninen Oy
```

...which tells us that three vendors had the product in their catalog.
