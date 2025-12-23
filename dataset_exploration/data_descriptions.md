# Data Descriptions

## products.csv

### Structure
- Columns: ['product_id', 'created_at', 'product_name']
- Data Types: {'product_id': dtype('int64'), 'created_at': dtype('O'), 'product_name': dtype('O')}
- Shape: (4, 3)

### Description
|              |   count |   unique | top                    |   freq |   mean |       std |   min |    25% |   50% |    75% |   max |
|--------------|---------|----------|------------------------|--------|--------|-----------|-------|--------|-------|--------|-------|
| product_id   |       4 |      nan | nan                    |    nan |    2.5 |   1.29099 |     1 |   1.75 |   2.5 |   3.25 |     4 |
| created_at   |       4 |        4 | 2012-03-19 08:00:00    |      1 |  nan   | nan       |   nan | nan    | nan   | nan    |   nan |
| product_name |       4 |        4 | The Original Mr. Fuzzy |      1 |  nan   | nan       |   nan | nan    | nan   | nan    |   nan |

## orders.csv

### Structure
- Columns: ['order_id', 'created_at', 'website_session_id', 'user_id', 'primary_product_id', 'items_purchased', 'price_usd', 'cogs_usd']
- Data Types: {'order_id': dtype('int64'), 'created_at': dtype('O'), 'website_session_id': dtype('int64'), 'user_id': dtype('int64'), 'primary_product_id': dtype('int64'), 'items_purchased': dtype('int64'), 'price_usd': dtype('float64'), 'cogs_usd': dtype('float64')}
- Shape: (32313, 8)

### Description
|                    |   count |   unique | top                 |   freq |         mean |           std |    min |       25% |       50% |       75% |       max |
|--------------------|---------|----------|---------------------|--------|--------------|---------------|--------|-----------|-----------|-----------|-----------|
| order_id           |   32313 |      nan | nan                 |    nan |  16157       |   9328.1      |   1    |   8079    |  16157    |  24235    |  32313    |
| created_at         |   32313 |    32299 | 2015-03-08 15:09:48 |      2 |    nan       |    nan        | nan    |    nan    |    nan    |    nan    |    nan    |
| website_session_id |   32313 |      nan | nan                 |    nan | 258292       | 132428        |  20    | 144828    | 263554    | 374799    | 472818    |
| user_id            |   32313 |      nan | nan                 |    nan | 215692       | 108402        |  13    | 124135    | 221461    | 310542    | 394273    |
| primary_product_id |   32313 |      nan | nan                 |    nan |      1.39247 |      0.732277 |   1    |      1    |      1    |      2    |      4    |
| items_purchased    |   32313 |      nan | nan                 |    nan |      1.23867 |      0.426274 |   1    |      1    |      1    |      1    |      2    |
| price_usd          |   32313 |      nan | nan                 |    nan |     59.9916  |     17.8088   |  29.99 |     49.99 |     49.99 |     59.99 |    109.98 |
| cogs_usd           |   32313 |      nan | nan                 |    nan |     22.3554  |      6.23862  |   9.49 |     19.49 |     19.49 |     22.49 |     41.98 |

## website_sessions.csv

### Structure
- Columns: ['website_session_id', 'created_at', 'user_id', 'is_repeat_session', 'utm_source', 'utm_campaign', 'utm_content', 'device_type', 'http_referer']
- Data Types: {'website_session_id': dtype('int64'), 'created_at': dtype('O'), 'user_id': dtype('int64'), 'is_repeat_session': dtype('int64'), 'utm_source': dtype('O'), 'utm_campaign': dtype('O'), 'utm_content': dtype('O'), 'device_type': dtype('O'), 'http_referer': dtype('O')}
- Shape: (472871, 9)

### Description
|                    |   count |   unique | top                     |   freq |          mean |           std |   min |    25% |    50% |    75% |    max |
|--------------------|---------|----------|-------------------------|--------|---------------|---------------|-------|--------|--------|--------|--------|
| website_session_id |  472871 |      nan | nan                     |    nan | 236436        | 136506        |     1 | 118218 | 236436 | 354654 | 472871 |
| created_at         |  472871 |   470444 | 2013-11-29 18:04:21     |      3 |    nan        |    nan        |   nan |    nan |    nan |    nan |    nan |
| user_id            |  472871 |      nan | nan                     |    nan | 198038        | 111993        |     1 | 101966 | 199483 | 294433 | 394318 |
| is_repeat_session  |  472871 |      nan | nan                     |    nan |      0.166119 |      0.372188 |     0 |      0 |      0 |      0 |      1 |
| utm_source         |  389543 |        3 | gsearch                 | 316035 |    nan        |    nan        |   nan |    nan |    nan |    nan |    nan |
| utm_campaign       |  389543 |        4 | nonbrand                | 337615 |    nan        |    nan        |   nan |    nan |    nan |    nan |    nan |
| utm_content        |  389543 |        6 | g_ad_1                  | 282706 |    nan        |    nan        |   nan |    nan |    nan |    nan |    nan |
| device_type        |  472871 |        2 | desktop                 | 327027 |    nan        |    nan        |   nan |    nan |    nan |    nan |    nan |
| http_referer       |  432954 |        3 | https://www.gsearch.com | 351237 |    nan        |    nan        |   nan |    nan |    nan |    nan |    nan |

## order_item_refunds.csv

### Structure
- Columns: ['order_item_refund_id', 'created_at', 'order_item_id', 'order_id', 'refund_amount_usd']
- Data Types: {'order_item_refund_id': dtype('int64'), 'created_at': dtype('O'), 'order_item_id': dtype('int64'), 'order_id': dtype('int64'), 'refund_amount_usd': dtype('float64')}
- Shape: (1731, 5)

### Description
|                      |   count |   unique | top                 |   freq |       mean |         std |    min |     25% |      50% |      75% |      max |
|----------------------|---------|----------|---------------------|--------|------------|-------------|--------|---------|----------|----------|----------|
| order_item_refund_id |    1731 |      nan | nan                 |    nan |   866      |   499.841   |   1    |  433.5  |   866    |  1298.5  |  1731    |
| created_at           |    1731 |     1731 | 2012-04-06 11:32:43 |      1 |   nan      |   nan       | nan    |  nan    |   nan    |   nan    |   nan    |
| order_item_id        |    1731 |      nan | nan                 |    nan | 18472.2    | 11438.1     |  57    | 7417    | 19858    | 26900    | 39950    |
| order_id             |    1731 |      nan | nan                 |    nan | 15868.2    |  9096.06    |  57    | 7412    | 17375    | 22539.5  | 32255    |
| refund_amount_usd    |    1731 |      nan | nan                 |    nan |    49.3002 |     4.95602 |  29.99 |   49.99 |    49.99 |    49.99 |    59.99 |

## order_items.csv

### Structure
- Columns: ['order_item_id', 'created_at', 'order_id', 'product_id', 'is_primary_item', 'price_usd', 'cogs_usd']
- Data Types: {'order_item_id': dtype('int64'), 'created_at': dtype('O'), 'order_id': dtype('int64'), 'product_id': dtype('int64'), 'is_primary_item': dtype('int64'), 'price_usd': dtype('float64'), 'cogs_usd': dtype('float64')}
- Shape: (40025, 7)

### Description
|                 |   count |   unique | top                 |   freq |        mean |          std |    min |      25% |      50% |      75% |      max |
|-----------------|---------|----------|---------------------|--------|-------------|--------------|--------|----------|----------|----------|----------|
| order_item_id   |   40025 |      nan | nan                 |    nan | 20013       | 11554.4      |   1    | 10007    | 20013    | 30019    | 40025    |
| created_at      |   40025 |    32299 | 2014-08-01 18:09:43 |      4 |   nan       |   nan        | nan    |   nan    |   nan    |   nan    |   nan    |
| order_id        |   40025 |      nan | nan                 |    nan | 17122       |  9053.77     |   1    |  9871    | 17490    | 24818    | 32313    |
| product_id      |   40025 |      nan | nan                 |    nan |     1.77002 |     1.08556  |   1    |     1    |     1    |     2    |     4    |
| is_primary_item |   40025 |      nan | nan                 |    nan |     0.80732 |     0.394408 |   0    |     1    |     1    |     1    |     1    |
| price_usd       |   40025 |      nan | nan                 |    nan |    48.4325  |     8.01237  |  29.99 |    49.99 |    49.99 |    49.99 |    59.99 |
| cogs_usd        |   40025 |      nan | nan                 |    nan |    18.048   |     3.85682  |   9.49 |    19.49 |    19.49 |    19.49 |    22.49 |

## website_pageviews.csv

### Structure
- Columns: ['website_pageview_id', 'created_at', 'website_session_id', 'pageview_url']
- Data Types: {'website_pageview_id': dtype('int64'), 'created_at': dtype('O'), 'website_session_id': dtype('int64'), 'pageview_url': dtype('O')}
- Shape: (1188124, 4)

### Description
|                     |       count |        unique | top                 |   freq |   mean |    std |   min |    25% |    50% |    75% |              max |
|---------------------|-------------|---------------|---------------------|--------|--------|--------|-------|--------|--------|--------|------------------|
| website_pageview_id | 1.18812e+06 | nan           | nan                 |    nan | 594062 | 342982 |     1 | 297032 | 594062 | 891093 |      1.18812e+06 |
| created_at          | 1.18812e+06 |   1.17196e+06 | 2013-11-29 15:01:02 |      4 |    nan |    nan |   nan |    nan |    nan |    nan |    nan           |
| website_session_id  | 1.18812e+06 | nan           | nan                 |    nan | 244459 | 135620 |     1 | 127786 | 247808 | 362739 | 472871           |
| pageview_url        | 1.18812e+06 |  16           | /products           | 261231 |    nan |    nan |   nan |    nan |    nan |    nan |    nan           |

## _fuzzy_factory_data_dictionary.csv

### Structure
- Columns: ['Table', 'Field', 'Description']
- Data Types: {'Table': dtype('O'), 'Field': dtype('O'), 'Description': dtype('O')}
- Shape: (36, 3)

### Description
|             |   count |   unique | top                                 |   freq |
|-------------|---------|----------|-------------------------------------|--------|
| Table       |      36 |        6 | website_sessions                    |      9 |
| Field       |      36 |       22 | created_at                          |      6 |
| Description |      36 |       34 | Timestamp when the order was placed |      2 |

