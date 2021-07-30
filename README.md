# Flattering

Flattering is the tool to flatten, format and export any JSON-like data, no matter how complex or mixed it is.

So, items like this:

```yaml
{
    "name": "Product",
    "offers": [{"price": "154.95", "currency": "$"}],
    "sku": 9204,
    "images": [
        "https://m.site.com/i/9204_1.jpg",
        "https://m.site.com/i/9204_2.jpg",
        "https://m.site.com/i/9204550_3.jpg"
    ],
    "description": "Custom description\non multiple lines.",
    "additionalProperty": [
        {"name": "size", "value": "XL"}, {"name": "color", "value": "blue"}
    ],
    "aggregateRating": {"ratingValue": 5.0, "reviewCount": 3}
}
```

will look like this:

| <sub>Name</sub> | <sub>Price</sub>  | <sub>Currency</sub>  | <sub>Sku</sub>  | <sub>Images</sub>  | <sub>Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</sub> | <sub>AdditionalProperty</sub>  | <sub>RatingValue</sub>  | <sub>ReviewCount</sub>  |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub> | <sub>154.95</sub> | <sub>$</sub> | <sub>9204</sub> | <sub>https://m.site.com/i/9204_1.jpg<br>https://m.site.com/i/9204_2.jpg<br>https://m.site.com/i/9204550_3.jpg</sub> | <sub>Custom description<br>on multiple lines.</sub>  | <sub>size: XL<br>color:blue</sub>  | <sub>5</sub>  | <sub>3</sub>  |

## Requirements
- Python 3.6+
- Works on Linux, Windows, macOS, BSD


## Quickstart

Flattering consists of two elements:

- `StatsCollector`, to understand how many columns are required, what headers they'll have, and what data is mixed/invalid (to skip or stringify).
- `Exporter`, to format and beatify the data, fit it in columns, and export it (as `.csv` or flat data).

```python
item_list = [{"some_field": "some_value", "another_field": [1, 2, 3]}]
sc = StatsCollector()
sc.process_items(item_list)
exporter = Exporter(sc.stats["stats"], sc.stats["invalid_properties"])
exporter.export_csv_full(item_list, "example.csv")
```

You could use both parts on the same side or separately. For example, collect stats during a running job, and then provide them (tiny `JSON` with numbers) to the backend when a user wants to export the data.

## CLI

Also, you can use the tool through CLI:

```bash
flattering --path="example.json" --outpath="example.csv"
```
CLI supports all the same parameters, you can get full list using `flattering -h` command.

## What you can do

Let's pick an initial item to explain what parameters and formatting options do.

```yaml
{
    "name": "Product",
    "offers": [{"price": "154.95", "currency": "$"}],
    "sku": 9204,
    "images": [
        "https://m.site.com/i/9204_1.jpg",
        "https://m.site.com/i/9204_2.jpg",
        "https://m.site.com/i/9204550_3.jpg"
    ],
    "description": "Custom description\non multiple lines.",
    "additionalProperty": [
        {"name": "size", "value": "XL"}, {"name": "color", "value": "blue"}
    ],
    "aggregateRating": {"ratingValue": 5.0, "reviewCount": 3}
}
```
If you don't provide any custom options:

```python
item_list = [item]
sc = StatsCollector()
sc.process_items(item_list)
exporter = Exporter(sc.stats["stats"], sc.stats["invalid_properties"])
exporter.export_csv_full(item_list, "example.csv")
```

the export will look like this:

| name | offers0->price | offers0->currency | sku | images0 | images1 | images2 | description | additionalProperty0->name | additionalProperty0->value | additionalProperty1->name | additionalProperty1->value | aggregateRating->ratingValue | aggregateRating->reviewCount |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Product | 154.95 | $ | 9204 | https://m.site.com/i/9204_1.jpg | https://m.site.com/i/9204_2.jpg | https://m.site.com/i/9204550_3.jpg | Custom description\non multiple lines. | size | XL | color | blue | 5.0 | 3 |

Let's make it a bit more readable with `headers_renaming`:

```python
renaming = [
    (r"^offers\[0\]->", ""),
    (r"^aggregateRating->", ""),
    (r"^additionalProperty->(.*)->value", r"\1")
]
exporter = Exporter(
    sc.stats["stats"],
    sc.stats["invalid_properties"],
    headers_renaming=renaming)
```

| Name | Price | Currency | Sku | Images[0] | Images[1] | Images[2] | Description | AdditionalProperty[0]->name | AdditionalProperty[0]->value | AdditionalProperty[1]->name | AdditionalProperty[1]->value | RatingValue | ReviewCount |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Product | 154.95 | $ | 9204 | https://m.site.com/i/9204_1.jpg | https://m.site.com/i/9204_2.jpg | https://m.site.com/i/9204550_3.jpg | Custom description\non multiple lines. | size | XL | color | blue | 5.0 | 3 |

Better, but images take too much place. Let's **group them in a single cell**, using the name of the field and `field_options`. Fields could be `grouped` (all data in a single cell), `named` (create columns based on an object property), or both.

```python
options = {"images": {"named": False, "grouped": True}}
exporter = Exporter(
    sc.stats["stats"],
    sc.stats["invalid_properties"],
    headers_renaming=renaming,
    field_options=options)
```

| Name | Price | Currency | Sku | Images | Description | AdditionalProperty[0]->name | AdditionalProperty[0]->value | AdditionalProperty[1]->name | AdditionalProperty[1]->value | RatingValue | ReviewCount |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Product | 154.95 | $ | 9204 | https://m.site.com/i/9204_1.jpg<br>https://m.site.com/i/9204_2.jpg<br>https://m.site.com/i/9204550_3.jpg | Custom description\non multiple lines. | size | XL | color | blue | 5.0 | 3 |

Looks even better, but we still have a lot of `additionalProperty` columns. Let's make them `named`, by using `name` property as the name of the column to make it better.


