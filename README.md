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

*ADD CLI PART*
