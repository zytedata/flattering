# Flattering

&nbsp;

<p align="center">
<img src="/images/flatlogo.png" alt="Flatteting" title="Flatteting" />
</p>

Flattering is the tool to flatten, format, and export any JSON-like data to CSV (or any other output), no matter how complex or mixed the data is.

So, items like this:

```yaml
{
    "name": "Product",
    "offers": [{"price": "154.95", "currency": "$"}],
    "sku": 9204,
    "images": [
        "https://m.site.com/i/9204_1.jpg",
        "https://m.site.com/i/9204_2.jpg",
        "https://m.site.com/i/9204_3.jpg"
    ],
    "description": "Custom description\non multiple lines.",
    "additionalProperty": [
        {"name": "size", "value": "XL"}, {"name": "color", "value": "blue"}
    ],
    "aggregateRating": {"ratingValue": 5.0, "reviewCount": 3}
}
```

will look like this:

| <sub>Name</sub>| <sub>Price</sub> </sub> | <sub>Currency</sub> </sub> | <sub>Sku</sub> </sub> | <sub>Images</sub> </sub> | <sub>Description&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</sub>| <sub>AdditionalProperty</sub> </sub> | <sub>RatingValue</sub> </sub> | <sub>ReviewCount</sub>  |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub>| <sub>154.95</sub>| <sub>$</sub>| <sub>9204</sub>| <sub>https://m.site.com/i/9204_1.jpg<br>https://m.site.com/i/9204_2.jpg<br>https://m.site.com/i/9204_3.jpg</sub>| <sub>Custom description<br>on multiple lines.</sub> </sub> | <sub>size: XL<br>color:blue</sub> </sub> | <sub>5</sub> </sub> | <sub>3</sub>  |

&nbsp;

## Contents

- [Flattering](#flattering)
  - [Contents](#contents)
  - [Quickstart](#quickstart)
  - [CLI](#cli)
  - [What you can do](#what-you-can-do)
    - [1. Flatten data](#1-flatten-data)
    - [2. Rename columns](#2-rename-columns)
    - [3. Format data](#3-format-data)
    - [4. Filter columns](#4-filter-columns)
    - [5. Order columns](#5-order-columns)
    - [6. Process invalid data](#6-process-invalid-data)
    - [7. Process complex data](#7-process-complex-data)
    - [8. Export data](#8-export-data)
  - [Arguments](#arguments)
    - [StatsCollector](#statscollector)
    - [Exporter](#exporter)
  - [Requirements](#requirements)

&nbsp;

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

Also, stats and **items could be processed one by one** (use `append=True` to append rows, if needed):

```python
item_list = [{"some_field": "some_value", "another_field": [1, 2, 3]}]
sc = StatsCollector()
[sc.process_object(x) for x in item_list]
exporter = Exporter(sc.stats["stats"], sc.stats["invalid_properties"])
exporter.export_csv_headers("example.csv")
for item in item_list:
    exporter.export_csv_row(item, "example.csv", append=True)
```

When you provide the filename, the file will be opened to write/append automatically. If you want to open the file manually or write to any other form of `StringIO`, `TextIO`, etc. - check the [8. Export data](#8-export-data) section.


## CLI

Plus, you can use the tool through CLI:

```bash
flattering --path="example.json" --outpath="example.csv"
```
CLI supports all the same parameters, you can get a complete list using the `flattering -h` command.

&nbsp;

## What you can do

### 1. Flatten data

Let's pick an initial item to explain what parameters and formatting options do.

```yaml
{
    "name": "Product",
    "offers": [{"price": "154.95", "currency": "$"}],
    "sku": 9204,
    "images": [
        "https://m.site.com/i/9204_1.jpg",
        "https://m.site.com/i/9204_2.jpg",
        "https://m.site.com/i/9204_3.jpg"
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

| <sub>name</sub> | <sub>offers0->price</sub> | <sub>offers0->currency</sub> | <sub>sku</sub> | <sub>images0</sub> | <sub>images1</sub> | <sub>images2</sub> | <sub>description</sub> | <sub>additionalProperty0->name</sub> | <sub>additionalProperty0->value</sub> | <sub>additionalProperty1->name</sub> | <sub>additionalProperty1->value</sub> | <sub>aggregateRating->ratingValue</sub> | <sub>aggregateRating->reviewCount</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub> | <sub>154.95</sub> | <sub>$</sub> | <sub>9204</sub> | <sub>https://m.site.com/i/9204_1.jpg</sub> | <sub>https://m.site.com/i/9204_2.jpg</sub> | <sub>https://m.site.com/i/9204_3.jpg</sub> | <sub>Custom description<br>on multiple lines.</sub> | <sub>size</sub> | <sub>XL</sub> | <sub>color</sub> | <sub>blue</sub> | <sub>5.0</sub> | <sub>3</sub> |

&nbsp;

### 2. Rename columns

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

| <sub>Name</sub> | <sub>Price</sub> | <sub>Currency</sub> | <sub>Sku</sub> | <sub>Images[0]</sub> | <sub>Images[1]</sub> | <sub>Images[2]</sub> | <sub>Description</sub> | <sub>AdditionalProperty[0]->name</sub> | <sub>AdditionalProperty[0]->value</sub> | <sub>AdditionalProperty[1]->name</sub> | <sub>AdditionalProperty[1]->value</sub> | <sub>RatingValue</sub> | <sub>ReviewCount</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub> | <sub>154.95</sub> | <sub>$</sub> | <sub>9204</sub> | <sub>https://m.site.com/i/9204_1.jpg</sub> | <sub>https://m.site.com/i/9204_2.jpg</sub> | <sub>https://m.site.com/i/9204_3.jpg</sub> | <sub>Custom description<br>on multiple lines.</sub> | <sub>size</sub> | <sub>XL</sub> | <sub>color</sub> | <sub>blue</sub> | <sub>5.0</sub> | <sub>3</sub> |

&nbsp;

### 3. Format data

Better, but images take too much place. Let's **group them in a single cell**, using the name of the field and `field_options`. Fields could be `grouped` (all data in a single cell), `named` (create columns based on an object property), or both.

```python
options = {"images": {"named": False, "grouped": True}}
exporter = Exporter(
    sc.stats["stats"],
    sc.stats["invalid_properties"],
    headers_renaming=renaming,
    field_options=options)
```

| <sub>Name</sub> | <sub>Price</sub> | <sub>Currency</sub> | <sub>Sku</sub> | <sub>Images</sub> | <sub>Description</sub> | <sub>AdditionalProperty[0]->name</sub> | <sub>AdditionalProperty[0]->value</sub> | <sub>AdditionalProperty[1]->name</sub> | <sub>AdditionalProperty[1]->value</sub> | <sub>RatingValue</sub> | <sub>ReviewCount</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub> | <sub>154.95</sub> | <sub>$</sub> | <sub>9204</sub> | <sub>https://m.site.com/i/9204_1.jpg<br>https://m.site.com/i/9204_2.jpg<br>https://m.site.com/i/9204_3.jpg</sub> | <sub>Custom description<br>on multiple lines.</sub> | <sub>size</sub> | <sub>XL</sub> | <sub>color</sub> | <sub>blue</sub> | <sub>5.0</sub> | <sub>3</sub> |

&nbsp;

Looks even better, but we still have a lot of `additionalProperty` columns. Let's make them `named`, by using `name` property as the name of the column to make it better:

```python
options = {
    "images": {"named": False, "grouped": True},
    "additionalProperty": {
        "named": True, "name": "name", "grouped": False
    }
}
```
| <sub>Name</sub> | <sub>Price</sub> | <sub>Currency</sub> | <sub>Sku</sub> | <sub>Images</sub> | <sub>Description</sub> | <sub>Size</sub> | <sub>Color</sub> | <sub>RatingValue</sub> | <sub>ReviewCount</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub> | <sub>154.95</sub> | <sub>$</sub> | <sub>9204</sub> | <sub>https://m.site.com/i/9204_1.jpg<br>https://m.site.com/i/9204_2.jpg<br>https://m.site.com/i/9204_3.jpg</sub> | <sub>Custom description<br>on multiple lines.</sub> | <sub>XL</sub> | <sub>blue</sub> | <sub>5.0</sub> | <sub>3</sub> |

&nbsp;

Now we have a column with a value for each `additionalProperty`. But if you don't need separate columns for that, you can go even further and format them as both `named` and `grouped`:

```python
"additionalProperty": {
    "named": True, "name": "name", "grouped": True
}
```

| <sub>Name</sub> | <sub>Price</sub> | <sub>Currency</sub> | <sub>Sku</sub> | <sub>Images</sub> | <sub>Description</sub> | <sub>AdditionalProperty</sub> | <sub>RatingValue</sub> | <sub>ReviewCount</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub> | <sub>154.95</sub> | <sub>$</sub> | <sub>9204</sub> | <sub>https://m.site.com/i/9204_1.jpg<br>https://m.site.com/i/9204_2.jpg<br>https://m.site.com/i/9204_3.jpg</sub> | <sub>Custom description<br>on multiple lines.</sub> | <sub>size: XL<br>color: blue</sub> | <sub>5.0</sub> | <sub>3</sub> |

&nbsp;

### 4. Filter columns

Also, let's assume we don't really need `ratingValue` and `reviewCount` in this export, so we want to filter them with `headers_filters`:

```python
filters = [r".*ratingValue.*", ".*reviewCount.*"]
exporter = Exporter(
    sc.stats["stats"],
    sc.stats["invalid_properties"],
    headers_renaming=renaming,
    headers_filters=filters,
    field_options=options
)
```
It's important to remember that filters are regular expressions and work with the initial headers, so we're replacing `aggregateRating->ratingValue` and `aggregateRating->reviewCount` here.

| <sub>Name</sub> | <sub>Price</sub> | <sub>Currency</sub> | <sub>Sku</sub> | <sub>Images</sub> | <sub>Description</sub> | <sub>AdditionalProperty</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub> | <sub>154.95</sub> | <sub>$</sub> | <sub>9204</sub> | <sub>https://m.site.com/i/9204_1.jpg<br>https://m.site.com/i/9204_2.jpg<br>https://m.site.com/i/9204_3.jpg</sub> | <sub>Custom description<br>on multiple lines.</sub> | <sub>size: XL<br>color: blue</sub> |

&nbsp;

### 5. Order columns

And, to add a final touch, let's reorder the headers with `headers_order`. For example, I want `Name` and `Sku` as the first two columns:

```python
order = ["name", "sku"]
exporter = Exporter(
    sc.stats["stats"],
    sc.stats["invalid_properties"],
    headers_renaming=renaming,
    headers_filters=filters,
    headers_order=order,
    field_options=options
)
```
All headers present in the `headers_order` list will be ordered, and other headers will be provided in the natural order they appear in your data. Also, we're sorting initial headers, so using `name` and `sku` in lowercase.

| <sub>Name</sub> | <sub>Sku</sub> | <sub>Price</sub> | <sub>Currency</sub> | <sub>Images</sub> | <sub>Description</sub> | <sub>AdditionalProperty</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| <sub>Product</sub> | <sub>9204</sub> | <sub>154.95</sub> | <sub>$</sub> | <sub>https://m.site.com/i/9204_1.jpg<br>https://m.site.com/i/9204_2.jpg<br>https://m.site.com/i/9204_3.jpg</sub> | <sub>Custom description<br>on multiple lines.</sub> | <sub>size: XL<br>color: blue</sub> |

&nbsp;

### 6. Process invalid data

If your input has mixed types or invalid data, it could be hard to flatten it properly. So, you can decide - either `skip` such columns or `stringify` them.

For example, here the property changed type from `dict` to `list`:

```python
item_list = [
    {"a": "a_1", "b": {"c": "c_1"}},
    {"a": "a_2", "b": [1, 2, 3]}
]
sc = StatsCollector()
sc.process_items(item_list)
exporter = Exporter(sc.stats["stats"], sc.stats["invalid_properties"])
exporter.export_csv_full(item_list, "example.csv")
```

By default, invalid properties would be stringified, so you'll get:
| <sub>a</sub> | <sub>b</sub> |
| :--- | :--- |
| <sub>a_1</sub> | <sub>{'c': 'c_1'}</sub> |
| <sub>a_2</sub> | <sub>some_value</sub> |

&nbsp;

But if you want to skip them, you could set `stringify_invalid` parameter to `False`. It works at all level of nesting, and will affect only the invalid property, so items like this:

```python
item_list = [
    {"a": "a_1", "b": {"c": "c_1", "b": "b_1"}},
    {"a": "a_1", "b": {"c": "c_2", "b": [1, 2, 3]}},
]
sc = StatsCollector()
sc.process_items(item_list)
exporter = Exporter(
    sc.stats["stats"],
    sc.stats["invalid_properties"],
    stringify_invalid=False
)
exporter.export_csv_full(item_list, "example.csv")
```

Will export like this:

| <sub>a</sub> | <sub>b->c</sub> |
| :--- | :--- |
| <sub>a_1</sub> | <sub>c_1</sub> |
| <sub>a_1</sub> | <sub>c_2</sub> |


&nbsp;

### 7. Process complex data

Following the nesting, you can export and format data with any amount of nested levels. So, let's create a bit unrealistic item with multiple levels, arrays of arrays, and so on:

```yaml
{
    "a": {
        "nested_a": [[
            {
                "2x_nested_a": {
                    "3x_nested_a": [
                        {"name": "parameter1", "value": "value1"},
                        {"name": "parameter2", "value": "value2"},
                    ]
                }
            },
        ]],
        "second_nested_a": "some_value",
    }
}
```

If we try to flatten it as is, it will work. However, headers will be a bit questionable, so let's show it as a code:

```python
[
    "a->nested_a[0][0]->2x_nested_a->3x_nested_a[0]->name",
    "a->nested_a[0][0]->2x_nested_a->3x_nested_a[0]->value",
    "a->nested_a[0][0]->2x_nested_a->3x_nested_a[1]->name",
    "a->nested_a[0][0]->2x_nested_a->3x_nested_a[1]->value",
    "a->second_nested_a",
]
["parameter1", "value1", "parameter2", "value2", "some_value"]
```

But the best part is that we can format data (`grouped`, `named`) on any level, so with a bit of `field_options` magic:

```python
"a->nested_a[0][0]->2x_nested_a->3x_nested_a": {
    "named": True, "name": "name", "grouped": True
}
```

It will look like this:

| <sub>a->nested_a[0][0]->2x_nested_a->3x_nested_a</sub> | <sub>a->second_nested_a</sub> |
| :--- | :--- |
| <sub>parameter1: value1<br>parameter2: value2</sub> | <sub>some_value</sub>


&nbsp;


### 8. Export data

By default, all the data is exported to `.csv`, either in one go:

```python
exporter = Exporter(sc.stats["stats"], sc.stats["invalid_properties"])
exporter.export_csv_full(item_list, "example.csv")
```

or one-by-one:

```python
exporter.export_csv_headers("example.csv")
[exporter.export_csv_row(x, "example.csv", append=True) for x in item_list]
```

Also, you could use any writable input, like `TextIO`, `StringIO`, and so on, so all of the examples below will work:

```python
# StringIO
buffer = io.StringIO()
exporter.export_csv_full(item_list, buffer)

# File objects
with open("example.csv", "w") as f:
    exporter.export_csv_full(item_list, f)

# Path-like objects
filename = tmpdir.join("example")
exporter.export_csv_full(item_list, filename)
```

We plan to support other formats, but for now  you could also get flattened items **one by one** trough `export_item_as_row` method and write them wherever you want:

```python
# [{"property_1": "value", "property_2": {"nested_property": [1, 2, 3]}}]
flattened_items = [exporter.export_item_as_row(x) for x in item_list]
# [['value', '1', '2', '3']]
```

&nbsp;

## Arguments
### StatsCollector

- **named_columns_limit** `int(default=50)` 
  
  How many named columns could be created for a single field. For example, you have a set of objects like `{"name": "color", "value": "blue"}`. If you decide to create a separate column for each `name` ("color", "size", etc.), the limit defines how much data would be collected to make it work. If the limit is hit (too many columns) - no named columns would be created in export. It's required to control memory usage and data size during stats collection (no need to collect stats for 1000 columns if you don't plan to have 1000 columns anyway).

- **cut_separator** `str(default="->")`
  
  Separator to organize values from items to required columns. Used instead of default "`.`" separator. If your properties' names include the separator - replace it with a custom one.

&nbsp;

### Exporter

- **stats** `Dict[str, Header]`
  
  Item stats collected by `StatsCollector` (`stats_collector.stats["stats"]`).

- **invalid_properties** `Dict[str, str]`
  
  Invalid properties data provided by `StatsCollector` (`stats_collector.stats["invalid_properties"]`)

- **stringify_invalid** `bool(default=True)`
  
  If `True` - columns with invalid data would be stringified. If `False` - columns with invalid data would be skipped

- **field_options** `Dict[str, FieldOption]`
    
    Field options to format data.
    - Options could be `named` (`named=True, name="property_name"`), so the exporter will try to create columns based on the values of the property provided in the `"name"` attribute.
    - Options could be `grouped` (`grouped=True`), so the exporter will try to fit all the data for this field into a single cell.
    - Options could be both `named` and `grouped`, so the exporter will try to get data collected for each named property and fit all this data in a single field.

- **array_limits** `Dict[str, int]`
  
  Limit for the array fields to export only first N elements (`{"images": 1}`).

- **headers_renaming** `List[Tuple[str, str]]`
  
   Set of RegExp rules to rename existing item columns (`[".*_price", "regularPrice"]`). The first value is the pattern to replace, while the second one is the replacement.

- **headers_order** `List[str]`
  
  List to sort columns headers. All headers that are present both in this list and actual data - would be sorted. All other headers would be appended in a natural order. Headers should be provided in the form before renaming (`"offers[0]->price"`, not `"Price"`).

- **headers_filters** `List[str]`
  
  List of RegExp statements to filter columns. Headers that match any of these statements would be skipped (`["name.*", "_key"]`).

- **grouped_separator** `str`
  
  Separator to divide values when grouping data in a single cell (if `grouped=True`).

- **cut_separator** `str(default="->")`
  
  Separator to organize values from items to required columns. Used instead of default "`.`" separator. If your properties' names include the separator - replace it with a custom one.
  
- **capitalize_headers**  `bool(default=False)`

  Capitalize fist letter of CSV headers when exporting.

&nbsp;

## Requirements
- Python 3.7+
- Works on Linux, Windows, macOS, BSD
<br><br>
