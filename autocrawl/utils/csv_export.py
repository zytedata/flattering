import json
from collections import defaultdict

from scalpl import Cut


class CSVExporter:
    def __init__(self, named_properties, skip_fields, schema):
        # Insertion-ordered dict
        self.headers_meta = {}
        self.flat_headers = []
        self.skip_fields = skip_fields
        self.named_properties = named_properties
        self.schema = schema

    def process_object(self, prefix, object_value, object_schema):
        for property_name in object_value:
            property_path = f"{prefix}.{property_name}"
            property_type = object_schema["properties"][property_name]["type"]
            if property_type not in {"object", "array"}:
                if self.headers_meta.get(property_path) is None:
                    self.headers_meta[property_path] = {"count": 1}
            elif property_type == "object":
                self.process_object(property_path, object_value[property_name])

    def process_array(self, prefix, array_value, array_schema):
        if self.headers_meta.get(prefix) is None:
            self.headers_meta[prefix] = {"count": 0, "properties": []}
        # Currently there're no array of arrays, so arrays could be ignored
        if array_schema["items"]["type"] not in {"object"}:
            if self.headers_meta[prefix]["count"] < len(array_value):
                self.headers_meta[prefix]["count"] = len(array_value)
        else:
            # TODO Check if object properties are not nested objects
            # Process only the first offer
            if prefix == "offers":
                array_value = array_value[:1]
            if prefix in self.named_properties:
                for element in array_value:
                    property_path = f"{prefix}.{element[self.named_properties[prefix]]}"
                    value_properties = [x for x in element.keys() if x != self.named_properties[prefix]]
                    if property_path in self.headers_meta:
                        continue
                    property_schema = array_schema["items"]["properties"][self.named_properties[prefix]]
                    property_type = property_schema["type"]
                    if property_type not in {"object", "array"}:
                        self.headers_meta[property_path] = {"count": 1, "properties": value_properties}
                    elif property_type == "array":
                        self.process_array(property_path, element, property_schema)
                    else:
                        self.process_object(property_path, element, property_schema)
            else:
                if self.headers_meta[prefix]["count"] < len(array_value):
                    self.headers_meta[prefix]["count"] = len(array_value)
                # Checking manually to keep properties order instead of checking subsets
                for element in array_value:
                    for property_name, property_value in element.items():
                        property_path = f"{prefix}.{property_name}"
                        property_schema = array_schema["items"]["properties"][
                            property_name
                        ]
                        property_type = property_schema["type"]
                        if property_type not in {"object", "array"}:
                            if property_name in self.headers_meta[prefix]["properties"]:
                                continue
                            self.headers_meta[prefix]["properties"].append(property_name)
                        elif property_type == "array":
                            self.process_array(property_path, property_value, property_schema)
                        else:
                            self.process_object(property_path, property_value, property_schema)

    def process_product(self, product):
        for product_field, product_value in product.items():
            if product_field in self.headers_meta:
                continue
            elif product_field in self.skip_fields:
                self.headers_meta[product_field] = {"count": 1}
                continue
            field_schema = self.schema.get(
                f"allOf[0].properties.{product_field}"
            )
            # Save non-array/object fields
            if field_schema["type"] not in {"object", "array"}:
                self.headers_meta[product_field] = {"count": 1}
                continue
            elif field_schema["type"] == "object":
                self.process_object(product_field, product_value, field_schema)
            else:
                self.process_array(product_field, product_value, field_schema)

    def flatten_headers(self):
        headers = []
        for field, meta in self.headers_meta.items():
            if meta["count"] == 0:
                continue
            elif not meta.get("properties"):
                if meta["count"] == 1:
                    headers.append(field)
                else:
                    for i in range(meta["count"]):
                        headers.append(f"{field}[{i}]")
            else:
                # TODO Decide how to process offers.itemCondition as offers[0].itemCondition
                # Different processing logic for nested arrays or nested objects
                if meta["count"] == 1 and field != "offers":
                    for pr in meta["properties"]:
                        headers.append(f"{field}.{pr}")
                    continue
                for i in range(meta["count"]):
                    for pr in meta["properties"]:
                        headers.append(f"{field}[{i}].{pr}")
        self.flat_headers = headers

    def export_product(self, product):
        row = []
        product_data = Cut(product)
        for header in self.flat_headers:
            header_path = header.split(".")
            if header_path[0] not in self.named_properties:
                row.append(product_data.get(header, ""))
            else:
                for pr in product_data[header_path[0]]:
                    if pr.get(self.named_properties[header_path[0]]) == header_path[1]:
                        # TODO Require not only name of the name property, but also value property
                        row.append(str(pr))
        pprint(row)


with open("autocrawl/utils/csv_export_assets/products_full_test.json") as f:
    product_list = json.loads(f.read())
with open("autocrawl/utils/csv_export_assets/product_full_schema.json") as f:
    product_schema = Cut(json.loads(f.read()))
test_named_properties = {
    "gtin": "type",
    "additionalProperty": "name",
    "ratingHistogram": "ratingOption"
}
# TODO Pick all other properties from the objects as an additional columns
# isbn.value, ratingHistogram.5 stars.ratingCount, ratingHistogram.5 stars.ratingPercentage etc.
test_skip_fields = {"probability", "_key"}

csv_exporter = CSVExporter(test_named_properties, test_skip_fields, product_schema)
for p in product_list:
    csv_exporter.process_product(p)

from pprint import pprint
print('*' * 500)
pprint(csv_exporter.headers_meta)
print('*' * 500)
csv_exporter.flatten_headers()
pprint(csv_exporter.flat_headers)
print('*' * 500)
for p in product_list:
    csv_exporter.export_product(p)
    break
