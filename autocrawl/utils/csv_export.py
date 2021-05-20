import json
from collections import defaultdict

from scalpl import Cut


class CSVExporter:
    def __init__(self):
        # Insertion-ordered dict
        self.csv_schema = {}
        self.skip_fields = {"probability", "_key"}
        with open("autocrawl/utils/csv_export_assets/products_full_test.json") as f:
            self.product_list = json.loads(f.read())
        with open("autocrawl/utils/csv_export_assets/product_full_schema.json") as f:
            self.product_schema = Cut(json.loads(f.read()))
        self.named_properties = {
            "gtin": "type",
            "additionalProperty": "name",
            "ratingHistogram": "ratingOption"
        }

    def process_object(self, prefix, object_value, object_schema):
        for property_name in object_value:
            property_path = f"{prefix}.{property_name}"
            property_type = object_schema["properties"][property_name]["type"]
            if property_type not in {"object", "array"}:
                if self.csv_schema.get(property_path) is None:
                    self.csv_schema[property_path] = {"count": 1}
            elif property_type == "object":
                self.process_object(property_path, object_value[property_name])

    def process_array(self, prefix, array_value, array_schema):
        if self.csv_schema.get(prefix) is None:
            self.csv_schema[prefix] = {"count": 0, "properties": []}
        # Currently there're no array of arrays, so arrays could be ignored
        if array_schema["items"]["type"] not in {"object"}:
            if self.csv_schema[prefix]["count"] < len(array_value):
                self.csv_schema[prefix]["count"] = len(array_value)
        else:
            # TODO Check if object properties are not nested objects
            # Process only the first offer
            if prefix == "offers":
                array_value = array_value[:1]
            if prefix in self.named_properties:
                for element in array_value:
                    property_path = f"{prefix}.{element[self.named_properties[prefix]]}"
                    if property_path in self.csv_schema:
                        continue
                    property_schema = array_schema["items"]["properties"][self.named_properties[prefix]]
                    property_type = property_schema["type"]
                    if property_type not in {"object", "array"}:
                        self.csv_schema[property_path] = {"count": 1}
                    elif property_type == "array":
                        self.process_array(property_path, element, property_schema)
                    else:
                        self.process_object(property_path, element, property_schema)
            else:
                if self.csv_schema[prefix]["count"] < len(array_value):
                    self.csv_schema[prefix]["count"] = len(array_value)
                # Checking manually to keep properties order instead of checking subsets
                for element in array_value:
                    for property_name, property_value in element.items():
                        property_path = f"{prefix}.{property_name}"
                        property_schema = array_schema["items"]["properties"][
                            property_name
                        ]
                        property_type = property_schema["type"]
                        if property_type not in {"object", "array"}:
                            if property_name in self.csv_schema[prefix]["properties"]:
                                continue
                            self.csv_schema[prefix]["properties"].append(property_name)
                        elif property_type == "array":
                            self.process_array(property_path, property_value, property_schema)
                        else:
                            self.process_object(property_path, property_value, property_schema)

    def process_product_list(self):
        for product in self.product_list:
            for product_field, product_value in product.items():
                if product_field in self.csv_schema:
                    continue
                elif product_field in self.skip_fields:
                    self.csv_schema[product_field] = {"count": 1}
                    continue
                field_schema = self.product_schema.get(
                    f"allOf[0].properties.{product_field}"
                )
                # Save non-array/object fields
                if field_schema["type"] not in {"object", "array"}:
                    self.csv_schema[product_field] = {"count": 1}
                    continue
                elif field_schema["type"] == "object":
                    self.process_object(product_field, product_value, field_schema)
                else:
                    self.process_array(product_field, product_value, field_schema)
        print(json.dumps(self.csv_schema, indent=4))

    def flatten_csv_schema(self):
        headers = []
        for field, meta in self.csv_schema.items():
            if meta["count"] == 0:
                continue
            elif not meta.get("properties"):
                if meta["count"] == 1:
                    headers.append(field)
                else:
                    for i in range(meta["count"]):
                        headers.append(f"{field}[{i}]")
            else:
                for i in range(meta["count"]):
                    for pr in meta["properties"]:
                        headers.append(f"{field}[{i}].{pr}")
        return headers


waka = CSVExporter()
waka.process_product_list()
print('*' * 500)
from pprint import pprint

pprint(waka.flatten_csv_schema())
