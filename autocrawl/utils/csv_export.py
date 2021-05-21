import json
import csv
import os
from pprint import pprint
from scalpl import Cut


class CSVExporter:
    def __init__(self, named_properties, skip_fields):
        # Insertion-ordered dict
        self.headers_meta = {}
        self.flat_headers = []
        self.skip_fields = skip_fields
        self.named_properties = named_properties

    def process_object(self, prefix, object_value):
        for property_name, property_value in object_value.items():
            property_path = f"{prefix}.{property_name}"
            if type(property_value) not in {dict, list}:
                if self.headers_meta.get(property_path) is None:
                    self.headers_meta[property_path] = {}
            elif type(property_value) == dict:
                self.process_object(property_path, object_value[property_name])
            # TODO: Process arrays

    def process_array(self, prefix, array_value):
        if len(array_value) == 0:
            return
        if self.headers_meta.get(prefix) is None:
            self.headers_meta[prefix] = {"count": 0, "properties": []}
        # Currently there're no array of arrays, so arrays could be ignored
        # Also assuming all elements of array are the same type
        if type(array_value[0]) != dict:
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
                    self.headers_meta[property_path] = {"properties": value_properties}
                    # if type(element) not in {dict, list}:
                    #     self.headers_meta[property_path] = {"properties": value_properties}
                    # elif type(element) == list:
                    #     self.process_array(property_path, element)
                    # else:
                    #     self.process_object(property_path, element)
            else:
                if self.headers_meta[prefix]["count"] < len(array_value):
                    self.headers_meta[prefix]["count"] = len(array_value)
                # Checking manually to keep properties order instead of checking subsets
                for i, element in enumerate(array_value):
                    for property_name, property_value in element.items():
                        property_path = f"{prefix}[{i}].{property_name}"
                        if type(property_value) not in {dict, list}:
                            if property_name in self.headers_meta[prefix]["properties"]:
                                continue
                            self.headers_meta[prefix]["properties"].append(property_name)
                        elif type(property_value) == list:
                            self.process_array(property_path, property_value)
                        else:
                            self.process_object(property_path, property_value)

    def process_product(self, product):
        for product_field, product_value in product.items():
            if product_field in self.headers_meta:
                continue
            elif product_field in self.skip_fields:
                self.headers_meta[product_field] = {}
                continue
            # Save non-array/object fields
            if type(product_value) not in {dict, list}:
                self.headers_meta[product_field] = {}
                continue
            elif type(product_value) == list:
                self.process_array(product_field, product_value)
            else:
                self.process_object(product_field, product_value)

    def flatten_headers(self):
        headers = []
        for field, meta in self.headers_meta.items():
            if meta.get("count") == 0:
                continue
            elif meta.get("count") is not None:
                if not meta.get("properties"):
                    for i in range(meta["count"]):
                        headers.append(f"{field}[{i}]")
                    continue
                else:
                    for i in range(meta["count"]):
                        for pr in meta["properties"]:
                            headers.append(f"{field}[{i}].{pr}")
                    continue
            else:
                if not meta.get("properties"):
                    headers.append(field)
                else:
                    for pr in meta.get("properties"):
                        headers.append(f"{field}.{pr}")
        self.flat_headers = headers

    def export_product(self, product):
        row = []
        # Using scalpl (instead of jmespath/etc.) as an existing fast backend dependency
        product_data = Cut(product)
        for header in self.flat_headers:
            header_path = header.split(".")
            if header_path[0] not in self.named_properties:
                row.append(product_data.get(header, ""))
            # Assuming one nesting level of named properties
            # like `additionalProperty.Focus Type.value`, where
            # `name` (Focus Type) and `value` are on the same level
            else:
                name_property = self.named_properties[header_path[0]]
                value_found = False
                for pr in product_data[header_path[0]]:
                    if pr.get(name_property) == header_path[1]:
                        row.append(pr.get(header_path[2], ""))
                        value_found = True
                        break
                if not value_found:
                    row.append("")
        return row


if __name__ == "__main__":
    # Define fields where to use property values as column names
    test_named_properties = {
        "gtin": "type",
        "additionalProperty": "name",
        "ratingHistogram": "ratingOption"
    }
    # Define fields to provide data as-is, without additional processing
    test_skip_fields = {"probability", "_key"}

    # Load product list from JSON (simulate API response)
    with open("autocrawl/utils/csv_export_assets/products_xod_test.json") as f:
        product_list = json.loads(f.read())

    csv_exporter = CSVExporter(test_named_properties, test_skip_fields)

    # Collect stats
    for p in product_list:
        csv_exporter.process_product(p)

    # pprint(csv_exporter.headers_meta)

    # Flatten headers
    csv_exporter.flatten_headers()

    # print('*' * 500)
    # pprint(csv_exporter.flat_headers)

    with open('test_csv_export.csv', mode='w') as export_file:
        employee_writer = csv.writer(export_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        employee_writer.writerow(csv_exporter.flat_headers)
        for p in product_list:
            employee_writer.writerow(csv_exporter.export_product(p))
