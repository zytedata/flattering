import json
from collections import defaultdict

from scalpl import Cut


class CSVExporter:
    def __init__(self):
        # Insertion-ordered dict
        self.csv_schema = {}
        self.skip_fields = {"probability", "_key"}
        with open("autocrawl/utils/csv_export_assets/products_csv_test.json") as f:
            self.product_list = json.loads(f.read())
        with open("autocrawl/utils/csv_export_assets/product_schema.json") as f:
            self.product_schema = Cut(json.loads(f.read()))
        self.named_properties = {"gtin", "additionalProperty"}

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
        # Currently there're no array of arrays, so they could be ignored
        if self.csv_schema.get(prefix) is None:
            self.csv_schema[prefix] = {"count": 0, "properties": []}
        if array_schema["items"]["type"] not in {"object"}:
            if self.csv_schema[prefix]["count"] < len(array_value):
                self.csv_schema[prefix]["count"] = len(array_value)
        else:
            # TODO Check if object properties are not nested objects
            # Process only the first offer
            if prefix == "offers":
                array_value = array_value[:1]
            if prefix in self.named_properties:
                # Picking first required property to use as a name
                # because named properties must require the name
                name = array_schema["items"]["required"][0]
                for nm in self.pick_array_names(array_value, name):
                    if nm not in self.csv_schema[prefix]["properties"]:
                        self.csv_schema[prefix]["properties"].append(nm)
            else:
                if self.csv_schema[prefix]["count"] < len(array_value):
                    self.csv_schema[prefix]["count"] = len(array_value)
                # Checking manually to keep properties order instead of checking subsets
                for pr in self.pick_array_properties(array_value):
                    if pr not in self.csv_schema[prefix]["properties"]:
                        self.csv_schema[prefix]["properties"].append(pr)

    @staticmethod
    def pick_array_properties(array_value):
        array_properties = {}
        for element in array_value:
            for key in [x for x in element.keys() if x not in array_properties]:
                array_properties[key] = None
        return list(array_properties.keys())

    @staticmethod
    def pick_array_names(array_value, name):
        array_names = {}
        for element in array_value:
            if element[name] not in array_names:
                array_names[element[name]] = None
        return list(array_names.keys())

    def process_product_list(self):
        for product in self.product_list:
            for product_field, product_value in product.items():
                if product_field in self.csv_schema:
                    continue
                elif product_field in self.skip_fields:
                    self.csv_schema[product_field] = {"count": 1}
                    continue
                field_schema = self.product_schema.get(f"allOf[0].properties.{product_field}")
                # Save non-array/object fields
                if field_schema["type"] not in {"object", "array"}:
                    self.csv_schema[product_field] = {"count": 1}
                    continue
                elif field_schema["type"] == "object":
                    self.process_object(product_field, product_value, field_schema)
                else:
                    self.process_array(product_field, product_value, field_schema)
                # # Process
                # if key_schema.get("type") == "array":
                #     if key_schema.get("items.type") not in {"object", "array"}:
                #         csv_key_value = csv_schema.get(key, 0)
                #         if csv_key_value < len(value):
                #             csv_schema[key] = len(value)
                #         continue
                #     # TODO: Skipping arrays of arrays for now (couldn't find real examples)
                #     if key_schema.get("items.type") == "object":
                #         if key == "additionalProperty":
                #             available_add_properties = []
                #             for sub_item in value:
                #                 name = sub_item.get("name")
                #                 if name and name not in available_add_properties:
                #                     if f"{key}.{name}" not in csv_schema:
                #                         csv_schema[f"{key}.{name}"] = 1
                #         else:
                #             available_sub_fields = []
                #             for sub_item in value:
                #                 for sub_key in sub_item:
                #                     if sub_key not in available_sub_fields:
                #                         available_sub_fields.append(sub_key)
                #                 if key == "offers":
                #                     break
                #             for i in range(len(value)):
                #                 for asf in available_sub_fields:
                #                     csv_schema[f"{key}[{i}].{asf}"] = 1
                #
                #     continue
        print(self.csv_schema)


waka = CSVExporter()
waka.process_product_list()
