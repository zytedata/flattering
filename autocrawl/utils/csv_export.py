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

    # TODO Fix
    def process_object(self, prefix, object_value, object_schema):
        return ""
        # for property_name, property_value in object_field["properties"]:
        #     property_path = f"{prefix}.{property_name}"
        #     if property_value["type"] not in {"object", "array"}:
        #         if self.csv_schema.get(property_path) is None:
        #             self.csv_schema[property_path] = ""
        #     elif property_value["type"] == "object":
        #         self.process_object(property_path, property_value)
        #     # TODO Add arrays processing

    def process_array(self, prefix, array_value, array_schema):
        # Currently there're no array of arrays, so they could be ignored
        if self.csv_schema.get(prefix) is None:
            self.csv_schema[prefix] = {}
        if array_schema["items"]["type"] not in {"object"}:
            if self.csv_schema.get(f"{prefix}.count", 0) < len(array_value):
                self.csv_schema[prefix]["count"] = len(array_value)
        else:
            # Process only the first offer
            if prefix == "offers":
                array_value = array_value[:1]
            if prefix in self.named_properties:
                pass
            else:
                if self.csv_schema.get(f"{prefix}.count", 0) < len(array_value):
                    self.csv_schema[prefix]["count"] = len(array_value)
                object_properties = self.pick_objects_array_properties(array_value)
                # TODO: Check not length but new/non-existing elements
                if len(self.csv_schema.get(f"{prefix}.properties", [])) < len(object_properties):
                    self.csv_schema[prefix]["properties"] = object_properties

    @staticmethod
    def pick_objects_array_properties(array_value):
        array_properties = {}
        for element in array_value:
            for key in [x for x in element.keys() if x not in array_properties]:
                array_properties[key] = None
        return list(array_properties.keys())

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
                    pass
                    # self.process_object(product_field, product_value)
                else:
                    # TODO Temporary filter
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
