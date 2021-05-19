import json

from scalpl import Cut

with open("autocrawl/utils/csv_export_assets/products_csv_test.json") as f:
    product_list = json.loads(f.read())

with open("autocrawl/utils/csv_export_assets/product_schema.json") as f:
    product_schema = Cut(json.loads(f.read()))


# Insertion-ordered dict
csv_schema = {}
skip_fields = {"probability", "_key"}

for p in product_list:
    for key, value in p.items():
        if key in csv_schema:
            continue
        if key in skip_fields:
            csv_schema[key] = 1
            continue
        key_schema = Cut(product_schema.get(f"allOf[0].properties.{key}"))
        if key_schema.get("type") not in {"object", "array"}:
            csv_schema[key] = 1
            continue
        if key_schema.get("type") == "array":
            if key_schema.get("items.type") not in {"object", "array"}:
                csv_key_value = csv_schema.get(key, 0)
                if csv_key_value < len(value):
                    csv_schema[key] = len(value)
                continue
            # TODO: Skipping arrays of arrays for now (couldn't find real examples)
            if key_schema.get("items.type") == "object":
                if key == "additionalProperty":
                    available_add_properties = []
                    for sub_item in value:
                        name = sub_item.get("name")
                        if name and name not in available_add_properties:
                            if f"{key}.{name}" not in csv_schema:
                                csv_schema[f"{key}.{name}"] = 1
                else:
                    available_sub_fields = []
                    for sub_item in value:
                        for sub_key in sub_item:
                            if sub_key not in available_sub_fields:
                                available_sub_fields.append(sub_key)
                        if key == "offers":
                            break
                    for i in range(len(value)):
                        for asf in available_sub_fields:
                            csv_schema[f"{key}[{i}].{asf}"] = 1

            continue
        # # Assuming no first-level objects
        # if key_schema.get("items.type") not in {"object", "array"}:
        #
        # print(key)
        # print(key_type)
        # print(value)
        print("*" * 50)
    break
print("*" * 500)
print(csv_schema)
