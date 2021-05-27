import csv
import json
import re
from typing import Dict, List, TypedDict

# Using scalpl (instead of jmespath/etc.) as an existing fast backend dependency
from pkg_resources import resource_string
from scalpl import Cut  # NOQA


class Header(TypedDict, total=False):
    count: int
    properties: List[str]


class CSVExporter:
    def __init__(
        self,
        named_properties: Dict[str, str],
        array_limits: Dict[str, int],
        grouped_properties,
        headers_remapping,
    ):
        # Insertion-ordered dict
        self.headers_meta: Dict[str, Header] = {}
        self.flat_headers: List[str] = []
        self.named_properties = named_properties
        self.array_limits = array_limits
        self.grouped_properties = grouped_properties
        self.headers_remapping = headers_remapping

    # TODO What if no prefix provided?
    #  If the initial doc is not array of objects, but array of arrays?
    def process_array(self, prefix: str, array_value: List):
        if len(array_value) == 0:
            return

        # TODO Move to export
        # Limit number of elements processed based on pre-defined limits
        if prefix in self.array_limits:
            array_value = array_value[: self.array_limits[prefix]]

        if self.headers_meta.get(prefix) is None:
            self.headers_meta[prefix] = {"count": 0, "properties": []}

        # Assuming all elements of array are the same type
        if type(array_value[0]) not in {dict, list}:
            if prefix not in self.grouped_properties:
                if self.headers_meta[prefix]["count"] < len(array_value):
                    self.headers_meta[prefix]["count"] = len(array_value)
            else:
                self.headers_meta[prefix] = {}

        elif type(array_value[0]) == list:
            for i, element in enumerate(array_value):
                property_path = f"{prefix}[{i}]"
                self.process_array(property_path, element)

        else:
            if prefix in self.named_properties:
                for element in array_value:
                    property_path = f"{prefix}.{element[self.named_properties[prefix]]}"
                    value_properties = [
                        x for x in element.keys() if x != self.named_properties[prefix]
                    ]
                    if property_path in self.headers_meta:
                        continue
                    self.headers_meta[property_path] = {"properties": value_properties}
            elif prefix in self.grouped_properties:
                properties = []
                for element in array_value:
                    for property_name in element:
                        if property_name not in properties:
                            properties.append(property_name)
                for property_name in properties:
                    property_path = f"{prefix}.{property_name}"
                    self.headers_meta[property_path] = {}
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
                            self.headers_meta[prefix]["properties"].append(
                                property_name
                            )
                        elif type(property_value) == list:
                            self.process_array(property_path, property_value)
                        else:
                            self.process_object(property_value, property_path)

    def process_object(self, object_value: Dict, prefix: str = ""):
        for property_name, property_value in object_value.items():
            property_path = f"{prefix}.{property_name}" if prefix else property_name
            if type(property_value) not in {dict, list}:
                if self.headers_meta.get(property_path) is None:
                    self.headers_meta[property_path] = {}
            elif type(property_value) == list:
                self.process_array(property_path, object_value[property_name])
            else:
                self.process_object(object_value[property_name], property_path)

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

    @property
    def remapped_headers(self, capitalize=True):
        if not self.headers_remapping:
            return self.flat_headers
        remapped_headers = []
        for header in self.flat_headers:
            for old, new in self.headers_remapping:
                header = re.sub(old, new, header)
            if capitalize and header:
                header = header[:1].capitalize() + header[1:]
            remapped_headers.append(header)
        return remapped_headers

    def export_item(self, item: Dict):
        row = []
        item_data = Cut(item)
        for header in self.flat_headers:
            header_path = header.split(".")
            if (
                header_path[0] not in self.named_properties
                and header_path[0] not in self.grouped_properties
            ):
                row.append(item_data.get(header, ""))
            elif header_path[0] in self.grouped_properties:
                if len(header_path) == 1:
                    value = item_data.get(header_path[0])
                    if not value:
                        continue
                    # Objects can't get here because path consists of only one element
                    elif type(value) != list:
                        row.append(value)
                    else:
                        row.append(
                            self.grouped_properties[header_path[0]]["separators"][
                                header
                            ].join(value)
                        )
                # TODO What if more than 2 levels?
                else:
                    value = []
                    for element in item_data.get(header_path[0], []):
                        if element.get(header_path[1]) is not None:
                            value.append(element[header_path[1]])
                    row.append(
                        self.grouped_properties[header_path[0]]["separators"][
                            header
                        ].join(value)
                    )
            # Assuming one nesting level of named properties
            # like `additionalProperty.Focus Type.value`, where
            # `name` (Focus Type) and `value` are on the same level
            else:
                name_property = self.named_properties[header_path[0]]
                value_found = False
                for pr in item_data.get(header_path[0], []):
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
        "ratingHistogram": "ratingOption",
        "named_array_field": "name",
    }
    test_grouped_properties = {
        "images": {"separators": {"images": ", \n"}},
        "breadcrumbs": {
            "separators": {"breadcrumbs.name": " >\n", "breadcrumbs.link": ",\n"}
        },
    }
    test_headers_remapping = [
        (r"offers\[0\].", ""),
        (r"aggregateRating\.", ""),
        (r"additionalProperty\.(.*)\.value", r"\1"),
        (r"breadcrumbs\.name", "breadcrumbs"),
        (r"breadcrumbs\.link", "breadcrumbs links"),
    ]
    # Define how many elements of array to process
    test_array_limits = {"offers": 1}

    # Load item list from JSON (simulate API response)
    file_name = "products_xod_test.json"
    item_list = json.loads(
        resource_string(__name__, f"tests/assets/{file_name}").decode("utf-8")
    )

    csv_exporter = CSVExporter(
        test_named_properties,
        test_array_limits,
        test_grouped_properties,
        test_headers_remapping,
    )

    # Collect stats
    for it in item_list:
        csv_exporter.process_object(it)

    # Flatten headers
    from pprint import pprint

    pprint(csv_exporter.headers_meta, sort_dicts=False)
    print("*" * 500)
    csv_exporter.flatten_headers()
    pprint(csv_exporter.flat_headers, sort_dicts=False)

    with open(
        f"autocrawl/utils/csv_assets/{file_name.replace('.json', '.csv')}", mode="w"
    ) as export_file:
        csv_writer = csv.writer(
            export_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        csv_writer.writerow(csv_exporter.remapped_headers)
        for p in item_list:
            csv_writer.writerow(csv_exporter.export_item(p))
