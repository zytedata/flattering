import csv
import json
import logging
import re
from typing import Dict, List, Tuple, TypedDict

import attr
from pkg_resources import resource_string

# Using scalpl (instead of jmespath/etc.) as an existing fast backend dependency
from scalpl import Cut  # NOQA

logger = logging.getLogger(__name__)


class Header(TypedDict, total=False):
    count: int
    properties: List[str]


def prepare_adjusted_properties(properties: Dict) -> Cut:
    to_filter = set()
    for property_name, property_value in properties.items():
        if not property_value.get("named") and not property_value.get("grouped"):
            logger.warning(
                f"Adjusted properties ({property_name}) without either `named` or `grouped` "
                "parameters enabled should be avoided or commented out."
            )
            to_filter.add(property_name)
    for flt in to_filter:
        properties.pop(flt, None)
    return Cut(properties)


@attr.s(auto_attribs=True)
class CSVExporter:
    adjusted_properties: Dict = attr.ib(converter=prepare_adjusted_properties)
    array_limits: Dict[str, int]
    headers_remapping: List[Tuple[str, str]] = attr.ib()
    grouped_separator: str = "\n"
    headers: List[str] = attr.Factory(list)
    headers_meta: Dict[str, Header] = attr.Factory(dict)

    @adjusted_properties.validator
    def check_adjusted_properties(self, attribute, value):
        for property_name, property_value in value.items():
            for tp in {"named", "grouped"}:
                if type(property_value.get(tp)) != bool:
                    raise ValueError(
                        f"Adjusted properties ({property_name}) must include `{tp}` parameter with boolean value."
                    )
            if (
                property_value.get("named") is not None
                and property_value.get("name") is None
            ):
                raise ValueError(
                    f"Named adjusted properties ({property_name}) must include `name` parameter."
                )
            if not property_value.get("named") and not property_value.get("grouped"):
                raise ValueError

    @headers_remapping.validator
    def check_headers_remapping(self, attribute, value):
        if type(value) != list:
            raise ValueError("Headers remappings must be provided as a list of tuples.")
        for rmp in value:
            if type(rmp) not in {list, tuple}:
                raise ValueError(f"Headers remappings ({rmp}) must be tuples.")
            if len(rmp) != 2:
                raise ValueError(
                    f"Headers remappings ({rmp}) must include two elements: pattern and replacement."
                )
            if any([type(x) != str for x in rmp]):
                raise ValueError(
                    f"Headers remappings ({rmp}) elements must be strings."
                )

    # TODO What if no prefix provided?
    #  If the initial doc is not array of objects, but array of arrays?
    def process_array(self, prefix: str, array_value: List):
        if len(array_value) == 0:
            return
        if self.headers_meta.get(prefix) is None:
            self.headers_meta[prefix] = {"count": 0, "properties": []}
        # Assuming all elements of array are the same type
        if type(array_value[0]) not in {dict, list}:
            if prefix not in self.adjusted_properties:
                if self.headers_meta[prefix]["count"] < len(array_value):
                    self.headers_meta[prefix]["count"] = len(array_value)
            else:
                self.headers_meta[prefix] = {}
        elif type(array_value[0]) == list:
            for i, element in enumerate(array_value):
                property_path = f"{prefix}[{i}]"
                self.process_array(property_path, element)
        else:
            if prefix not in self.adjusted_properties:
                self.process_base_array(prefix, array_value)
            else:
                self.process_adjusted_array(prefix, array_value)

    def process_base_array(self, prefix: str, array_value: List):
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
                    self.process_object(property_value, property_path)

    def process_adjusted_array(self, prefix: str, array_value: List):
        if self.adjusted_properties.get(f"{prefix}.grouped"):
            # Arrays that both grouped and named don't need stats to group data
            if self.adjusted_properties[prefix]["named"]:
                self.headers_meta[prefix] = {}
                return
            properties = []
            for element in array_value:
                for property_name in element:
                    if property_name not in properties:
                        properties.append(property_name)
            for property_name in properties:
                property_path = f"{prefix}.{property_name}"
                self.headers_meta[property_path] = {}
        elif self.adjusted_properties.get(f"{prefix}.named"):
            for element in array_value:
                name = self.adjusted_properties.get(f"{prefix}.name")
                property_path = f"{prefix}.{element[name]}"
                value_properties = [x for x in element.keys() if x != name]
                if property_path in self.headers_meta:
                    continue
                self.headers_meta[property_path] = {"properties": value_properties}

    def process_object(self, object_value: Dict, prefix: str = ""):
        for property_name, property_value in object_value.items():
            property_path = f"{prefix}.{property_name}" if prefix else property_name
            if type(property_value) not in {dict, list}:
                if self.headers_meta.get(property_path) is None:
                    self.headers_meta[property_path] = {}
            elif type(property_value) == list:
                self.process_array(property_path, object_value[property_name])
            else:
                if (
                    property_path in self.adjusted_properties
                    and self.adjusted_properties.get(f"{property_path}.grouped")
                ):
                    self.headers_meta[property_path] = {}
                    return
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
        self.headers = headers

    def remap_headers(self, capitalize=True):
        if not self.headers_remapping:
            return self.headers
        remapped_headers = []
        for header in self.headers:
            for old, new in self.headers_remapping:
                header = re.sub(old, new, header)
            if capitalize and header:
                header = header[:1].capitalize() + header[1:]
            remapped_headers.append(header)
        return remapped_headers

    def export_item(self, item: Dict):
        row = []
        item_data = Cut(item)
        for header in self.headers:
            # Limit number of elements processed based on pre-defined limits
            if header in self.array_limits and type(item_data.get(header)) == list:
                item_data[header] = item_data[header][: self.array_limits[header]]
            header_path = header.split(".")
            if header_path[0] not in self.adjusted_properties:
                row.append(item_data.get(header, ""))
                continue
            elif self.adjusted_properties.get(f"{header_path[0]}.grouped"):
                separator = (
                    self.adjusted_properties.get(
                        f"{header_path[0]}.grouped_separators.{header}"
                    )
                    or self.grouped_separator
                )
                if not self.adjusted_properties.get(f"{header_path[0]}.named"):
                    if len(header_path) == 1:
                        value = item_data.get(header_path[0])
                        if not value:
                            continue
                        elif type(value) not in {list, dict}:
                            row.append(value)
                        elif type(value) == list:
                            row.append(separator.join(value))
                        else:
                            row.append(
                                separator.join(
                                    [f"{pn}: {pv}" for pn, pv in value.items()]
                                )
                            )
                        continue
                    # TODO What if more than 2 levels?
                    else:
                        value = []
                        for element in item_data.get(header_path[0], []):
                            if element.get(header_path[1]) is not None:
                                value.append(element[header_path[1]])
                        row.append(separator.join(value))
                        continue
                else:
                    name = self.adjusted_properties.get(f"{header_path[0]}.name")
                    values = []
                    for element in item_data.get(header_path[0], []):
                        element_name = element.get(name, "")
                        element_values = []
                        for property_name, property_value in element.items():
                            if property_name == name:
                                continue
                            element_values.append(property_value)
                        values.append(f"{element_name}: {','.join(element_values)}")
                    row.append(separator.join(values))
            # Assuming one nesting level of named properties
            # like `additionalProperty.Focus Type.value`, where
            # `name` (Focus Type) and `value` are on the same level
            elif self.adjusted_properties.get(f"{header_path[0]}.named"):
                name = self.adjusted_properties.get(f"{header_path[0]}.name")
                value_found = False
                for element in item_data.get(header_path[0], []):
                    if element.get(name) == header_path[1]:
                        row.append(element.get(header_path[2], ""))
                        value_found = True
                        break
                if not value_found:
                    row.append("")
        return row


if __name__ == "__main__":
    test_adjusted_properties = {
        "gtin": {
            "named": True,
            "grouped": False,
            "name": "type",
            "grouped_separators": {},
        },
        "additionalProperty": {
            "named": True,
            "grouped": False,
            "name": "name",
            "grouped_separators": {"additionalProperty": "\n"},
        },
        # "ratingHistogram": {
        #     "named": True,
        #     "grouped": False,
        #     "name": "ratingOption",
        #     "grouped_separators": {"ratingHistogram": "\n"},
        # },
        "aggregateRating": {
            "named": False,
            "grouped": True,
            "name": "",
            "grouped_separators": {"aggregateRating": "\n"},
        },
        # "named_array_field": {
        #     "named": True,
        #     "grouped": False,
        #     "name": "name",
        #     "grouped_separators": {},
        # },
        "images": {
            "named": False,
            "grouped": True,
            "name": "",
            "grouped_separators": {"images": ",\n"},
        },
        "breadcrumbs": {
            "named": False,
            "grouped": True,
            "name": "name",
            # "grouped_separators": {"breadcrumbs.name": " >\n", "breadcrumbs.link": "\n"},
        },
    }
    # TODO Validate input
    # If not grouped and not names - show exception to not to add it or comment then
    # If grouped and named separators must be set for the main field
    # If named - name must be set
    # If grouped - custom separator could be set
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
        test_adjusted_properties,
        test_array_limits,
        test_headers_remapping,
    )

    # Collect stats
    for it in item_list:
        csv_exporter.process_object(it)

    # Flatten headers
    from pprint import pprint

    # pprint(csv_exporter.headers_meta, sort_dicts=False)
    # print("*" * 500)
    csv_exporter.flatten_headers()
    # pprint(csv_exporter.headers, sort_dicts=False)

    with open(
        f"autocrawl/utils/csv_assets/{file_name.replace('.json', '.csv')}", mode="w"
    ) as export_file:
        csv_writer = csv.writer(
            export_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        csv_writer.writerow(csv_exporter.remap_headers())
        for p in item_list:
            csv_writer.writerow(csv_exporter.export_item(p))

    # TODO Add input validation
    # TODO Add escaping for key-value-elements if grouping
    # Maybe using \n as a default separator should work, to don't mess up with escaping
    # Define fields where to use property values as column names
