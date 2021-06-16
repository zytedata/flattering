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


class FieldOption(TypedDict, total=False):
    name: int
    named: bool
    grouped: bool
    grouped_separators: Dict[str, str]


def prepare_field_options(properties: Dict) -> Cut:
    to_filter = set()
    for property_name, property_value in properties.items():
        if not property_value.get("named") and not property_value.get("grouped"):
            logger.warning(
                f"Adjusted properties ({property_name}) without either `named` or `grouped` "
                "parameters will be skipped."
            )
            to_filter.add(property_name)
    for flt in to_filter:
        properties.pop(flt, None)
    return Cut(properties)


@attr.s(auto_attribs=True)
class CSVStatsCollector:
    """"""

    field_options: Dict[str, FieldOption] = attr.ib(
        converter=prepare_field_options, default=attr.Factory(dict)
    )
    _stats: Dict[str, Header] = attr.ib(init=False, default=attr.Factory(dict))

    @property
    def stats(self):
        return self._stats

    @field_options.validator
    def check_field_options(self, _, value):
        allowed_separators = (";", ",", "\n")
        for property_name, property_value in value.items():
            for tp in {"named", "grouped"}:
                if not isinstance(property_value.get(tp), bool):
                    raise ValueError(
                        f"Adjusted properties ({property_name}) must include `{tp}` parameter with boolean value."
                    )
            if property_value.get("named") and not property_value.get("name"):
                raise ValueError(
                    f"Named adjusted properties ({property_name}) must include `name` parameter."
                )
            if not property_value.get("named") and not property_value.get("grouped"):
                raise ValueError
            for key, value in property_value.get("grouped_separators", {}).items():
                if value not in allowed_separators:
                    raise ValueError(
                        f"Only {allowed_separators} could be used"
                        f" as custom grouped separators ({key}:{value})."
                    )

    def process_items(self, items):
        if not isinstance(items, list):
            raise ValueError(f"Initial items data must be array, not {type(items)}.")
        if len(items) == 0:
            logger.warning("No items provided.")
            return
        item_types = set([type(x) for x in items])
        if len(item_types) > 1:
            raise TypeError(
                f"All elements of the array must be "
                f"of the same type instead of {item_types}."
            )
        if isinstance(items[0], dict):
            for item in items:
                self.process_object(item)
        elif isinstance(items[0], list):
            raise TypeError("Arrays of arrays currently are not supported.")
        else:
            raise ValueError(f"Unsupported item type ({type(items[0])}).")

    def process_array(self, array_value: List, prefix: str = ""):
        if len(array_value) == 0:
            return
        array_types = set([type(x) for x in array_value])
        for et in (dict, list, tuple, set):
            if len(set([x == et for x in array_types])) > 1:
                raise ValueError(
                    f"{str(et)}'s can't be mixed with other types in an array ({prefix})."
                )
        if self._stats.get(prefix) is None:
            self._stats[prefix] = {"count": 0, "properties": []}
        if not isinstance(array_value[0], (dict, list)):
            if prefix not in self.field_options:
                if self._stats[prefix]["count"] < len(array_value):
                    self._stats[prefix]["count"] = len(array_value)
            else:
                self._stats[prefix] = {}
        elif isinstance(array_value[0], list):
            for i, element in enumerate(array_value):
                property_path = f"{prefix}[{i}]"
                self.process_array(element, property_path)
        else:
            if prefix not in self.field_options:
                self._process_base_array(array_value, prefix)
            else:
                self._process_array_with_options(array_value, prefix)

    def _process_base_array(self, array_value: List, prefix: str):
        if self._stats[prefix]["count"] < len(array_value):
            self._stats[prefix]["count"] = len(array_value)
        # Checking manually to keep properties order instead of checking subsets
        for i, element in enumerate(array_value):
            for property_name, property_value in element.items():
                property_path = f"{prefix}[{i}].{property_name}"
                if not isinstance(property_value, (dict, list)):
                    if property_name in self._stats[prefix]["properties"]:
                        continue
                    self._stats[prefix]["properties"].append(property_name)
                elif isinstance(property_value, list):
                    self.process_array(property_value, property_path)
                else:
                    self.process_object(property_value, property_path)

    def _process_array_with_options(self, array_value: List, prefix: str):
        if self.field_options.get(f"{prefix}.grouped"):
            # Arrays that both grouped and named don't need stats to group data
            if self.field_options[prefix]["named"]:
                self._stats[prefix] = {}
                return
            properties = []
            for element in array_value:
                for property_name in element:
                    if property_name not in properties:
                        properties.append(property_name)
            for property_name in properties:
                property_path = f"{prefix}.{property_name}"
                self._stats[property_path] = {}
        elif self.field_options.get(f"{prefix}.named"):
            for element in array_value:
                name = self.field_options.get(f"{prefix}.name")
                property_path = f"{prefix}.{element[name]}"
                value_properties = [x for x in element.keys() if x != name]
                if property_path in self._stats:
                    continue
                self._stats[property_path] = {"properties": value_properties}

    def process_object(self, object_value: Dict, prefix: str = ""):
        for property_name, property_value in object_value.items():
            property_path = f"{prefix}.{property_name}" if prefix else property_name
            if not isinstance(property_value, (dict, list)):
                if self._stats.get(property_path) is None:
                    self._stats[property_path] = {}
            elif isinstance(property_value, list):
                self.process_array(object_value[property_name], property_path)
            else:
                if property_path in self.field_options and self.field_options.get(
                    f"{property_path}.grouped"
                ):
                    self._stats[property_path] = {}
                    return
                self.process_object(object_value[property_name], property_path)


@attr.s(auto_attribs=True)
class CSVExporter:
    """"""

    default_stats: Dict[str, Header] = attr.ib()
    stats_collector: CSVStatsCollector = attr.ib()
    array_limits: Dict[str, int] = attr.ib(default=attr.Factory(dict))
    headers_renaming: List[Tuple[str, str]] = attr.ib(default=attr.Factory(list))
    grouped_separator: str = attr.ib(default="\n")
    _headers: List[str] = attr.ib(init=False, default=attr.Factory(list))

    @headers_renaming.validator
    def check_headers_renaming(self, _, value):
        if not isinstance(value, list):
            raise ValueError("Headers renamings must be provided as a list of tuples.")
        for rmp in value:
            if not isinstance(rmp, (list, tuple)):
                raise ValueError(f"Headers renamings ({rmp}) must be tuples.")
            if len(rmp) != 2:
                raise ValueError(
                    f"Headers renamings ({rmp}) must include two elements: pattern and replacement."
                )
            if any([not isinstance(x, str) for x in rmp]):
                raise ValueError(f"Headers renamings ({rmp}) elements must be strings.")

    @staticmethod
    def _generate_max_item(columns: List[str]) -> Cut:
        """
        Generate the largest possible item (max fields included)
        based on the headers from CSVStatsCollector.
        :param columns: List of column names
        :return: Item with empty values
        """
        item = Cut({})
        for column in columns:
            full_path = []
            column_path = column.split(".")
            # Skipping array of arrays cases
            for i, field in enumerate(column_path):
                # Check if array or dict
                array_data = re.findall(r"^(.+)\[(\d+)\]$", field)
                if not array_data:
                    full_path.append(field)
                    field_path = ".".join(full_path)
                    # If object doesn't exist
                    if not item.get(field_path):
                        # More elements in path means dict
                        if i + 1 < len(column_path):
                            item[field_path] = {}
                        # Last element means simple type
                        else:
                            item[field_path] = ""
                else:
                    field_path = ".".join(full_path + [array_data[0][0]])
                    element_path = ".".join(full_path + [field])
                    # If array doesn't exist
                    if not item.get(field_path):
                        item[field_path] = []
                    # If array element doesn't exist
                    if not item.get(element_path):
                        # More elements in path means array of dicts
                        if i + 1 < len(column_path):
                            item[field_path].append({})
                        # Last element means array of simple types
                        else:
                            item[field_path].append("")
                    full_path.append(field)
        return item

    def _convert_stats_to_headers(self):
        headers = []
        for field, meta in self.default_stats.items():
            if meta.get("count") == 0:
                continue
            elif meta.get("count") is not None:
                if not meta.get("properties"):
                    for i in range(meta["count"]):
                        headers.append(f"{field}[{i}]")
                else:
                    for i in range(meta["count"]):
                        for pr in meta["properties"]:
                            headers.append(f"{field}[{i}].{pr}")
            else:
                if not meta.get("properties"):
                    headers.append(field)
                else:
                    for pr in meta.get("properties"):
                        headers.append(f"{field}.{pr}")
        self._headers = headers

    def _rename_headers(self, capitalize=True):
        if not self.headers_renaming:
            return self._headers
        renamed_headers = []
        for header in self._headers:
            for old, new in self.headers_renaming:
                header = re.sub(old, new, header)
            if capitalize and header:
                header = header[:1].capitalize() + header[1:]
            renamed_headers.append(header)
        return renamed_headers

    def _limit_field_elements(self):
        """
        Limit number of elements exported based on pre-defined limits
        """
        filters = set()
        # Find fields that need to be limited
        for key, value in self.array_limits.items():
            if key not in self.default_stats:
                continue
            if not self.default_stats[key].get("count"):
                continue
            if self.default_stats[key]["count"] <= value:
                continue
            for i in range(self.default_stats[key]["count"]):
                if i < value:
                    continue
                filters.add(f"{key}[{i}]")
            self.default_stats[key]["count"] = value
        limited_default_stats = {}
        # Limit field elements
        for field, stats in self.default_stats.items():
            for key in filters:
                if field.startswith(key):
                    break
            else:
                limited_default_stats[field] = stats
        self.default_stats = limited_default_stats

    @staticmethod
    def _escape_grouped_data(value, separator):
        if not value:
            return value
        escaped_separator = f"\\{separator}" if separator != "\n" else "\\n"
        if isinstance(value, list):
            return [str(x).replace(separator, escaped_separator) for x in value]
        else:
            return str(value).replace(separator, escaped_separator)

    def _export_item_as_row(self, item: Dict) -> List:
        # TODO Allow to use it separately
        # But before that we need to check that headers were flattened
        # max item collected, and export headers collected
        row = []
        item_data = Cut(item)
        for header in self._headers:
            header_path = header.split(".")
            if header_path[0] not in self.stats_collector.field_options:
                row.append(item_data.get(header, ""))
            else:
                row.append(
                    self._export_property_with_options(header, header_path, item_data)
                )
        return row

    def _export_property_with_options(
        self, header: str, header_path: List[str], item_data: Cut
    ):
        if self.stats_collector.field_options.get(f"{header_path[0]}.grouped"):
            separator = (
                self.stats_collector.field_options.get(header_path[0], {})
                .get("grouped_separators", {})
                .get(header)
                or self.grouped_separator
            )
            # Grouped
            if not self.stats_collector.field_options.get(f"{header_path[0]}.named"):
                if len(header_path) == 1:
                    value = item_data.get(header_path[0])
                    if value is None:
                        return ""
                    elif not isinstance(value, (list, dict)):
                        return value
                    elif isinstance(value, list):
                        return separator.join(
                            self._escape_grouped_data(value, separator)
                        )
                    else:
                        return separator.join(
                            [
                                f"{self._escape_grouped_data(pn, separator)}: {self._escape_grouped_data(pv, separator)}"
                                for pn, pv in value.items()
                            ]
                        )
                # TODO What if more than 2 levels?
                else:
                    value = []
                    for element in item_data.get(header_path[0], []):
                        if element.get(header_path[1]) is not None:
                            value.append(element[header_path[1]])
                    return separator.join(self._escape_grouped_data(value, separator))
            # Grouped AND Named
            else:
                name = self.stats_collector.field_options.get(f"{header_path[0]}.name")
                values = []
                for element in item_data.get(header_path[0], []):
                    element_name = element.get(name, "")
                    element_values = []
                    for property_name, property_value in element.items():
                        if property_name == name:
                            continue
                        element_values.append(property_value)
                    values.append(
                        f"{element_name}: {','.join([str(x) for x in element_values])}"
                    )
                return separator.join(self._escape_grouped_data(values, separator))
        # Named; if not grouped and not named - adjusted property was filtered
        else:
            name = self.stats_collector.field_options.get(f"{header_path[0]}.name")
            for element in item_data.get(header_path[0], []):
                if element.get(name) == header_path[1]:
                    return element.get(header_path[2], "")
            else:
                return ""

    def export_csv(self, items: list, export_path: str):
        # Convert stats to headers
        self._convert_stats_to_headers()
        self.limit_headers_meta()
        self.flatten_headers()
        with open(export_path, mode="w") as export_file:
            csv_writer = csv.writer(
                export_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csv_writer.writerow(self._rename_headers())
            for p in items:
                csv_writer.writerow(self._export_item_as_row(p))


if __name__ == "__main__":
    # test_field_options = {
    #     "gtin": {
    #         "named": True,
    #         "grouped": False,
    #         "name": "type",
    #         "grouped_separators": {},
    #     },
    #     "additionalProperty": {
    #         "named": True,
    #         "grouped": False,
    #         "name": "name",
    #         "grouped_separators": {"additionalProperty": "\n"},
    #     },
    #     "aggregateRating": {
    #         "named": False,
    #         "grouped": False,
    #         "name": "",
    #         "grouped_separators": {"aggregateRating": "\n"},
    #     },
    #     "images": {
    #         "named": False,
    #         "grouped": True,
    #         "name": "",
    #         "grouped_separators": {"images": "\n"},
    #     },
    #     "breadcrumbs": {
    #         "named": False,
    #         "grouped": True,
    #         "name": "name",
    #         "grouped_separators": {
    #             "breadcrumbs.name": "\n",
    #             "breadcrumbs.link": "\n",
    #         },
    #     },
    #     # "ratingHistogram": {
    #     #     "named": True,
    #     #     "grouped": False,
    #     #     "name": "ratingOption",
    #     #     "grouped_separators": {"ratingHistogram": "\n"},
    #     # },
    #     # "named_array_field": {
    #     #     "named": True,
    #     #     "grouped": False,
    #     #     "name": "name",
    #     #     "grouped_separators": {},
    #     # }
    # }
    # test_headers_renaming = [
    #     (r"offers\[0\].", ""),
    #     (r"aggregateRating\.", ""),
    #     (r"additionalProperty\.(.*)\.value", r"\1"),
    #     (r"breadcrumbs\.name", "breadcrumbs"),
    #     (r"breadcrumbs\.link", "breadcrumbs links"),
    # ]
    # # Define how many elements of array to process
    # test_array_limits = {"offers": 1}
    # Load item list from JSON (simulate API response)
    # TODO Test additionalProperties processing
    file_name = "products_simple_xod_test.json"
    item_list = json.loads(
        resource_string(__name__, f"tests/assets/{file_name}").decode("utf-8")
    )
    csv_sc = CSVStatsCollector()
    csv_sc.process_items(item_list)
    # csv_exporter.limitdefault_stats()
    # csv_exporter.flatten_headers()
    print(csv_sc.stats)
    #
    # test_data = csv_exporter._headers
    # _generate_max_item(test_data)
    #
    # ###########################
    # # csv_exporter.export_csv(
    # #     item_list, f"autocrawl/utils/csv_assets/{file_name.replace('.json', '.csv')}"
    # # )
