import csv
import json
import logging
import re
from typing import Dict, List, Tuple, TypedDict, Union

import attr  # NOQA
from pkg_resources import resource_string

# Using scalpl (instead of jmespath/etc.) as an existing fast backend dependency
from scalpl import Cut  # NOQA

logger = logging.getLogger(__name__)


class Property(TypedDict):
    values: Dict[str, None]
    limited: bool


class Header(TypedDict, total=False):
    count: int
    properties: Dict[str, Property]


class FieldOption(TypedDict, total=False):
    name: str
    named: bool
    grouped: bool
    grouped_separators: Dict[str, str]


@attr.s(auto_attribs=True)
class CSVStatsCollector:
    """"""

    named_columns_limit: int = attr.ib(default=20)
    cut_separator: str = attr.ib(default="->")
    _stats: Dict[str, Header] = attr.ib(init=False, default=attr.Factory(dict))

    @property
    def stats(self):
        return self._stats

    def process_items(self, items: List[Dict]):
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
            self._stats[prefix] = {"count": 0, "properties": {}, "type": "array"}
        if not isinstance(array_value[0], (dict, list)):
            self._stats[prefix]["count"] = max(
                self._stats[prefix]["count"], len(array_value)
            )
        elif isinstance(array_value[0], list):
            for i, element in enumerate(array_value):
                property_path = f"{prefix}[{i}]"
                self.process_array(element, property_path)
        else:
            self._process_base_array(array_value, prefix)

    def _process_base_array(self, array_value: List, prefix: str):
        if self._stats[prefix]["count"] < len(array_value):
            self._stats[prefix]["count"] = len(array_value)
        # Checking manually to keep properties order instead of checking subsets
        for i, element in enumerate(array_value):
            for property_name, property_value in element.items():
                property_path = f"{prefix}[{i}]{self.cut_separator}{property_name}"
                if not isinstance(property_value, (dict, list)):
                    if property_name not in self._stats[prefix]["properties"]:
                        # Using dictionaries instead of sets to keep order
                        self._stats[prefix]["properties"][property_name] = {
                            "values": {},
                            "limited": False,
                        }
                    property_data = self._stats[prefix]["properties"][property_name]
                    # If number of different values for property hits the limit of the allowed named columns
                    # No values would be collected for such property
                    if property_data.get("limited"):
                        continue
                    self._stats[prefix]["properties"][property_name]["values"][
                        property_value
                    ] = None
                    if len(property_data.get("values", {})) > self.named_columns_limit:
                        # Clear previously collected values if the limit was hit to avoid partly processed columns
                        self._stats[prefix]["properties"][property_name]["values"] = {}
                        self._stats[prefix]["properties"][property_name][
                            "limited"
                        ] = True
                        continue
                elif isinstance(property_value, list):
                    self.process_array(property_value, property_path)
                else:
                    self.process_object(property_value, property_path)

    @staticmethod
    def _is_hashable(value):
        if isinstance(value, (dict, list)):
            return False
        else:
            return True

    def process_object(self, object_value: Dict, prefix: str = ""):
        if not prefix:
            for property_name, property_value in object_value.items():
                if self._is_hashable(property_value):
                    if self._stats.get(property_name) is None:
                        self._stats[property_name] = {}
                elif isinstance(property_value, list):
                    self.process_array(object_value[property_name], property_name)
                else:
                    self.process_object(object_value[property_name], property_name)
        else:

            # IMPORTANT Values for specific property mustn't change from hashable to non-hashable types
            # during the run
            # TODO: Initialize type when adding first property to "properties" and then compare with it
            # If prefix was filled with hashable data, but some items were unhashable
            # so prefix was rebuilt into regular object headers, not saving values
            if self._stats.get(prefix, {}).get("count") == 0:
                for property_name, property_value in object_value.items():
                    property_path = (
                        f"{prefix}{self.cut_separator}{property_name}"
                        if prefix
                        else property_name
                    )
                    if self._is_hashable(property_value):
                        if self._stats.get(property_path) is None:
                            self._stats[property_path] = {}
                    elif isinstance(property_value, list):
                        self.process_array(object_value[property_name], property_path)
                    else:
                        self.process_object(object_value[property_name], property_path)
                return
            # Check that object has a single level and all values are hashable
            values_hashable = {k: self._is_hashable(v) for k, v in object_value.items()}
            # # If everything is hashable - collect names and values, so the field could be grouped later
            if all(values_hashable.values()):
                if not self._stats.get(prefix):
                    self._stats[prefix] = {"properties": {}, "type": "object"}
                for property_name, property_value in object_value.items():
                    if not self._stats[prefix]["properties"].get(property_name):
                        self._stats[prefix]["properties"][property_name] = {
                            "values": {property_value: None},
                            # "is_hashable": self._is_hashable(property_value),
                            "limited": False,
                        }
                    else:
                        # properties = self._stats[prefix]["properties"]
                        # if self._is_hashable(property_value) != properties[property_name]["is_hashable"]:
                        #     raise ValueError(f"Value of property \"{property_name}\" should be stable "
                        #                      "(hashable or non-hashable) between items.")
                        self._stats[prefix]["properties"][property_name]["values"][
                            property_value
                        ] = None
            else:
                # IMPORTANT What if just new non-hashable field was added? :)
                # If property values are not all hashable, but there're properties saved
                # it means that type
                if self._stats.get(prefix, {}).get("properties"):
                    # raise ValueError(f"Properties types can't change their type "
                    #                  "from hashable to non-hashable: {object_value}")
                    prev_stats = self._stats.pop(prefix)
                    # Mark prefix as rebuilt to avoid checking hashable types, because no values would be collected
                    self._stats[prefix] = {"count": 0}
                    # Rebuild previously collected starts
                    for name, values in prev_stats.get("properties", {}).items():
                        for value, _ in values.get("values", {}).items():
                            self._process_base_object({name: value}, prefix)
                self._process_base_object(object_value, prefix, values_hashable)

    def _process_base_object(
        self, object_value: Dict, prefix: str = "", values_hashable: Dict = None
    ):
        for property_name, property_value in object_value.items():
            property_path = (
                f"{prefix}{self.cut_separator}{property_name}"
                if prefix
                else property_name
            )
            if values_hashable:
                property_stats = self._stats.get(property_path)
                # If hashable, but have existing non-empty properties
                if (
                    values_hashable[property_name]
                    and property_stats != {}
                    and property_stats is not None
                ):
                    raise ValueError(
                        f"Field {property_name} was processed as non-hashable "
                        f"but later got hashable value: ({property_value})"
                    )
                # If not hashable, but doesn't have properties
                if not values_hashable[property_name] and property_stats == {}:
                    raise ValueError(
                        f"Field {property_name} was processed as hashable "
                        f"but later got non-hashable value: ({property_value})"
                    )
            if not isinstance(property_value, (dict, list)):
                if self._stats.get(property_path) is None:
                    self._stats[property_path] = {}
            elif isinstance(property_value, list):
                self.process_array(object_value[property_name], property_path)
            else:
                self.process_object(object_value[property_name], property_path)


@attr.s(auto_attribs=True)
class CSVExporter:
    """"""

    default_stats: Dict[str, Header] = attr.ib()
    field_options: Dict[str, FieldOption] = attr.ib(default=attr.Factory(dict))
    array_limits: Dict[str, int] = attr.ib(default=attr.Factory(dict))
    headers_renaming: List[Tuple[str, str]] = attr.ib(default=attr.Factory(list))
    grouped_separator: str = attr.ib(default="\n")
    cut_separator: str = attr.ib(default="->")
    _headers: List[str] = attr.ib(init=False, default=attr.Factory(list))

    def __attrs_post_init__(self):
        self.field_options: Union[
            Dict[str, FieldOption], Cut
        ] = self._prepare_field_options(self.field_options, self.cut_separator)

    @field_options.validator
    def check_field_options(self, _, value: Dict):
        allowed_separators = (";", ",", "\n")
        for property_name, property_value in value.items():
            for tp in ["named", "grouped"]:
                if not isinstance(property_value.get(tp), bool):
                    raise ValueError(
                        f"Adjusted properties ({property_name}) must include `{tp}` parameter with boolean value."
                    )
            if property_value.get("named") and not property_value.get("name"):
                raise ValueError(
                    f"Named adjusted properties ({property_name}) must include `name` parameter."
                )
            for key, value in property_value.get("grouped_separators", {}).items():
                if value not in allowed_separators:
                    raise ValueError(
                        f"Only {allowed_separators} could be used"
                        f" as custom grouped separators ({key}:{value})."
                    )

    @staticmethod
    def _prepare_field_options(properties: Dict, separator: str) -> Cut:
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
        return Cut(properties, sep=separator)

    @headers_renaming.validator
    def check_headers_renaming(self, _, value: List[Tuple[str, str]]):
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
    def _convert_stats_to_headers(
        stats: Dict[str, Header], separator: str, field_options: Dict[str, FieldOption]
    ) -> List[str]:
        def expand(field, meta, field_option: FieldOption):
            field_option = field_option or {}
            headers = [field]
            count, properties = meta.get("count"), meta.get("properties", [])
            if count == 0:
                return []  # If no count, then no content at all
            named, grouped = field_option.get("named", False), field_option.get(
                "grouped", False
            )
            if named and grouped:
                # Everything will be summarized in a single cell
                return headers
            elif named and not grouped:
                # Each value for the named property will be a new column
                name = field_option["name"]
                named_prop = properties[name]
                if named_prop.get("limited", False):
                    raise NotImplementedError()  # TODO: deal with the limited case
                values = named_prop.get("values", {})
                rest_of_keys = [key for key in properties if key != name]
                headers = [
                    f"{field}{separator}{value}{separator}{key}"
                    for value in values
                    for key in rest_of_keys
                ]
                return headers
            elif not named and grouped:
                if meta.get("type") == "array":
                    # One group per each property if array
                    return [f"{field}{separator}{key}" for key in properties]
                else:
                    # Group everything in a single cell if not
                    return headers
            # Regular case. Handle arrays.
            if count is not None:
                headers = [f"{f}[{i}]" for f in headers for i in range(count)]
            if properties:
                headers = [f"{f}{separator}{pr}" for f in headers for pr in properties]
            return headers

        return [
            f
            for field, meta in stats.items()
            for f in expand(field, meta, field_options.get(field, {}))
        ]

    @staticmethod
    def _get_renamed_headers(
        headers: List[str],
        headers_renaming: List[Tuple[str, str]],
        capitalize: bool = True,
    ) -> List[str]:
        if not headers_renaming:
            return headers
        renamed_headers = []
        for header in headers:
            for old, new in headers_renaming:
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
            count = self.default_stats[key].get("count")
            if not count:
                continue
            for i in range(value, count):
                filters.add(f"{key}[{i}]")
            if count > value:
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
        return str(value).replace(separator, escaped_separator)

    def _export_field_with_options(
        self, header: str, header_path: List[str], item_data: Cut
    ) -> str:
        if self.field_options[header_path[0]]["grouped"]:
            separator = (
                self.field_options.get(header_path[0], {})
                .get("grouped_separators", {})
                .get(header)
                or self.grouped_separator
            )
            # Grouped
            if not self.field_options[header_path[0]]["named"]:
                if len(header_path) == 1:
                    value = item_data.get(header_path[0])
                    if value is None:
                        return ""
                    elif not isinstance(value, (list, dict)):
                        return value
                    elif isinstance(value, list):
                        return separator.join(
                            [self._escape_grouped_data(x, separator) for x in value]
                        )
                    else:
                        return separator.join(
                            [
                                f"{self._escape_grouped_data(pn, separator)}"
                                f": {self._escape_grouped_data(pv, separator)}"
                                for pn, pv in value.items()
                            ]
                        )
                # TODO What if more than 2 levels?
                else:
                    value = []
                    for element in item_data.get(header_path[0], []):
                        if element.get(header_path[1]) is not None:
                            value.append(element[header_path[1]])
                    return separator.join(
                        [self._escape_grouped_data(x, separator) for x in value]
                    )
            # Grouped AND Named
            else:
                name = self.field_options[header_path[0]]["name"]
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
                return separator.join(
                    [self._escape_grouped_data(x, separator) for x in values]
                )
        # Named; if not grouped and not named - adjusted property was filtered
        else:
            name = self.field_options[header_path[0]]["name"]
            for element in item_data.get(header_path[0], []):
                if element.get(name) == header_path[1]:
                    return element.get(header_path[2], "")
            else:
                return ""

    def _prepare_for_export(self):
        # If headers are set - they've been processed already and ready for export
        if self._headers:
            return
        self._limit_field_elements()
        separator = self.cut_separator
        self._headers = self._convert_stats_to_headers(
            self.default_stats, separator, self.field_options
        )
        print("*" * 50)
        print(self._headers)
        # TODO Think about implementing custom headers sorting

    def export_item_as_row(self, item: Dict) -> List:
        self._prepare_for_export()
        row = []
        separator = self.cut_separator
        item_data = Cut(item, sep=separator)
        for header in self._headers:
            header_path = header.split(separator)
            if header_path[0] not in self.field_options:
                row.append(item_data.get(header, ""))
            else:
                row.append(
                    self._export_field_with_options(header, header_path, item_data)
                )
        return row

    def export_csv(self, items: List[Dict], export_path: str):
        self._prepare_for_export()
        with open(export_path, mode="w") as export_file:
            csv_writer = csv.writer(
                export_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csv_writer.writerow(
                self._get_renamed_headers(self._headers, self.headers_renaming)
            )
            for p in items:
                csv_writer.writerow(self.export_item_as_row(p))


if __name__ == "__main__":
    # CUSTOM OPTIONS
    test_field_options = {
        # "gtin": FieldOption(named=True, grouped=False, name="type"),
        # "additionalProperty": FieldOption(
        #     named=True,
        #     grouped=False,
        #     name="name",
        #     grouped_separators={"additionalProperty": "\n"},
        # ),
        # "aggregateRating": FieldOption(
        #     named=False,
        #     grouped=False,
        #     name="",
        #     grouped_separators={"aggregateRating": "\n"},
        # ),
        # "images": FieldOption(
        #     named=False, grouped=True, name="", grouped_separators={"images": "\n"}
        # ),
        # "breadcrumbs": FieldOption(
        #     named=False,
        #     grouped=True,
        #     name="name",
        #     grouped_separators={
        #         "breadcrumbs.name": "\n",
        #         "breadcrumbs.link": "\n",
        #     },
        # ),
        # "ratingHistogram": FieldOption(
        #     named=True,
        #     grouped=False,
        #     name="ratingOption",
        #     grouped_separators={"ratingHistogram": "\n"},
        # ),
        # "named_array_field": FieldOption(named=True, name="name", grouped=True),
        # "c": FieldOption(named=False, name="name", grouped=True)
    }
    test_headers_renaming = [
        (r"offers\[0\]->", ""),
        (r"aggregateRating->", ""),
        (r"additionalProperty->(.*)->value", r"\1"),
        (r"breadcrumbs->name", "breadcrumbs"),
        (r"breadcrumbs->link", "breadcrumbs links"),
    ]
    # Define how many elements of array to process
    test_array_limits = {"offers": 1}

    # DATA TO PROCESS
    # file_name = "items_simple_test.json"
    # item_list = json.loads(
    #     resource_string(__name__, f"tests/assets/{file_name}").decode("utf-8")
    # )
    file_name = "waka.json"
    item_list = [
        {"c": {"name": "color", "value": "green"}},
        {"c": {"name": "color", "value": None}},
        {"c": {"name": "color", "value": [1, 2, 3]}},
        # {"c": {"name": "color", "value": "cyan", "meta": {"some": "data"}}},
        # {"c": {"name": "color", "value": "blue"}},
    ]

    # AUTOCRAWL PART
    autocrawl_csv_sc = CSVStatsCollector()
    # Items could be processed in batch or one-by-one through `process_object`
    autocrawl_csv_sc.process_items(item_list)
    autocrawl_stats = autocrawl_csv_sc.stats
    from pprint import pprint

    pprint(autocrawl_stats)

    # BACKEND PART (assuming we send stats to backend)
    csv_exporter = CSVExporter(
        default_stats=autocrawl_stats,
        field_options=test_field_options,
        array_limits=test_array_limits,
        headers_renaming=test_headers_renaming,
    )
    # Items could be exported in batch or one-by-one through `export_item_as_row`

    csv_exporter.export_csv(
        item_list, f"autocrawl/utils/csv_assets/{file_name.replace('.json', '.csv')}"
    )
