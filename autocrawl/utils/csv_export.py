import csv
import json  # NOQA
import logging
import re
from functools import wraps
from os import PathLike
from typing import Dict, List, Set, TextIO, Tuple, TypedDict, Union

import attr  # NOQA

# Using scalpl (instead of jmespath/etc.) as an existing fast backend dependency
from pkg_resources import resource_string  # NOQA
from scalpl import Cut  # NOQA

logger = logging.getLogger(__name__)


class Property(TypedDict):
    values: Dict[Union[str, int, float, bool, None], None]
    limited: bool


class Header(TypedDict, total=False):
    count: int
    properties: Dict[str, Property]
    type: str


class FieldOption(TypedDict, total=False):
    """
    Optional field options to format data.
    - Options could be named (named=True, name="property_name"), so the exporter will try
      to create columns based on the values of the property provided in the "name" attribute.
    - Options could be grouped (grouped=True), so the exporter will try to fit
      all the data for this field into a single cell.
    - Options could be both named and grouped, so the exporter will try to get data collected
      for each named property and fit all this data in a single field.
    """

    name: str
    named: bool
    grouped: bool
    grouped_separators: Dict[str, str]


def is_hashable(value):
    # The list is not full: tuples, for example, could be used as dict keys (hashable),
    # but for our case we should avoid using them to not to hurt readability
    if isinstance(value, (str, int, float, complex, bool)) or value is None:
        return True
    else:
        return False


def is_list(value):
    # Sets are ignored because they're not indexed,
    # so stats can't be extracted in a required way
    return True if isinstance(value, (list, tuple)) else False


def prepare_io(func):
    @wraps(func)
    def prepare_io_wrapper(self, *args, **kwargs):
        if "export_path" in kwargs:
            csv_io, need_to_close = self._prepare_io(kwargs["export_path"])
            kwargs["export_path"]: str = csv_io
        else:
            csv_io, need_to_close = self._prepare_io(args[-1])
            args = list(args)
            args[-1]: str = csv_io
        func(self, *args, **kwargs)
        if need_to_close:
            csv_io.close()

    return prepare_io_wrapper


@attr.s(auto_attribs=True)
class CSVStatsCollector:
    """
    Collect stats from processed items to get the max required number of columns
    for each field, collect values for later grouping/naming using FieldOption's.
    """

    # How many named columns could be created for a single field. For example, you
    # have a set of additional properties like {"name": "color", "value": "blue"}.
    # If you decide to create a separate column for each one of them ("color", "size", etc.),
    # the limit defines how much data would be collected to make it work.
    # If the limit is hit (too many columns) - no named columns would be created in export.
    named_columns_limit: int = attr.ib(default=20)
    # Separator to place values from items to required columns. Used instead of default `.`.
    # If your properties names include the separator - replace it with a custom one.
    cut_separator: str = attr.ib(default="->")
    # Stats for each field, collected by processing items
    _stats: Dict[str, Header] = attr.ib(init=False, default=attr.Factory(dict))
    # TODO Add description
    _invalid_properties: Set[str] = attr.ib(init=False, default=attr.Factory(set))

    @property
    def stats(self):
        return {
            "stats": self._stats,
            "invalid_properties": list(self._invalid_properties),
        }

    def process_items(self, items: List[Dict]):
        """
        Validating and collecting stats for provided items.
        Errors raised by the method should stay in CSVStatsCollector to avoid invalid/broken inputs.
        """
        if not is_list(items):
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
        elif is_list(items[0]):
            raise TypeError("Items must be dicts (not arrays) to be supported.")
        else:
            raise ValueError(f"Unsupported item type ({type(items[0])}).")

    def _process_array(self, array_value: List, prefix: str = ""):
        if len(array_value) == 0 or prefix in self._invalid_properties:
            return
        elements_types = set([type(x) for x in array_value])
        for et in ((dict,), (list, tuple)):
            if len(set([x in et for x in elements_types])) > 1:
                logger.warning(
                    f"{str(et)}'s can't be mixed with other types in an array ({prefix})."
                )
                self._invalid_properties.add(prefix)
                return
                # raise ValueError(
                #     f"{str(et)}'s can't be mixed with other types in an array ({prefix})."
                # )
        if self._stats.get(prefix) is None:
            self._stats[prefix] = {"count": 0, "properties": {}, "type": "array"}
        if is_hashable(array_value[0]):
            self._stats[prefix]["count"] = max(
                self._stats[prefix]["count"], len(array_value)
            )
        elif is_list(array_value[0]):
            for i, element in enumerate(array_value):
                property_path = f"{prefix}[{i}]"
                self._process_array(element, property_path)
        else:
            self._process_base_array(array_value, prefix)

    def _process_base_array(self, array_value: List, prefix: str):
        if self._stats[prefix]["count"] < len(array_value):
            self._stats[prefix]["count"] = len(array_value)
        # Checking manually to keep properties order instead of checking subsets
        for i, element in enumerate(array_value):
            for property_name, property_value in element.items():
                property_path = f"{prefix}[{i}]{self.cut_separator}{property_name}"
                if property_path in self._invalid_properties:
                    continue
                if is_hashable(property_value):
                    self._process_hashable_value(property_name, property_value, prefix)
                elif is_list(property_value):
                    self._process_array(property_value, property_path)
                elif isinstance(property_value, dict):
                    self.process_object(property_value, property_path)
                else:
                    logger.warning(
                        f'Unsupported value type "{type(property_value)}" ({property_value}) '
                        f'for property "{property_path}" ({prefix}).'
                    )
                    self._invalid_properties.add(property_path)
                    # self._stats[prefix] = {"invalid": True}
                    # return
                    # raise ValueError(
                    #     f'Unsupported value type "{type(property_value)}" ({property_path}) '
                    #     f'for property "{property_name}" ({prefix}).'
                    # )

    def process_object(self, object_value: Dict, prefix: str = ""):
        if prefix in self._invalid_properties:
            return
        values_hashable = {k: is_hashable(v) for k, v in object_value.items()}
        # `count: 0` for objects means that some items for this prefix
        # had non-hashable values, so all next values should be processed as non-hashable ones
        if self._stats.get(prefix, {}).get("count") == 0:
            self._process_base_object(object_value, prefix, values_hashable)
            return

        # If everything is hashable - collect names and values, so the field could be grouped later
        # Skip if init (no prefix) to avoid parenting like `->value` because no parent is present
        if all(values_hashable.values()) and prefix:
            self._process_hashable_object(object_value, prefix)
        else:
            # If property values are not all hashable, but there're properties saved for the prefix
            # it means that for previous items they were all hashable, so need to rebuild previous stats
            if self._stats.get(prefix, {}).get("properties"):
                prev_stats = self._stats.pop(prefix)
                for name, values in prev_stats.get("properties", {}).items():
                    for value, _ in values.get("values", {}).items():
                        self._process_base_object({name: value}, prefix)
            # Mark that prefix has non-hashable values, so no need to collect properties/values/names
            if prefix:
                self._stats[prefix] = {"count": 0, "type": "object"}
            self._process_base_object(object_value, prefix, values_hashable)

    def _process_base_object(
        self,
        object_value: Dict,
        prefix: str = "",
        values_hashable: Dict[str, bool] = None,
    ):
        for property_name, property_value in object_value.items():
            # Skip None values; if there're items with actual values for
            # this property - it will be filled as "" automatically
            if property_value is None:
                continue
            property_path = (
                f"{prefix}{self.cut_separator}{property_name}"
                if prefix
                else property_name
            )
            if property_path in self._invalid_properties:
                continue
            property_stats = self._stats.get(property_path)
            if values_hashable:
                # If hashable, but have existing non-empty properties
                if (
                    values_hashable[property_name]
                    and property_stats != {}
                    and property_stats is not None
                ):
                    # TODO Add boolean check if the user want to don't throw an error and just stringify
                    logger.warning(
                        f"Field ({property_path}) was processed as non-hashable "
                        f"but later got hashable value: ({property_value})"
                    )
                    self._invalid_properties.add(property_path)
                    continue
                    # self._stats[prefix] = {"invalid": True}
                    # return
                    # raise ValueError(
                    #     f"Field ({property_path}) was processed as non-hashable "
                    #     f"but later got hashable value: ({property_value})"
                    # )
                # If not hashable, but doesn't have properties
                if not values_hashable[property_name] and property_stats == {}:
                    logger.warning(
                        f"Field ({property_path}) was processed as hashable "
                        f"but later got non-hashable value: ({property_value})"
                    )
                    self._invalid_properties.add(property_path)
                    continue
                    # self._stats[prefix] = {"invalid": True}
                    # return
                    # raise ValueError(
                    #     f"Field ({property_path}) was processed as hashable "
                    #     f"but later got non-hashable value: ({property_value})"
                    # )
            property_type = property_stats.get("type") if property_stats else None
            if property_type and not isinstance(
                property_value, self._map_types(property_name, property_type)
            ):
                # Not throwing an error here, but if type was changed from dict to list - the exporter
                # would throw TypeError because collected dict keys can't be accesed in list
                logger.warning(
                    f'Field ({property_path}) value changed the type from "{property_type}" '
                    f"to {type(property_value)}: ({property_value})"
                )
                self._invalid_properties.add(property_path)
                continue
                # self._stats[prefix] = {"invalid": True}
                # return
                # raise ValueError(
                #     f'Field ({property_path}) value changed the type from "{property_type}" '
                #     f"to {type(property_value)}: ({property_value})"
                # )
            if is_hashable(property_value):
                if self._stats.get(property_path) is None:
                    self._stats[property_path] = {}
            elif is_list(property_value):
                self._process_array(object_value[property_name], property_path)
            elif isinstance(property_value, dict):
                self.process_object(object_value[property_name], property_path)
            else:
                logger.warning(
                    f'Unsupported value type "{type(property_value)}" ({property_value}) '
                    f'for property "{property_path}" ({prefix}).'
                )
                self._invalid_properties.add(property_path)
                # Adding empty stats so the property could be stringified later
                self._stats[property_path] = {}
                # self._stats[prefix] = {"invalid": True}
                # return
                # raise ValueError(
                #     f'Unsupported value type "{type(property_value)}" ({property_value}) '
                #     f'for property "{property_path}" ({prefix}).'
                # )

    def _process_hashable_object(self, object_value: Dict, prefix: str = ""):
        if not self._stats.get(prefix):
            self._stats[prefix] = {"properties": {}, "type": "object"}
        for property_name, property_value in object_value.items():
            # Skip None values; if there're items with actual values for
            # this property - it will be filled as "" automatically
            if property_value is None:
                continue
            self._process_hashable_value(property_name, property_value, prefix)

    def _process_hashable_value(
        self,
        property_name: str,
        property_value: Union[str, int, float, bool, None],
        prefix: str,
    ):
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
            return
        self._stats[prefix]["properties"][property_name]["values"][
            property_value
        ] = None
        if len(property_data.get("values", {})) > self.named_columns_limit:
            # Clear previously collected values if the limit was hit to avoid partly processed columns
            self._stats[prefix]["properties"][property_name]["values"] = {}
            self._stats[prefix]["properties"][property_name]["limited"] = True
            return

    @staticmethod
    def _map_types(property_name: str, type_name: str):
        types = {"object": dict, "array": (list, tuple)}
        mapped_type = types.get(type_name)
        if type_name:
            return mapped_type
        else:
            raise TypeError(
                f"Unexpected property type ({type_name}) for property ({property_name})."
            )


@attr.s(auto_attribs=True)
class CSVExporter:
    """
    Export items as CSV based on the previously collected stats.
    Detailed documentation with examples: <place_for_a_link>
    """

    # Items stats (CSVStatsCollector)
    stats: Dict[str, Header] = attr.ib()
    # List of properties that had invalid data during stats collection
    invalid_properties: List[str] = attr.ib()
    # If True: all invalid data would be stringified
    # If False: all columns with invalid data would be skipped
    strinfigy_invalid: bool = attr.ib(default=True)
    # Optional field options to format data (FieldOption class).
    field_options: Dict[str, FieldOption] = attr.ib(default=attr.Factory(dict))
    # Limit for the arrays to export only first N elements ({"offers": 1})
    array_limits: Dict[str, int] = attr.ib(default=attr.Factory(dict))
    # Set of regexp rules to rename existing item colulmns (r"offers\[0\]->", "")
    # The first value is the pattern to replace, while the second one is the replacement
    headers_renaming: List[Tuple[str, str]] = attr.ib(default=attr.Factory(list))
    # List to sort CSV headers. All headers that are present both it this list and actual
    # file - would be sorted. All other headers would be appended in a natural order.
    # Headers should be provided in the form before renaming ("offers[0]->price", not "Price").
    # TODO: Add validator
    headers_order: List[str] = attr.ib(default=attr.Factory(list))
    # TODO Add description and validator
    headers_filters: List[str] = attr.ib(default=attr.Factory(list))
    # Separator to divide values when grouping data in a single cell (grouped=True)
    grouped_separator: str = attr.ib(default="\n")
    # Separator to place values from items to required columns. Used instead of default `.`.
    # If your properties names include the separator - replace it with a custom one.
    cut_separator: str = attr.ib(default="->")
    # CSV headers generated from item stats
    _headers: List[str] = attr.ib(init=False, default=attr.Factory(list))

    def __attrs_post_init__(self):
        self.field_options = self._prepare_field_options(self.field_options)
        self._prepare_for_export()

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
            property_stats = self.stats.get(property_name)
            if not property_stats:
                continue
            if property_value.get("named"):
                name = property_value["name"]
                if not property_stats.get("properties"):
                    raise ValueError(
                        f'Field "{property_name}" doesn\'t have any properties '
                        f'(as an array of hashable elements), so "named" option can\'t be applied.'
                    )
                if not property_stats["properties"].get(name):
                    raise ValueError(
                        f'Field "{property_name}" doesn\'t have name property '
                        f"\"{property_value['name']}\", so \"named\" option can't be applied."
                    )
                # If property is both grouped and named - we don't care columns limit because
                # everytihg will be grouped in a single cell
                if not property_value.get("grouped"):
                    if property_stats["properties"][name].get("limited"):
                        raise ValueError(
                            f"Field \"{property_name}\" values for name property \"{property_value['name']}\" "
                            f'were limited by "named_columns_limit" when collecting stats, '
                            f'so "named" option can\'t be applied.'
                        )

    @staticmethod
    def _prepare_io(
        export_path: Union[str, bytes, PathLike, TextIO]
    ) -> Tuple[TextIO, bool]:
        need_to_close = False
        if isinstance(export_path, (str, bytes, PathLike)):
            export_file = open(export_path, mode="w", newline="")
            need_to_close = True
        elif hasattr(export_path, "write"):
            export_file = export_path
        else:
            raise TypeError(f"Unexpeted export_path type ({type(export_path)}).")
        return export_file, need_to_close

    @staticmethod
    def _prepare_field_options(
        properties: Dict[str, FieldOption]
    ) -> Dict[str, FieldOption]:
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
        return properties

    @headers_renaming.validator
    def check_headers_renaming(self, _, value: List[Tuple[str, str]]):
        if not is_list(value):
            raise ValueError(
                "Headers renamings must be provided as a list/tuple of tuples."
            )
        for rmp in value:
            if not is_list(rmp):
                raise ValueError(f"Headers renamings ({rmp}) must be lists/tuples.")
            if len(rmp) != 2:
                raise ValueError(
                    f"Headers renamings ({rmp}) must include two elements: pattern and replacement."
                )
            if any([not isinstance(x, str) for x in rmp]):
                raise ValueError(f"Headers renamings ({rmp}) elements must be strings.")

    def _convert_stats_to_headers(
        self,
        stats: Dict[str, Header],
        separator: str,
        field_options: Dict[str, FieldOption],
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
                    if properties:
                        return [f"{field}{separator}{key}" for key in properties]
                    else:
                        return headers
                else:
                    # Group everything in a single cell if not
                    return headers
            # Regular case. Handle arrays.
            if count is not None:
                headers = [f"{f}[{i}]" for f in headers for i in range(count)]
            if properties:
                headers = [f"{f}{separator}{pr}" for f in headers for pr in properties]
            return headers

        processed_headers = [
            f
            for field, meta in stats.items()
            for f in expand(field, meta, field_options.get(field, {}))
        ]
        # Skip columns with invalid data
        if not self.strinfigy_invalid:
            return [x for x in processed_headers if x not in self.invalid_properties]
        else:
            return processed_headers

    def _limit_field_elements(self):
        """
        Limit number of elements exported based on pre-defined limits
        """
        filters = set()
        # Find fields that need to be limited
        for key, value in self.array_limits.items():
            if key not in self.stats:
                continue
            count = self.stats[key].get("count")
            if not count:
                continue
            for i in range(value, count):
                filters.add(f"{key}[{i}]")
            if count > value:
                self.stats[key]["count"] = value
        limited_stats = {}
        # Limit field elements
        for field, stats in self.stats.items():
            for key in filters:
                if field.startswith(key):
                    break
            else:
                limited_stats[field] = stats
        self.stats = limited_stats

    def _filter_headers(self):
        if not self.headers_filters:
            return
        filtered_headers = []
        for header in self._headers:
            for ft in self.headers_filters:
                if re.match(ft, header):
                    filtered_headers.append(header)
                    break
        self._headers = [x for x in self._headers if x not in filtered_headers]

    def _sort_headers(self):
        if not self.headers_order:
            return
        ordered_headers = []
        for head in self.headers_order:
            if head in self._headers:
                ordered_headers.append(self._headers.pop(self._headers.index(head)))
        self._headers = ordered_headers + self._headers

    def _prepare_for_export(self):
        # If headers are set - they've been processed already and ready for export
        if self._headers:
            return
        self._limit_field_elements()
        separator = self.cut_separator
        self._headers = self._convert_stats_to_headers(
            self.stats, separator, self.field_options
        )
        self._filter_headers()
        self._sort_headers()
        # TODO Think about implementing custom headers filtering

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
                return self._export_grouped_field(item_data, header_path, separator)
            # Grouped AND Named
            else:
                return self._export_grouped_and_named_field(
                    item_data, header_path, separator
                )
        # Named; if not grouped and not named - adjusted property was filtered
        else:
            return self._export_named_field(item_data, header_path)

    def _export_grouped_field(
        self, item_data: Cut, header_path: List[str], separator: str
    ) -> str:
        if len(header_path) == 1:
            value = item_data.get(header_path[0])
            if value is None:
                return ""
            elif is_hashable(value):
                return value
            elif is_list(value):
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
        else:
            value = []
            for element in item_data.get(header_path[0], []):
                if element.get(header_path[1]) is not None:
                    value.append(element[header_path[1]])
                else:
                    # Add empty values to make all grouped columns the same height for better readability
                    value.append("")
            return separator.join(
                [self._escape_grouped_data(x, separator) for x in value]
            )

    def _export_grouped_and_named_field(
        self, item_data: Cut, header_path: List[str], separator: str
    ) -> str:
        name = self.field_options[header_path[0]]["name"]
        values = []
        for element in item_data.get(header_path[0], []):
            element_name = element.get(name, "")
            element_values = []
            for property_name, property_value in element.items():
                if property_name == name:
                    continue
                element_values.append((property_name, property_value))
            # Check how many properties, except name, the field has
            properties_stats = [
                x
                for x in self.stats.get(header_path[0], {}).get("properties", {}).keys()
                if x != name
            ]
            # If there're more then one - use name as a header and other properties as separate rows
            if len(properties_stats) > 1:
                element_str = separator.join(
                    [f"{pn}: {pv}" for pn, pv in element_values]
                )
                values.append(f"- {element_name}{separator}{element_str}")
            # If only one (like in {"name": "color", "value": "green"}) - use name instead of property
            else:
                values.append(
                    f"{element_name}: {','.join([pv for pn, pv in element_values])}"
                )

        return separator.join([self._escape_grouped_data(x, separator) for x in values])

    def _export_named_field(self, item_data: Cut, header_path: List[str]) -> str:
        name = self.field_options[header_path[0]]["name"]
        elements = item_data.get(header_path[0], [])
        if is_list(elements):
            for element in elements:
                if element.get(name) == header_path[1]:
                    return element.get(header_path[2], "")
            else:
                return ""
        elif isinstance(elements, dict):
            for element_key, element_value in elements.items():
                if element_key == header_path[2]:
                    return element_value
            else:
                return ""
        else:
            raise ValueError(
                f"Unexpected value type ({type(elements)}) for field ({header_path}): {elements}"
            )

    def export_item_as_row(self, item: Dict) -> List:
        row = []
        separator = self.cut_separator
        item_data = Cut(item, sep=separator)
        for header in self._headers:
            # Stringify invalid data
            if self.strinfigy_invalid and header in self.invalid_properties:
                row.append(str(item_data.get(header, "")))
                continue
            header_path = header.split(separator)
            # TODO Check nested grouping as `c[0]->list | grouped=True`
            if header_path[0] not in self.field_options:
                try:
                    row.append(item_data.get(header, ""))
                except TypeError:
                    # Could be an often case, so commenting to avoid overflowing logs
                    # logger.debug(f"{er} Returning empty data.")
                    row.append("")
            else:
                row.append(
                    self._export_field_with_options(header, header_path, item_data)
                )
        print(row)
        return row

    def _get_renamed_headers(self, capitalize: bool = True) -> List[str]:
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

    @prepare_io
    def export_csv_headers(self, export_path):
        csv_writer = csv.writer(
            export_path, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        csv_writer.writerow(self._get_renamed_headers())

    @prepare_io
    def export_csv_row(self, item: Dict, export_path):
        csv_writer = csv.writer(
            export_path, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        csv_writer.writerow(self.export_item_as_row(item))

    @prepare_io
    def export_csv_full(self, items: List[Dict], export_path):
        csv_writer = csv.writer(
            export_path, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        csv_writer.writerow(self._get_renamed_headers())
        for p in items:
            csv_writer.writerow(self.export_item_as_row(p))


if __name__ == "__main__":
    # CUSTOM OPTIONS
    test_field_options: Dict[str, FieldOption] = {
        # "gtin": FieldOption(named=True, grouped=False, name="type"),
        # "additionalProperty": FieldOption(
        #     named=True,
        #     grouped=True,
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
        # TODO What should happend if hashable dict if both grouped and named? I assume, that should be impossible?
        # TODO Test nested cases like `c->list`
        # TODO Check arrays of arrays processing, but not on item level, but on nested level
        # "c": FieldOption(named=False, name="name", grouped=True),
    }
    test_headers_renaming = [
        (r"^offers\[0\]->", ""),
        (r"^aggregateRating->", ""),
        (r"^additionalProperty->(.*)->value", r"\1"),
        (r"^breadcrumbs->name", "breadcrumbs"),
        (r"^breadcrumbs->link", "breadcrumbs links"),
    ]
    test_headers_order = ["name", "sku"]
    test_headers_filters = ["_key", r"^breadcrumbs.*", "^images.*"]

    # Define how many elements of array to process
    test_array_limits = {"offers": 1}

    # DATA TO PROCESS
    # file_name = "products_simple_xod_test.json"
    # item_list = json.loads(
    #     resource_string(__name__, f"tests/assets/{file_name}").decode("utf-8")
    # )
    file_name = "custom.json"
    item_list: List[Dict] = [
        # DONE: Mixed arrays covered
        # {"c": [[1, 2], (3, 4), 123]},
        # {"c": [[1, 2], (3, 4), "text"]},
        # {"c": [[1, 2], (3, 4), {1, 2, 3}]},
        # {"c": [[1, 2], (3, 4), False]},
        # # TODO: Update _process_base_array - Unsupported value type
        # These ones look file
        # TODO Think what to do if datatype changes - from array to dict, so data would be inaccesible
        # I assume it's ok :)
        {"b": 123, "c": {"yoko": "yo", "waka": {1, 2}}},
        {"b": 123, "c": {"yoko": {43432, 543}, "waka": {1, 2}}},
        {"b": 123, "c": {"yoko": {43432, 543}}},
        {
            "b": 123,
            "c": [
                {"name": {1, 2}, "value": "somevalue1"},
                {"name": "somename", "value": "somevalue2"},
            ],
        },
        {
            "b": 456,
            "c": [
                {"name": "ok", "value": {3, 4}},
                {"name": "ok1", "value": "somevalue4"},
            ],
        },
        # TODO All nest obj must be invalid? Also order shouldn't matter
        # {
        #     "c": {
        #         "name": "somename1",
        #         "value": "somevalue1",
        #         "nestobj": {"nname": "somename1", "nvalue": {1, 2}},
        #     }
        # },
        # {
        #     "c": {
        #         "name": "somename1",
        #         "value": "somevalue1",
        #         "nestobj": {"nname": {1, 2}, "nvalue": "somevalue1"},
        #     }
        # },
        # {
        #     "c": {
        #         "name": "somename1",
        #         "value": "somevalue1",
        #         "nestobj": {"nname": "somename3", "nvalue": "somevalue3"},
        #     }
        # },
    ]

    # AUTOCRAWL PART
    autocrawl_csv_sc = CSVStatsCollector(named_columns_limit=50)
    # Items could be processed in batch or one-by-one through `process_object`
    autocrawl_csv_sc.process_items(item_list)
    from pprint import pprint

    pprint(autocrawl_csv_sc._stats)
    print("*" * 10)
    pprint(autocrawl_csv_sc._invalid_properties)
    print("*" * 10)

    # BACKEND PART (assuming we send stats to backend)
    csv_exporter = CSVExporter(
        stats=autocrawl_csv_sc.stats["stats"],
        invalid_properties=autocrawl_csv_sc.stats["invalid_properties"],
        # strinfigy_invalid=False,
        field_options=test_field_options,
        array_limits=test_array_limits,
        headers_renaming=test_headers_renaming,
        headers_order=test_headers_order,
        headers_filters=test_headers_filters,
    )

    pprint(csv_exporter._headers)
    print("*" * 10)
    # Items could be exported in batch or one-by-one through `export_item_as_row`
    csv_exporter.export_csv_full(
        item_list, f"autocrawl/utils/csv_assets/{file_name.replace('.json', '.csv')}"
    )

    # with open(f"autocrawl/utils/csv_assets/{file_name.replace('.json', '.csv')}", "w") as f:
    #     csv_exporter.export_csv_full(item_list, f)
