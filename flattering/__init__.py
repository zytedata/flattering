import csv
import json  # NOQA
import logging
import re
import sys
from functools import wraps
from os import PathLike
from typing import Dict, Hashable, List, TextIO, Tuple, Union

# Python 3.7 compatibility
if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

import attr

# Using scalpl (instead of jmespath/etc.) as an existing fast backend dependency
from scalpl import Cut

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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
    if (isinstance(value, Hashable) and not isinstance(value, tuple)) or value is None:
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
        export_path = kwargs.get("export_path") or (
            args[1] if len(args) >= 2 else args[0]
        )
        append = kwargs.get("append") or (args[-1] if len(args) == 3 else False)
        csv_io, need_to_close = self._prepare_io(export_path, append)
        if "export_path" in kwargs:
            kwargs["export_path"] = csv_io
        else:
            args = list(args)
            if len(args) >= 2:
                args[1] = csv_io
            else:
                args[0] = csv_io
        try:
            func(self, *args, **kwargs)
        finally:
            if need_to_close:
                csv_io.close()

    return prepare_io_wrapper


@attr.s(auto_attribs=True)
class StatsCollector:
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
    # Names of properties with invalid data (wrong/mixed types/etc.) + messages what happened
    _invalid_properties: Dict[str, str] = attr.ib(
        init=False, default=attr.Factory(dict)
    )

    @property
    def stats(self):
        return {
            "stats": self._filter_stats(self._stats),
            "invalid_properties": self._invalid_properties,
        }

    def process_items(self, items: List[Dict]):
        """
        Validating and collecting stats for provided items.
        Errors raised by the method should stay in StatsCollector to avoid invalid/broken inputs.
        """
        if not is_list(items):
            raise TypeError(f"Initial items data must be array, not {type(items)}.")
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
            raise TypeError(f"Unsupported item type ({type(items[0])}).")

    @staticmethod
    def _filter_stats(stats: Dict[str, Header]) -> Dict[str, Header]:
        """
        Filter stats that can't be used to be able to remove
        impossible field options to avoid bad formatting.
        """
        updated_stats = {}
        for s_key, s_value in stats.items():
            if s_value.get("count") == 0:
                continue
            updated_stats[s_key] = s_value
        return updated_stats

    def _process_array(self, array_value: List, prefix: str = ""):
        # Skip empty arrays or invalid columns that would be stringified
        if len(array_value) == 0 or (
            prefix in self._invalid_properties and self._stats[prefix] == {}
        ):
            return
        elements_types = set([type(x) for x in array_value])
        for et in ((dict,), (list, tuple)):
            if len(set([x in et for x in elements_types])) > 1:
                msg = f"{str(et)}'s can't be mixed with other types in an array ({prefix})."
                logger.warning(msg)
                self._invalid_properties[prefix] = msg
                break
        if self._stats.get(prefix) is None:
            self._stats[prefix] = {"count": 0, "properties": {}, "type": "array"}
        # Process invalid arrays as arrays of hashable objects because they would be either stringified or skipped
        if is_hashable(array_value[0]) or prefix in self._invalid_properties:
            self._stats[prefix]["count"] = max(
                self._stats[prefix]["count"], len(array_value)
            )
        elif is_list(array_value[0]):
            for i, element in enumerate(array_value):
                property_path = f"{prefix}[{i}]"
                self._process_array(element, property_path)
        # If objects
        else:
            self._process_base_array(array_value, prefix)

    def _process_base_array(self, array_value: List, prefix: str):
        has_hashable_values = False
        for i, element in enumerate(array_value):
            for property_name, property_value in element.items():
                property_path = f"{prefix}[{i}]{self.cut_separator}{property_name}"
                if property_path in self._invalid_properties:
                    continue
                if is_hashable(property_value):
                    self._process_hashable_value(property_name, property_value, prefix)
                    has_hashable_values = True
                elif is_list(property_value):
                    self._process_array(property_value, property_path)
                elif isinstance(property_value, dict):
                    self.process_object(property_value, property_path)
                else:
                    # TODO Add test case/example for that
                    msg = (
                        f'Unsupported value type "{type(property_value)}" ({property_value}) '
                        f'for property "{property_path}" ({prefix}).'
                    )
                    logger.warning(msg)
                    self._invalid_properties[property_path] = msg
        # Count makes sense only for arrays with properties and hashable values
        if self._stats[prefix].get("properties") or has_hashable_values:
            if self._stats[prefix]["count"] < len(array_value):
                self._stats[prefix]["count"] = len(array_value)

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
            property_stats = self._stats.get(property_path)
            if values_hashable:
                # If hashable, but have existing non-empty properties
                if (
                    values_hashable[property_name]
                    and property_stats != {}
                    and property_stats is not None
                ):
                    msg = (
                        f"Field ({property_path}) was processed as non-hashable "
                        f"but later got hashable value: ({property_value})"
                    )
                    logger.warning(msg)
                    self._invalid_properties[property_path] = msg
                    self.clear_outdated_stats(property_path)
                    # Setting empty stats so the property could be stringified later
                    self._stats[property_path] = {}
                    continue
                # If not hashable, but doesn't have properties
                elif not values_hashable[property_name] and property_stats == {}:
                    msg = (
                        f"Field ({property_path}) was processed as hashable "
                        f"but later got non-hashable value: ({property_value})"
                    )
                    logger.warning(msg)
                    self._invalid_properties[property_path] = msg
                    self.clear_outdated_stats(property_path)
                    # Setting empty stats so the property could be stringified later
                    self._stats[property_path] = {}
                    continue
            property_type = property_stats.get("type") if property_stats else None
            if (
                property_type
                and not isinstance(
                    property_value, self._map_types(property_name, property_type)
                )
                and property_path not in self._invalid_properties
            ):
                msg = (
                    f'Field ({property_path}) value changed the type from "{property_type}" '
                    f"to {type(property_value)}: ({property_value})"
                )
                logger.warning(msg)
                self._invalid_properties[property_path] = msg
                self.clear_outdated_stats(property_path)
                # Setting empty stats so the property could be stringified later
                self._stats[property_path] = {}
                continue
            elif is_hashable(property_value):
                if self._stats.get(property_path) is None:
                    self._stats[property_path] = {}
            elif is_list(property_value):
                self._process_array(object_value[property_name], property_path)
            elif isinstance(property_value, dict):
                self.process_object(object_value[property_name], property_path)
            else:
                msg = (
                    f'Unsupported value type "{type(property_value)}" ({property_value}) '
                    f'for property "{property_path}" ({prefix}).'
                )
                logger.warning(msg)
                self._invalid_properties[property_path] = msg
                self.clear_outdated_stats(property_path)
                # Setting empty stats so the property could be stringified later
                self._stats[property_path] = {}

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

    def clear_outdated_stats(self, prefix):
        """
        If property converted from array or dict to hashable, then all of the headers
        created for potential columns should be removed, because all the values would
        be stringified in a single column or skipped
        """
        self._stats = {
            k: v
            for k, v in self._stats.items()
            if not re.match(
                r"^(" + prefix + r"\[\d+\].*|" + prefix + self.cut_separator + r".*)", k
            )
        }


@attr.s(auto_attribs=True)
class Exporter:
    """
    Export items as CSV based on the previously collected stats.
    Detailed documentation with examples: <place_for_a_link>
    """

    # Items stats (StatsCollector)
    stats: Dict[str, Header] = attr.ib()
    # Properties that had invalid data during stats collection + messages what happened
    invalid_properties: Dict[str, str] = attr.ib()
    # If True: all invalid data would be stringified
    # If False: all columns with invalid data would be skipped
    stringify_invalid: bool = attr.ib(default=True)
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
    headers_order: List[str] = attr.ib(default=attr.Factory(list))
    # Regex statements. If the header matches any of these statements the column would be skipped.
    headers_filters: List[str] = attr.ib(default=attr.Factory(list))
    # Separator to divide values when grouping data in a single cell (grouped=True)
    grouped_separator: str = attr.ib(default="\n")
    # Separator to place values from items to required columns. Used instead of default `.`.
    # If your properties names include the separator - replace it with a custom one.
    cut_separator: str = attr.ib(default="->")
    # Capitalize fist letter of CSV headers when exporting
    capitalize_headers: bool = attr.ib(default=False)
    # CSV headers generated from item stats
    _headers: List[str] = attr.ib(init=False, default=attr.Factory(list))

    # TODO Add headers_match support
    # Middle storage to allow applying filters and renaming rules to the renamed headers
    # _headers_match: Dict[str, str] = attr.ib(default=attr.Factory(dict))

    def __attrs_post_init__(self):
        self._vocalize_invalid_properties()
        self._validate_field_options()
        self._validate_headers_order()
        self._validate_headers_filters()
        self._prepare_for_export()

    @staticmethod
    def _prepare_io(export_path: Union[str, bytes, PathLike, TextIO], append: bool):
        need_to_close = False
        write_mode = "a" if append else "w"
        if isinstance(export_path, (str, bytes, PathLike)):
            export_file = open(export_path, mode=write_mode, newline="")
            need_to_close = True
        elif hasattr(export_path, "write"):
            export_file = export_path
        else:
            raise TypeError(f"Unexpeted export_path type ({type(export_path)}).")
        return export_file, need_to_close

    def _validate_headers_order(self):
        validated_headers_order = []
        for header in self.headers_order:
            if not isinstance(header, str):
                logger.warning(
                    f"Headers provided through headers_order must be strings, not {type(header)}. "
                    f"Header {header} would be skipped."
                )
                continue
            validated_headers_order.append(header)
        self.headers_order = validated_headers_order

    def _validate_headers_filters(self):
        validated_headers_filters = []
        for header_filter in self.headers_filters:
            if not isinstance(header_filter, str):
                logger.warning(
                    f"Regex statements provided through headers_filters must be strings, not {type(header_filter)}. "
                    f"Header filter {header_filter} will be skipped."
                )
                continue
            validated_headers_filters.append(header_filter)
        self.headers_filters = validated_headers_filters

    def _validate_field_options(self):
        """
        Validate and filter field options that can't be applied.
        """
        validated_field_options = {}
        allowed_separators = (";", ",", "\n", ">", " ")
        for property_name, property_value in self.field_options.items():
            invalid_option = False
            if property_name not in self.stats:
                logger.warning(
                    f'Field option for field "{property_name}" will be skipped. Either this field doesn\'t '
                    f"exist, or input items have invalid data, so can't be grouped or named."
                )
                continue
            if not property_value.get("named") and not property_value.get("grouped"):
                logger.warning(
                    f"Field options must be either `named` or `grouped` or both. "
                    f'Field option "{property_name}" will be skipped.'
                )
                continue
            for tp in ("named", "grouped"):
                if not isinstance(property_value.get(tp), bool):  # NOQA
                    logger.warning(
                        f"Adjusted properties ({property_name}) must include `{tp}` parameter with boolean value. "
                        f'Field option "{property_name}" will be skipped.'
                    )
                    invalid_option = True
                    break
            if invalid_option:
                continue
            if property_value.get("named") and not property_value.get("name"):
                logger.warning(
                    f"Named adjusted properties ({property_name}) must include `name` parameter. "
                    f'Field option "{property_name}" will be skipped.'
                )
                continue
            for key, value in property_value.get("grouped_separators", {}).items():
                if not all([x in allowed_separators for x in value]):
                    logger.warning(
                        f"Only {allowed_separators} could be used"
                        f" as custom grouped separators ({key}:{value}). "
                        f'Field option "{property_name}" will be skipped.'
                    )
                    invalid_option = True
                    break
            if invalid_option:
                continue
            property_stats = self.stats.get(property_name)
            if not property_stats:
                continue
            if property_value.get("named"):
                name = property_value["name"]
                if not property_stats.get("properties"):
                    logger.warning(
                        f'Field "{property_name}" doesn\'t have any properties '
                        f'(not array of objects), so "named" option can\'t be applied. '
                        f'Field option "{property_name}" will be skipped.'
                    )
                    continue
                if not property_stats["properties"].get(name):
                    logger.warning(
                        f'Field "{property_name}" doesn\'t have name property '
                        f"\"{property_value['name']}\", so \"named\" option can't be applied. "
                        f'Field option "{property_name}" will be skipped.'
                    )
                    continue
                # If property is both grouped and named - we don't care columns limit because
                # everytihg will be grouped in a single cell
                if not property_value.get("grouped"):
                    if property_stats["properties"][name].get("limited"):
                        logger.warning(
                            f"Field \"{property_name}\" values for name property \"{property_value['name']}\" "
                            f'were limited by "named_columns_limit" when collecting stats, '
                            f'so "named" option can\'t be applied. '
                            f'Field option "{property_name}" will be skipped.'
                        )
                        continue
                # If grouped - need to check it's an array. No sense to use both `grouped` and `named`
                # for dict with hashable values, because each key could appear only once anyway.
                else:
                    if not property_stats.get("count"):
                        logger.warning(
                            f"Only arrays of objects could be grouped and named at the same time. "
                            f'Field "{property_name}" isn\'t an array, so field option '
                            f'"{property_name}" will be skipped.'
                        )
                        continue
            validated_field_options[property_name] = property_value
        self.field_options = validated_field_options

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

    def _vocalize_invalid_properties(self):
        if not self.invalid_properties:
            return
        logger.info(
            f"Columns with invalid data would be {'stringified' if self.stringify_invalid else 'skipped'}."
        )
        for prop, prop_msg in self.invalid_properties.items():
            if self.stringify_invalid:
                msg = f'All the data in column "{prop}" would be stringified because of data errors:'
            else:
                msg = f'Column "{prop}" would be skipped because of data errors:'
            msg += "\n" + prop_msg
            logger.info(msg)

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

        # Skip columns with invalid data
        if not self.stringify_invalid:
            processed_headers = [
                f
                for field, meta in stats.items()
                if field not in self.invalid_properties
                for f in expand(field, meta, field_options.get(field, {}))
            ]
        else:
            processed_headers = [
                f
                for field, meta in stats.items()
                for f in expand(field, meta, field_options.get(field, {}))
            ]
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

    @staticmethod
    def _escape_grouped_data(value, separator):
        if not value:
            return value
        escaped_separator = f"\\{separator}" if separator != "\n" else "\\n"
        return str(value).replace(separator, escaped_separator)

    def export_item_as_row(self, item: Dict) -> List:
        row = []
        separator = self.cut_separator
        item_data = Cut(item, sep=separator)
        for header in self._headers:
            # Stringify invalid data
            if self.stringify_invalid and header in self.invalid_properties:
                row.append(str(item_data.get(header, "")))
                continue
            header_path = header.split(separator)
            # TODO Check all possible paths (from 0 to end), pick first available
            # Log that all deeper ones would be skipped
            main_header = None
            child_headers = []
            for i in range(len(header_path)):
                # TODO Maybe pre-filter field options at the start, instead for each separate item?
                option_path = self.cut_separator.join(header_path[0 : i + 1])
                if option_path in self.field_options:
                    if not main_header:
                        main_header = option_path
                        child_headers = header_path[i + 1 :]
                    else:
                        logger.info(
                            f'Field option for field "{option_path}" would be ignored '
                            f'because option for higher level field "{main_header}" exists.'
                        )
            if main_header:
                row.append(
                    self._export_field_with_options(
                        header, main_header, child_headers, item_data
                    )
                )
            else:
                try:
                    value = item_data.get(header, "")
                    row.append(str(value) if value is not None else "")
                except TypeError:
                    # Could be an often case, so commenting to avoid overflowing logs
                    # logger.debug(f"{er} Returning empty data.")
                    row.append("")
        return row

    def _export_field_with_options(
        self, header: str, main_header: str, child_headers: List[str], item_data: Cut
    ) -> str:
        if self.field_options[main_header]["grouped"]:
            separator = (
                self.field_options.get(main_header, {})
                .get("grouped_separators", {})
                .get(header)
                or self.grouped_separator
            )
            # Grouped
            if not self.field_options[main_header]["named"]:
                return self._export_grouped_field(
                    item_data, main_header, child_headers, separator
                )
            # Grouped AND Named
            else:
                return self._export_grouped_and_named_field(
                    item_data, main_header, separator
                )
        # Named; if not grouped and not named - adjusted property was filtered
        else:
            return self._export_named_field(item_data, main_header, child_headers)

    def _export_grouped_field(
        self, item_data: Cut, main_header: str, child_headers: List[str], separator: str
    ) -> str:
        if len(child_headers) == 0:
            value = item_data.get(main_header)
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
            for element in item_data.get(main_header, []):
                if element.get(child_headers[0]) is not None:
                    value.append(element[child_headers[0]])
                else:
                    # Add empty values to make all grouped columns the same height for better readability
                    value.append("")
            return separator.join(
                [self._escape_grouped_data(x, separator) for x in value]
            )

    def _export_grouped_and_named_field(
        self, item_data: Cut, main_header: str, separator: str
    ) -> str:
        name = self.field_options[main_header]["name"]
        values = []
        for element in item_data.get(main_header, []):
            element_name = element.get(name, "")
            element_values = []
            for property_name, property_value in element.items():
                if property_name == name:
                    continue
                element_values.append((property_name, property_value))
            # Check how many properties, except name, the field has
            properties_stats = [
                x
                for x in self.stats.get(main_header, {}).get("properties", {}).keys()
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
                    f"{element_name}: {','.join([str(pv) for pn, pv in element_values])}"
                )
        return separator.join([self._escape_grouped_data(x, separator) for x in values])

    def _export_named_field(
        self, item_data: Cut, main_header: str, child_headers: List[str]
    ) -> str:
        name = self.field_options[main_header]["name"]
        elements = item_data.get(main_header, [])
        if is_list(elements):
            for element in elements:
                if element.get(name) == child_headers[0]:
                    return element.get(child_headers[1], "")
            else:
                return ""
        elif isinstance(elements, dict):
            for element_key, element_value in elements.items():
                if element_key == child_headers[1]:
                    return element_value
            else:
                return ""
        else:
            raise ValueError(
                f"Unexpected value type ({type(elements)}) for field ({[main_header] + child_headers}): {elements}"
            )

    def _get_renamed_headers(self) -> List[str]:
        if not self.headers_renaming:
            return self._headers
        renamed_headers = []
        for header in self._headers:
            for old, new in self.headers_renaming:
                header = re.sub(old, new, header)
            if self.capitalize_headers and header:
                header = header[:1].capitalize() + header[1:]
            renamed_headers.append(header)
        return renamed_headers

    @prepare_io
    def export_csv_headers(self, export_path, append: bool = False):
        csv_writer = csv.writer(
            export_path, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        csv_writer.writerow(self._get_renamed_headers())

    @prepare_io
    def export_csv_row(self, item: Dict, export_path, append: bool = False):
        csv_writer = csv.writer(
            export_path, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        csv_writer.writerow(self.export_item_as_row(item))

    @prepare_io
    def export_csv_full(self, items: List[Dict], export_path, append: bool = False):
        csv_writer = csv.writer(
            export_path, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        csv_writer.writerow(self._get_renamed_headers())
        for p in items:
            csv_writer.writerow(self.export_item_as_row(p))
