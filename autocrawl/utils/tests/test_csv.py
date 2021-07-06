import codecs
import csv
import io
import json
from typing import Dict, List

import pytest  # NOQA
from pkg_resources import resource_stream, resource_string

from ..csv_export import CSVExporter, CSVStatsCollector, FieldOption


class TestCSV:
    @pytest.mark.parametrize(
        "case_name, field_options, array_limits",
        [
            ("articles_xod_test", {}, {}),
            (
                "items_recursive_test",
                {
                    "named_array_field": {
                        "named": True,
                        "name": "name",
                        "grouped": False,
                    }
                },
                {},
            ),
            (
                "products_full_schema_test",
                {
                    "gtin": {"named": True, "name": "type", "grouped": False},
                    "additionalProperty": {
                        "named": True,
                        "name": "name",
                        "grouped": False,
                    },
                    "ratingHistogram": {
                        "named": True,
                        "name": "ratingOption",
                        "grouped": False,
                    },
                },
                {"offers": 1},
            ),
            (
                "products_simple_xod_test",
                {
                    "gtin": {"named": True, "name": "type", "grouped": False},
                    "additionalProperty": {
                        "named": True,
                        "name": "name",
                        "grouped": False,
                    },
                },
                {"offers": 1},
            ),
            (
                "products_xod_test",
                {
                    "gtin": {"named": True, "name": "type", "grouped": False},
                    "additionalProperty": {
                        "named": True,
                        "name": "name",
                        "grouped": False,
                    },
                },
                {"offers": 1},
            ),
            (
                "products_xod_100_test",
                {
                    "gtin": {"named": True, "name": "type", "grouped": False},
                    "additionalProperty": {
                        "named": True,
                        "name": "name",
                        "grouped": False,
                    },
                },
                {"offers": 1},
            ),
            (
                "items_simple_test",
                {},
                {},
            ),
        ],
    )
    def test_csv_export(self, case_name, field_options, array_limits):
        # Load item list from JSON (simulate API response)
        item_list = json.loads(
            resource_string(__name__, f"assets/{case_name}.json").decode("utf-8")
        )

        # AutoCrawl part
        autocrawl_csv_sc = CSVStatsCollector()
        # Items could be processed in batch or one-by-one through `process_object`
        autocrawl_csv_sc.process_items(item_list)

        # Backend part
        csv_exporter = CSVExporter(
            default_stats=autocrawl_csv_sc.stats,
            field_options=field_options,
            array_limits=array_limits,
        )
        # Compare with pre-processed data
        csv_data = list(
            csv.reader(
                codecs.getreader("utf-8")(
                    resource_stream(__name__, f"assets/{case_name}.csv")
                )
            )
        )
        assert len([csv_exporter._headers] + item_list) == len(csv_data)
        # assert csv_exporter._headers == csv_data[0]
        # Comparing row by row
        for item, row in zip(item_list, csv_data[1:]):
            # Stringify all values because to match string data from csv
            assert [
                str(x) if x is not None else ""
                for x in csv_exporter.export_item_as_row(item)
            ] == row

    @pytest.mark.parametrize(
        "field_options, array_limits, items, expected",
        [
            # Base list
            [
                {},
                {},
                [{"c": {"name": "color", "value": "green"}}],
                [["c->name", "c->value"], ["color", "green"]],
            ],
            # Tuple instead of the list
            [
                {},
                {},
                ({"c": {"name": "color", "value": "green"}},),
                [["c->name", "c->value"], ["color", "green"]],
            ],
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [{"c": {"name": "color", "value": "green"}}],
                [["c->color->value"], ["green"]],
            ],
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [{"c": {"name": "color", "value": "green", "other": "some"}}],
                [["c->color->value", "c->color->other"], ["green", "some"]],
            ],
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [{"c": [{"name": "color", "value": "green", "list": ["el1", "el2"]}]}],
                [
                    ["c->color->value", "c[0]->list[0]", "c[0]->list[1]"],
                    ["green", "el1", "el2"],
                ],
            ],
            # Property as a list
            [
                {},
                {},
                [{"c": [{"name": "color", "value": "green", "list": ["el1", "el2"]}]}],
                [
                    ["c[0]->name", "c[0]->value", "c[0]->list[0]", "c[0]->list[1]"],
                    ["color", "green", "el1", "el2"],
                ],
            ],
            # Property as a tuple
            [
                {},
                {},
                [{"c": ({"name": "color", "value": "green", "list": ["el1", "el2"]},)}],
                [
                    ["c[0]->name", "c[0]->value", "c[0]->list[0]", "c[0]->list[1]"],
                    ["color", "green", "el1", "el2"],
                ],
            ],
            [
                {"c": FieldOption(grouped=True, named=False)},
                {},
                [{"c": {"name": "color", "value": "green"}}],
                [["c"], ["name: color\nvalue: green"]],
            ],
            [
                {"c": FieldOption(grouped=True, named=False)},
                {},
                [{"c": {"name": "color", "value": "green", "other": "some"}}],
                [["c"], ["name: color\nvalue: green\nother: some"]],
            ],
            [
                {},
                {},
                [
                    {
                        "c": [
                            {"name": "color", "value": "green"},
                            {"name": "size", "value": "XL"},
                        ]
                    }
                ],
                [
                    ["c[0]->name", "c[0]->value", "c[1]->name", "c[1]->value"],
                    ["color", "green", "size", "XL"],
                ],
            ],
            [
                {"c": FieldOption(grouped=False, named=True, name="name")},
                {},
                [
                    {
                        "c": [
                            {"name": "color", "value": "green"},
                            {"name": "size", "value": "XL"},
                        ]
                    }
                ],
                [["c->color->value", "c->size->value"], ["green", "XL"]],
            ],
            # <=1 values excluding name
            [
                {"c": FieldOption(grouped=True, named=True, name="name")},
                {},
                [
                    {
                        "c": [
                            {"name": "color", "value": "green"},
                            {"name": "size", "value": "XL"},
                        ]
                    }
                ],
                [["c"], ["color: green\nsize: XL"]],
            ],
            # >1 values excluding name
            [
                {"c": FieldOption(grouped=True, named=True, name="name")},
                {},
                [
                    {
                        "c": [
                            {"name": "color", "value": "green"},
                            {"name": "size", "value": "XL", "available": True},
                        ]
                    }
                ],
                [
                    ["c"],
                    ["- color\\nvalue: green\n- size\\nvalue: XL\\navailable: True"],
                ],
            ],
            [
                {"c": FieldOption(grouped=True, named=False)},
                {},
                [
                    {
                        "c": [
                            {"name": "color", "value": "green"},
                            {"name": "size", "value": "XL"},
                        ]
                    }
                ],
                [["c->name", "c->value"], ["color\nsize", "green\nXL"]],
            ],
            [
                {"c": FieldOption(grouped=True, named=True, name="name")},
                {},
                [
                    {
                        "c": [
                            {"name": "color", "value": "green"},
                            {"name": "size", "value": "XL"},
                        ]
                    }
                ],
                [["c"], ["color: green\nsize: XL"]],
            ],
            [
                {},
                {},
                [{"c": "somevalue"}],
                [["c"], ["somevalue"]],
            ],
            # Subproperty as a list
            [
                {"c": FieldOption(grouped=False, named=True, name="name")},
                {},
                [{"c": {"name": "color", "value": "green"}, "b": [1, 2]}],
                [["c->color->value", "b[0]", "b[1]"], ["green", 1, 2]],
            ],
            # Subproperty as a tuple
            [
                {"c": FieldOption(grouped=False, named=True, name="name")},
                {},
                [{"c": {"name": "color", "value": "green"}, "b": (1, 2)}],
                [["c->color->value", "b[0]", "b[1]"], ["green", 1, 2]],
            ],
            [
                {},
                {},
                [{"c": {"name": "color", "value": "green"}, "b": [1, 2]}],
                [["c->name", "c->value", "b[0]", "b[1]"], ["color", "green", 1, 2]],
            ],
            [
                {"b": FieldOption(named=False, name="name", grouped=False)},
                {},
                [{"b": [1, 2]}],
                [["b[0]", "b[1]"], [1, 2]],
            ],
            [
                {"b": FieldOption(named=False, name="name", grouped=True)},
                {},
                [{"b": [1, 2]}],
                [["b"], ["1\n2"]],
            ],
            [
                {"b": FieldOption(named=False, name="name", grouped=True)},
                {},
                [{"c": {"name": "color", "value": "green"}, "b": [1, 2]}],
                [["c->name", "c->value", "b"], ["color", "green", "1\n2"]],
            ],
            [
                {
                    "b": FieldOption(named=False, name="name", grouped=True),
                    "c": FieldOption(grouped=True, named=False, name="name"),
                },
                {},
                [{"c": {"name": "color", "value": "green"}, "b": [1, 2]}],
                [["c", "b"], ["name: color\nvalue: green", "1\n2"]],
            ],
            [
                {"c": FieldOption(grouped=True, named=False)},
                {},
                [
                    {
                        "c": [
                            {"name": "color", "value": "green"},
                            {"name": "size"},
                            {"name": "material", "value": "cloth"},
                        ]
                    }
                ],
                [["c->name", "c->value"], ["color\nsize\nmaterial", "green\n\ncloth"]],
            ],
        ],
    )
    def test_single_item(
        self,
        field_options: Dict[str, FieldOption],
        array_limits: Dict[str, int],
        items,
        expected,
    ):
        csv_stats_col = CSVStatsCollector(named_columns_limit=50)
        csv_stats_col.process_items(items)

        csv_exporter = CSVExporter(
            default_stats=csv_stats_col.stats,
            field_options=field_options,
            array_limits=array_limits,
        )
        headers = csv_exporter._export_headers_as_row(
            csv_exporter._headers, csv_exporter.headers_renaming
        )
        exp_items = [csv_exporter.export_item_as_row(item) for item in items]
        assert [headers] + exp_items == expected

    @pytest.mark.parametrize(
        "field_options, array_limits, items, expected",
        [
            # Items with all hashable values, no field options
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": "green"}},
                    {"c": {"name": "color", "value": None}},
                ],
                [["c->name", "c->value"], ["color", "green"], ["color", None]],
            ],
            # Items with some non-hashable values, no field options
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": "green"}},
                    {"c": {"name": "color", "value": "blue", "list": [1, 2]}},
                ],
                [
                    ["c->name", "c->value", "c->list[0]", "c->list[1]"],
                    ["color", "green", "", ""],
                    ["color", "blue", 1, 2],
                ],
            ],
            # Don't count None as a type, so don't throw exceptions and process normally
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": [1, 2]}},
                    {"c": {"name": "color", "value": None}},
                ],
                [
                    ["c->name", "c->value[0]", "c->value[1]"],
                    ["color", 1, 2],
                    ["color", "", ""],
                ],
            ],
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": {"some1": "one", "some2": "two"}}},
                    {"c": {"name": "color", "value": None}},
                ],
                [
                    ["c->name", "c->value->some1", "c->value->some2"],
                    ["color", "one", "two"],
                    ["color", "", ""],
                ],
            ],
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": None}},
                    {"c": {"name": "color", "value": {"some1": "one", "some2": "two"}}},
                ],
                [
                    ["c->name", "c->value->some1", "c->value->some2"],
                    ["color", "", ""],
                    ["color", "one", "two"],
                ],
            ],
        ],
    )
    def test_multiple_items(
        self,
        field_options: Dict[str, FieldOption],
        array_limits: Dict[str, int],
        items,
        expected,
    ):
        csv_stats_col = CSVStatsCollector(named_columns_limit=50)
        csv_stats_col.process_items(items)

        csv_exporter = CSVExporter(
            default_stats=csv_stats_col.stats,
            field_options=field_options,
            array_limits=array_limits,
        )
        headers = csv_exporter._export_headers_as_row(
            csv_exporter._headers, csv_exporter.headers_renaming
        )
        exp_items = [csv_exporter.export_item_as_row(item) for item in items]
        assert [headers] + exp_items == expected

    @pytest.mark.parametrize(
        "field_options, array_limits, items, exception_type, exception_pattern",
        [
            # Value changed type from hashable to non-hashable
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": "green"}},
                    {"c": {"name": "color", "value": [1, 2]}},
                ],
                ValueError,
                r"Field \(.*\) was processed as hashable but later got non-hashable value: \(.*\)",
            ],
            [
                {},
                {},
                [
                    {"c": "some"},
                    {"c": {"name": "color", "value": [1, 2]}},
                ],
                ValueError,
                r"Field \(.*\) was processed as hashable but later got non-hashable value: \(.*\)",
            ],
            # Value changed type from non-hashable to hashable
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": [1, 2]}},
                    {"c": {"name": "color", "value": "green"}},
                ],
                ValueError,
                r"Field \(.*\) was processed as non-hashable but later got hashable value: \(.*\)",
            ],
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": [1, 2]}},
                    {"c": "some"},
                ],
                ValueError,
                r"Field \(.*\) was processed as non-hashable but later got hashable value: \(.*\)",
            ],
            # Value changed type from dict to array
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": "blue"}},
                    {"c": [{"name": "color", "value": "green"}]},
                ],
                ValueError,
                r"Field \(.*?\) value changed the type from \"object\" to <class 'list'>.*",
            ],
            # Value changed from array to dict
            [
                {},
                {},
                [
                    {"c": [{"name": "color", "value": "blue"}]},
                    {"c": {"name": "color", "value": "green"}},
                ],
                ValueError,
                r"Field \(.*?\) value changed the type from \"array\" to <class 'dict'>.*",
            ],
        ],
    )
    def test_stat_exceptions(
        self,
        field_options: Dict[str, FieldOption],
        array_limits: Dict[str, int],
        items: List[Dict],
        exception_type: ValueError,
        exception_pattern: str,
    ):
        with pytest.raises(exception_type, match=exception_pattern) as _:  # NOQA
            csv_stats_col = CSVStatsCollector(named_columns_limit=50)
            csv_stats_col.process_items(items)

    @pytest.mark.parametrize(
        "field_options, array_limits, items, exception_type, exception_pattern, named_columns_limit",
        [
            # Arrays of simple elements can't be named
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {"c": [1, 2, 3]},
                ],
                ValueError,
                r"Field \".*?\" doesn't have any properties \(.*?\), so \"named\" option can't be applied\.",
                50,
            ],
            # No `name` field to use
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {"c": {"name1": "color", "value": "blue"}},
                ],
                ValueError,
                r"Field \".*?\" doesn't have name property \".*?\", so \"named\" option can't be applied.",
                50,
            ],
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {"c": [{"name1": "color", "value": "blue"}]},
                ],
                ValueError,
                r"Field \".*?\" doesn't have name property \".*?\", so \"named\" option can't be applied.",
                50,
            ],
            # Non-hashable dict can't be named (no names/values collected)
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {"c": {"name": "color", "value": "blue", "list": [1, 2, 3]}},
                ],
                ValueError,
                r"Field \".*?\" doesn't have any properties \(.*?\), so \"named\" option can't be applied\.",
                50,
            ],
            # No names and values to used because of the limits
            [
                {"c": FieldOption(named=True, name="value", grouped=False)},
                {},
                [
                    {"c": [{"name": "color", "value": "blue"}]},
                    {"c": [{"name": "color", "value": "green"}]},
                    {"c": [{"name": "color", "value": "red"}]},
                ],
                ValueError,
                r"Field \".*?\" values for name property \".*?\" were limited by \"named_columns_limit\" when "
                r"collecting stats, so \"named\" option can't be applied.",
                2,
            ],
            [
                {"c": FieldOption(named=True, name="value", grouped=False)},
                {},
                [
                    {"c": {"name": "color", "value": "blue"}},
                    {"c": {"name": "color", "value": "green"}},
                    {"c": {"name": "color", "value": "red"}},
                ],
                ValueError,
                r"Field \".*?\" values for name property \".*?\" were limited by \"named_columns_limit\" when "
                r"collecting stats, so \"named\" option can't be applied.",
                2,
            ],
        ],
    )
    def test_export_exceptions(
        self,
        field_options: Dict[str, FieldOption],
        array_limits: Dict[str, int],
        items: List[Dict],
        exception_type: ValueError,
        exception_pattern: str,
        named_columns_limit: int,
    ):
        csv_stats_col = CSVStatsCollector(named_columns_limit=named_columns_limit)
        csv_stats_col.process_items(items)
        with pytest.raises(exception_type, match=exception_pattern) as _:  # NOQA
            CSVExporter(
                default_stats=csv_stats_col.stats,
                field_options=field_options,
                array_limits=array_limits,
            )

    def test_buffer_io(self):
        item_list = [
            {"c": {"name": "color", "value": "green"}},
            {"c": {"name": "color", "value": "blue"}},
        ]
        autocrawl_csv_sc = CSVStatsCollector()
        autocrawl_csv_sc.process_items(item_list)
        csv_exporter = CSVExporter(default_stats=autocrawl_csv_sc.stats)
        buffer = io.StringIO()
        csv_exporter.export_csv_full(item_list, buffer)
        assert buffer.getvalue() == "c->name,c->value\r\ncolor,green\r\ncolor,blue\r\n"

    def test_file_io(self, tmpdir):
        item_list = [
            {"c": {"name": "color", "value": "green"}},
            {"c": {"name": "color", "value": "blue"}},
        ]
        autocrawl_csv_sc = CSVStatsCollector()
        autocrawl_csv_sc.process_items(item_list)
        csv_exporter = CSVExporter(default_stats=autocrawl_csv_sc.stats)
        filename = tmpdir.join("custom.csv")
        with open(filename, "w") as f:
            csv_exporter.export_csv_full(item_list, f)
        with open(filename, "r") as f:
            assert f.read() == "c->name,c->value\ncolor,green\ncolor,blue\n"

    def test_path_io(self, tmpdir):
        item_list = [
            {"c": {"name": "color", "value": "green"}},
            {"c": {"name": "color", "value": "blue"}},
        ]
        autocrawl_csv_sc = CSVStatsCollector()
        autocrawl_csv_sc.process_items(item_list)
        csv_exporter = CSVExporter(default_stats=autocrawl_csv_sc.stats)
        filename = tmpdir.join("custom.csv")
        # Test path-like objects
        csv_exporter.export_csv_full(item_list, filename)
        with open(filename, "r") as f:
            assert f.read() == "c->name,c->value\ncolor,green\ncolor,blue\n"
        # Stringify path to make sure exporter works with regular string paths also
        csv_exporter.export_csv_full(item_list, str(filename))
        with open(str(filename), "r") as f:
            assert f.read() == "c->name,c->value\ncolor,green\ncolor,blue\n"
