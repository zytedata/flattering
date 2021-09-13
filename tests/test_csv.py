import codecs
import csv
import io
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest
from pkg_resources import resource_stream, resource_string

from flattering import Exporter, FieldOption, StatsCollector

LOGGER = logging.getLogger(__name__)


class TestCSV:
    @pytest.mark.parametrize(
        "case_name, field_options, export_options",
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
                {"array_limits": {"offers": 1}},
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
                {"array_limits": {"offers": 1}},
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
                {"array_limits": {"offers": 1}},
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
                {"array_limits": {"offers": 1}},
            ),
            (
                "items_simple_test",
                {},
                {},
            ),
        ],
    )
    def test_csv_export(self, case_name, field_options, export_options, tmpdir):
        # Load item list from JSON (simulate API response)
        item_list = json.loads(
            resource_string(__name__, f"assets/{case_name}.json").decode("utf-8")
        )
        # AutoCrawl part
        csv_stats_col = StatsCollector()
        # Collect stats fully (not row by row)
        csv_stats_col.process_items(item_list)
        # Backend part
        csv_exporter = Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
            field_options=field_options,
            **export_options,
        )
        # Get pre-processed data
        base_path = Path(__file__).parent
        with open((base_path / f"assets/{case_name}.csv").resolve(), "r") as f:
            init_csv_data = list(csv.reader(f))
        filename = tmpdir.join(f"{case_name}.csv")
        # Export data
        csv_exporter.export_csv_full(item_list, filename)
        # Get exported data
        with open(filename, "r") as f:
            test_csv_data = list(csv.reader(f))
        # Comparing full files without headers (different separators)
        assert init_csv_data[1:] == test_csv_data[1:]

    @pytest.mark.parametrize(
        "case_name, field_options, export_options",
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
                {"array_limits": {"offers": 1}},
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
                {"array_limits": {"offers": 1}},
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
                {"array_limits": {"offers": 1}},
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
                {"array_limits": {"offers": 1}},
            ),
            (
                "items_simple_test",
                {},
                {},
            ),
        ],
    )
    def test_csv_export_one_by_one(self, case_name, field_options, export_options):
        # Load item list from JSON (simulate API response)
        item_list = json.loads(
            resource_string(__name__, f"assets/{case_name}.json").decode("utf-8")
        )
        # AutoCrawl part
        csv_stats_col = StatsCollector()
        # Collect stats row by row
        [csv_stats_col.process_object(x) for x in item_list]
        # Backend part
        csv_exporter = Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
            field_options=field_options,
            **export_options,
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
        # Export and compare row by row
        for item, row in zip(item_list, csv_data[1:]):
            # Stringify all values because to match string data from csv
            assert [
                str(x) if x is not None else ""
                for x in csv_exporter.export_item_as_row(item)
            ] == row

    @pytest.mark.parametrize(
        "field_options, export_options, items, expected",
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
                [["c->color->value", "b[0]", "b[1]"], ["green", "1", "2"]],
            ],
            # Subproperty as a tuple
            [
                {"c": FieldOption(grouped=False, named=True, name="name")},
                {},
                [{"c": {"name": "color", "value": "green"}, "b": (1, 2)}],
                [["c->color->value", "b[0]", "b[1]"], ["green", "1", "2"]],
            ],
            [
                {},
                {},
                [{"c": {"name": "color", "value": "green"}, "b": [1, 2]}],
                [["c->name", "c->value", "b[0]", "b[1]"], ["color", "green", "1", "2"]],
            ],
            [
                {"b": FieldOption(named=False, name="name", grouped=False)},
                {},
                [{"b": [1, 2]}],
                [["b[0]", "b[1]"], ["1", "2"]],
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
            # Test other hashable types
            [
                {"b": FieldOption(named=False, grouped=False)},
                {},
                [{"b": datetime.fromisoformat("2011-11-04T00:05:23")}],
                [["b"], [str(datetime.fromisoformat("2011-11-04T00:05:23"))]],
            ],
            # Test nested arrays
            [
                {},
                {},
                [{"c": [["some_value"]]}],
                [["c[0][0]"], ["some_value"]],
            ],
            [
                {},
                {},
                [{"c": [[["some_value"]]]}],
                [["c[0][0][0]"], ["some_value"]],
            ],
            # Headers order (check non-existing headers also)
            [
                {},
                {"headers_order": ["another_name", "name", "non-existing-header"]},
                [{"name": "value", "another_name": "another_value"}],
                [["another_name", "name"], ["another_value", "value"]],
            ],
            # Headers filters (check non-existing headers also)
            [
                {},
                {"headers_filters": [r"name", "non-existing-header"]},
                [{"name": "value", "another_name": "another_value"}],
                [["another_name"], ["another_value"]],
            ],
            [
                {},
                {"headers_filters": [r".*name", "non-existing-header"]},
                [{"name": "value", "another_name": "another_value"}],
                [[], []],
            ],
            [
                {},
                {},
                [{"a": [{"b": [1, 2, 3]}]}],
                [["a[0]->b[0]", "a[0]->b[1]", "a[0]->b[2]"], ["1", "2", "3"]],
            ],
            [
                {},
                {},
                [
                    {
                        "a": {
                            "nested_a": [
                                [
                                    {
                                        "2x_nested_a": {
                                            "3x_nested_a": [
                                                {
                                                    "name": "parameter1",
                                                    "value": "value1",
                                                },
                                                {
                                                    "name": "parameter2",
                                                    "value": "value2",
                                                },
                                            ]
                                        }
                                    },
                                ]
                            ],
                            "second_nested_a": "some_value",
                        }
                    }
                ],
                [
                    [
                        "a->nested_a[0][0]->2x_nested_a->3x_nested_a[0]->name",
                        "a->nested_a[0][0]->2x_nested_a->3x_nested_a[0]->value",
                        "a->nested_a[0][0]->2x_nested_a->3x_nested_a[1]->name",
                        "a->nested_a[0][0]->2x_nested_a->3x_nested_a[1]->value",
                        "a->second_nested_a",
                    ],
                    ["parameter1", "value1", "parameter2", "value2", "some_value"],
                ],
            ],
            [
                {
                    "a->nested_a[0][0]->2x_nested_a->3x_nested_a": {
                        "named": True,
                        "name": "name",
                        "grouped": True,
                    }
                },
                {},
                [
                    {
                        "a": {
                            "nested_a": [
                                [
                                    {
                                        "2x_nested_a": {
                                            "3x_nested_a": [
                                                {
                                                    "name": "parameter1",
                                                    "value": "value1",
                                                },
                                                {
                                                    "name": "parameter2",
                                                    "value": "value2",
                                                },
                                            ]
                                        }
                                    },
                                ]
                            ],
                            "second_nested_a": "some_value",
                        }
                    }
                ],
                [
                    [
                        "a->nested_a[0][0]->2x_nested_a->3x_nested_a",
                        "a->second_nested_a",
                    ],
                    ["parameter1: value1\nparameter2: value2", "some_value"],
                ],
            ],
            # Test different symbols (including commas) in the description
            [
                {},
                {},
                [
                    {
                        "description": "їжачок біжав по лісу й грався з ягодами аґруса, поспішаючи до дому",
                        "name": "Якесь ім'я",
                    }
                ],
                [
                    ["description", "name"],
                    [
                        "їжачок біжав по лісу й грався з ягодами аґруса, поспішаючи до дому",
                        "Якесь ім'я",
                    ],
                ],
            ],
            [
                {},
                {},
                [
                    {
                        "description,,,te;xt": "їжачок біжав по лісу й грався з ягодами аґруса, поспішаючи до дому",
                        "name": "Якесь ім'я",
                    }
                ],
                [
                    ["description,,,te;xt", "name"],
                    [
                        "їжачок біжав по лісу й грався з ягодами аґруса, поспішаючи до дому",
                        "Якесь ім'я",
                    ],
                ],
            ],
            [
                {},
                {},
                [{"description": "刺猬穿过树林，玩弄醋栗，匆匆回家", "name": "一些名字"}],
                [["description", "name"], ["刺猬穿过树林，玩弄醋栗，匆匆回家", "一些名字"]],
            ],
        ],
    )
    def test_single_item(
        self,
        field_options: Dict[str, FieldOption],
        export_options: Dict,
        items,
        expected,
    ):
        csv_stats_col = StatsCollector(named_columns_limit=50)
        csv_stats_col.process_items(items)

        csv_exporter = Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
            field_options=field_options,
            **export_options,
        )
        exp_items = [csv_exporter.export_item_as_row(item) for item in items]
        assert [csv_exporter._get_renamed_headers()] + exp_items == expected

    @pytest.mark.parametrize(
        "field_options, export_options, items, expected",
        [
            # Items with all hashable values, no field options
            [
                {},
                {},
                [
                    {"c": {"name": "color", "value": "green"}},
                    {"c": {"name": "color", "value": None}},
                ],
                [["c->name", "c->value"], ["color", "green"], ["color", ""]],
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
                    ["color", "blue", "1", "2"],
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
                    ["color", "1", "2"],
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
            # Field options for nested fields
            [
                {"c->parameter1": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {
                        "c": {
                            "parameter1": [
                                {"name": "size", "value": "XL"},
                                {"name": "color", "value": "blue"},
                            ],
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": [
                                {"name": "size", "value": "L"},
                                {"name": "color", "value": "green"},
                            ],
                            "parameter2": "another some",
                        }
                    },
                ],
                [
                    [
                        "c->parameter1->size->value",
                        "c->parameter1->color->value",
                        "c->parameter2",
                    ],
                    ["XL", "blue", "some"],
                    ["L", "green", "another some"],
                ],
            ],
            [
                {"c->parameter1": FieldOption(named=False, name="name", grouped=True)},
                {},
                [
                    {
                        "c": {
                            "parameter1": [
                                {"name": "size", "value": "XL"},
                                {"name": "color", "value": "blue"},
                            ],
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": [
                                {"name": "size", "value": "L"},
                                {"name": "color", "value": "green"},
                            ],
                            "parameter2": "another some",
                        }
                    },
                ],
                [
                    ["c->parameter1->name", "c->parameter1->value", "c->parameter2"],
                    ["size\ncolor", "XL\nblue", "some"],
                    ["size\ncolor", "L\ngreen", "another some"],
                ],
            ],
            [
                {"c->parameter1": FieldOption(named=True, name="name", grouped=True)},
                {},
                [
                    {
                        "c": {
                            "parameter1": [
                                {"name": "size", "value": "XL"},
                                {"name": "color", "value": "blue"},
                            ],
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": [
                                {"name": "size", "value": "L"},
                                {"name": "color", "value": "green"},
                            ],
                            "parameter2": "another some",
                        }
                    },
                ],
                [
                    ["c->parameter1", "c->parameter2"],
                    ["size: XL\ncolor: blue", "some"],
                    ["size: L\ncolor: green", "another some"],
                ],
            ],
            # Double nested
            [
                {
                    "c->nested_c->double_nested_c": FieldOption(
                        named=False, name="name", grouped=True
                    )
                },
                {},
                [
                    {
                        "c": {
                            "nested_c": {"double_nested_c": [1, 2, 3]},
                            "some_field_1": "some_value_1",
                        },
                        "b": "some_other_value_1",
                    },
                    {
                        "c": {
                            "nested_c": {"double_nested_c": [4, 5, 6, 7]},
                            "some_field_2": "some_value_2",
                        },
                        "b": "some_other_value_2",
                    },
                ],
                [
                    [
                        "c->nested_c->double_nested_c",
                        "c->some_field_1",
                        "b",
                        "c->some_field_2",
                    ],
                    ["1\n2\n3", "some_value_1", "some_other_value_1", ""],
                    ["4\n5\n6\n7", "", "some_other_value_2", "some_value_2"],
                ],
            ],
            [
                {
                    "c->nested_c->double_nested_c": FieldOption(
                        named=True, name="name", grouped=True
                    )
                },
                {},
                [
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    {"name": "size", "value": "L"},
                                    {"name": "color", "value": "blue"},
                                ]
                            },
                            "some_field_1": "some_value_1",
                        },
                        "b": "some_other_value_1",
                    },
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    {"name": "size", "value": "XL"},
                                    {"name": "color", "value": "green"},
                                ]
                            },
                            "some_field_2": "some_value_2",
                        },
                        "b": "some_other_value_2",
                    },
                ],
                [
                    [
                        "c->nested_c->double_nested_c",
                        "c->some_field_1",
                        "b",
                        "c->some_field_2",
                    ],
                    ["size: L\ncolor: blue", "some_value_1", "some_other_value_1", ""],
                    [
                        "size: XL\ncolor: green",
                        "",
                        "some_other_value_2",
                        "some_value_2",
                    ],
                ],
            ],
            [
                {
                    "c->nested_c->double_nested_c": FieldOption(
                        named=True, name="name", grouped=False
                    )
                },
                {},
                [
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    {"name": "size", "value": "L"},
                                    {"name": "color", "value": "blue"},
                                ]
                            },
                            "some_field_1": "some_value_1",
                        },
                        "b": "some_other_value_1",
                    },
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    {"name": "size", "value": "XL"},
                                    {"name": "color", "value": "green"},
                                ]
                            },
                            "some_field_2": "some_value_2",
                        },
                        "b": "some_other_value_2",
                    },
                ],
                [
                    [
                        "c->nested_c->double_nested_c->size->value",
                        "c->nested_c->double_nested_c->color->value",
                        "c->some_field_1",
                        "b",
                        "c->some_field_2",
                    ],
                    ["L", "blue", "some_value_1", "some_other_value_1", ""],
                    ["XL", "green", "", "some_other_value_2", "some_value_2"],
                ],
            ],
            [
                {
                    "c->nested_c->double_nested_c": FieldOption(
                        named=False, name="name", grouped=True
                    )
                },
                {},
                [
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    {"name": "size", "value": "L"},
                                    {"name": "color", "value": "blue"},
                                ]
                            },
                            "some_field_1": "some_value_1",
                        },
                        "b": "some_other_value_1",
                    },
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    {"name": "size", "value": "XL"},
                                    {"name": "color", "value": "green"},
                                ]
                            },
                            "some_field_2": "some_value_2",
                        },
                        "b": "some_other_value_2",
                    },
                ],
                [
                    [
                        "c->nested_c->double_nested_c->name",
                        "c->nested_c->double_nested_c->value",
                        "c->some_field_1",
                        "b",
                        "c->some_field_2",
                    ],
                    [
                        "size\ncolor",
                        "L\nblue",
                        "some_value_1",
                        "some_other_value_1",
                        "",
                    ],
                    [
                        "size\ncolor",
                        "XL\ngreen",
                        "",
                        "some_other_value_2",
                        "some_value_2",
                    ],
                ],
            ],
            # Triple nested
            [
                {
                    "c->nested_c->double_nested_c[0]": FieldOption(
                        named=False, name="name", grouped=True
                    )
                },
                {},
                [
                    {
                        "c": {
                            "nested_c": {"double_nested_c": [[1, 2, 3]]},
                            "some_field_1": "some_value_1",
                        },
                        "b": "some_other_value_1",
                    },
                    {
                        "c": {
                            "nested_c": {"double_nested_c": [[4, 5, 6, 7]]},
                            "some_field_2": "some_value_2",
                        },
                        "b": "some_other_value_2",
                    },
                ],
                [
                    [
                        "c->nested_c->double_nested_c[0]",
                        "c->some_field_1",
                        "b",
                        "c->some_field_2",
                    ],
                    ["1\n2\n3", "some_value_1", "some_other_value_1", ""],
                    ["4\n5\n6\n7", "", "some_other_value_2", "some_value_2"],
                ],
            ],
            [
                {
                    "c->nested_c->double_nested_c[0]": FieldOption(
                        named=True, name="name", grouped=True
                    )
                },
                {},
                [
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    [
                                        {"name": "size", "value": "L"},
                                        {"name": "color", "value": "blue"},
                                    ]
                                ]
                            },
                            "some_field_1": "some_value_1",
                        },
                        "b": "some_other_value_1",
                    },
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    [
                                        {"name": "size", "value": "XL"},
                                        {"name": "color", "value": "green"},
                                    ]
                                ]
                            },
                            "some_field_2": "some_value_2",
                        },
                        "b": "some_other_value_2",
                    },
                ],
                [
                    [
                        "c->nested_c->double_nested_c[0]",
                        "c->some_field_1",
                        "b",
                        "c->some_field_2",
                    ],
                    ["size: L\ncolor: blue", "some_value_1", "some_other_value_1", ""],
                    [
                        "size: XL\ncolor: green",
                        "",
                        "some_other_value_2",
                        "some_value_2",
                    ],
                ],
            ],
            [
                {
                    "c->nested_c->double_nested_c[0]": FieldOption(
                        named=True, name="name", grouped=False
                    )
                },
                {},
                [
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    [
                                        {"name": "size", "value": "L"},
                                        {"name": "color", "value": "blue"},
                                    ]
                                ]
                            },
                            "some_field_1": "some_value_1",
                        },
                        "b": "some_other_value_1",
                    },
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    [
                                        {"name": "size", "value": "XL"},
                                        {"name": "color", "value": "green"},
                                    ]
                                ]
                            },
                            "some_field_2": "some_value_2",
                        },
                        "b": "some_other_value_2",
                    },
                ],
                [
                    [
                        "c->nested_c->double_nested_c[0]->size->value",
                        "c->nested_c->double_nested_c[0]->color->value",
                        "c->some_field_1",
                        "b",
                        "c->some_field_2",
                    ],
                    ["L", "blue", "some_value_1", "some_other_value_1", ""],
                    ["XL", "green", "", "some_other_value_2", "some_value_2"],
                ],
            ],
            [
                {
                    "c->nested_c->double_nested_c[0]": FieldOption(
                        named=False, name="name", grouped=True
                    )
                },
                {},
                [
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    [
                                        {"name": "size", "value": "L"},
                                        {"name": "color", "value": "blue"},
                                    ]
                                ]
                            },
                            "some_field_1": "some_value_1",
                        },
                        "b": "some_other_value_1",
                    },
                    {
                        "c": {
                            "nested_c": {
                                "double_nested_c": [
                                    [
                                        {"name": "size", "value": "XL"},
                                        {"name": "color", "value": "green"},
                                    ]
                                ]
                            },
                            "some_field_2": "some_value_2",
                        },
                        "b": "some_other_value_2",
                    },
                ],
                [
                    [
                        "c->nested_c->double_nested_c[0]->name",
                        "c->nested_c->double_nested_c[0]->value",
                        "c->some_field_1",
                        "b",
                        "c->some_field_2",
                    ],
                    [
                        "size\ncolor",
                        "L\nblue",
                        "some_value_1",
                        "some_other_value_1",
                        "",
                    ],
                    [
                        "size\ncolor",
                        "XL\ngreen",
                        "",
                        "some_other_value_2",
                        "some_value_2",
                    ],
                ],
            ],
        ],
    )
    def test_multiple_items(
        self,
        field_options: Dict[str, FieldOption],
        export_options: Dict,
        items,
        expected,
    ):
        csv_stats_col = StatsCollector(named_columns_limit=50)
        csv_stats_col.process_items(items)

        csv_exporter = Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
            field_options=field_options,
            **export_options,
        )
        exp_items = [csv_exporter.export_item_as_row(item) for item in items]
        assert [csv_exporter._get_renamed_headers()] + exp_items == expected

    @pytest.mark.parametrize(
        "field_options, export_options, items, expected",
        [
            # Mixed types, should be stringified
            [
                {},
                {},
                [
                    {"c": [[1, 2], "text", (5, 6)]},
                    {"c": [[1, 2], (5, 6), 100, {"test": "some"}]},
                ],
                [
                    ["c[0]", "c[1]", "c[2]", "c[3]"],
                    ["[1, 2]", "text", "(5, 6)", ""],
                    ["[1, 2]", "(5, 6)", "100", "{'test': 'some'}"],
                ],
            ],
            [
                {},
                {},
                [
                    {"c": 123},
                    {"c": [[1, 2], "text", (5, 6)]},
                    {"c": [[1, 2], (5, 6), 100, {"test": "some"}]},
                ],
                [
                    ["c"],
                    ["123"],
                    ["[[1, 2], 'text', (5, 6)]"],
                    ["[[1, 2], (5, 6), 100, {'test': 'some'}]"],
                ],
            ],
            [
                {},
                {},
                [
                    {"c": [[1, 2], "text", (5, 6)]},
                    {"c": [[1, 2], (5, 6), 100, {"test": "some"}]},
                    {"c": 123},
                ],
                [
                    ["c"],
                    ["[[1, 2], 'text', (5, 6)]"],
                    ["[[1, 2], (5, 6), 100, {'test': 'some'}]"],
                    ["123"],
                ],
            ],
            # From array of dicts to dict
            [
                {},
                {},
                [
                    {
                        "c": [
                            {"name": "size", "value": [123]},
                            {"name": "color", "value": "blue"},
                        ]
                    },
                    {
                        "c": [
                            {"name": "size", "value": "L"},
                            {"name": "color", "value": "green"},
                        ]
                    },
                    {"c": {"name": "color"}},
                    {"c": {"name": "width"}},
                ],
                [
                    ["c"],
                    [
                        "[{'name': 'size', 'value': [123]}, {'name': 'color', 'value': 'blue'}]"
                    ],
                    [
                        "[{'name': 'size', 'value': 'L'}, {'name': 'color', 'value': 'green'}]"
                    ],
                    ["{'name': 'color'}"],
                    ["{'name': 'width'}"],
                ],
            ],
            # From dict to array of dicts
            [
                {},
                {},
                [
                    {"c": {"name": "color"}},
                    {"c": {"name": "width"}},
                    {
                        "c": [
                            {"name": "size", "value": [123]},
                            {"name": "color", "value": "blue"},
                        ]
                    },
                    {
                        "c": [
                            {"name": "size", "value": "L"},
                            {"name": "color", "value": "green"},
                        ]
                    },
                ],
                [
                    ["c"],
                    ["{'name': 'color'}"],
                    ["{'name': 'width'}"],
                    [
                        "[{'name': 'size', 'value': [123]}, {'name': 'color', 'value': 'blue'}]"
                    ],
                    [
                        "[{'name': 'size', 'value': 'L'}, {'name': 'color', 'value': 'green'}]"
                    ],
                ],
            ],
            # From hashable array to non-hashable array
            [
                {},
                {},
                [
                    {"c": [1, "text", 3]},
                    {"c": [[1, 2], "another_text", {"test": "some"}]},
                ],
                [
                    ["c[0]", "c[1]", "c[2]"],
                    ["1", "text", "3"],
                    ["[1, 2]", "another_text", "{'test': 'some'}"],
                ],
            ],
            # From non-hashable array to hashable array
            [
                {},
                {},
                [
                    {"c": [[1, 2], "another_text", {"test": "some"}]},
                    {"c": [1, "text", 3]},
                ],
                [
                    ["c[0]", "c[1]", "c[2]"],
                    ["[1, 2]", "another_text", "{'test': 'some'}"],
                    ["1", "text", "3"],
                ],
            ],
            # From hashable values to non-hashable
            [
                {},
                {},
                [
                    {"c": 123, "b": "text"},
                    {"c": [456], "b": 321},
                ],
                [["c", "b"], ["123", "text"], ["[456]", "321"]],
            ],
            [
                {},
                {},
                [
                    {"c": 123, "b": "text"},
                    {"c": [456], "b": 321},
                    {"c": 123, "b": "text"},
                ],
                [["c", "b"], ["123", "text"], ["[456]", "321"], ["123", "text"]],
            ],
            [
                {},
                {},
                [
                    {"c": {"name": "size", "value": "XL"}},
                    {"c": {"name": "size", "value": [1, 2, 3]}},
                    {"c": {"name": "size", "value": [1, 2, 3]}},
                ],
                [
                    ["c->name", "c->value"],
                    ["size", "XL"],
                    ["size", "[1, 2, 3]"],
                    ["size", "[1, 2, 3]"],
                ],
            ],
            # Nested
            [
                {},
                {},
                [
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": "some_value"},
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": [1, 2, 3]},
                            "parameter2": "some",
                        }
                    },
                ],
                [
                    ["c->parameter2", "c->parameter1->name", "c->parameter1->value"],
                    ["some", "size", "some_value"],
                    ["some", "size", "[1, 2, 3]"],
                ],
            ],
            [
                {},
                {},
                [
                    {
                        "c": [
                            {
                                "parameter1": {"name": "size", "value": "some_value"},
                                "parameter2": "some",
                            }
                        ]
                    },
                    {
                        "c": [
                            {
                                "parameter1": {"name": "size", "value": [1, 2, 3]},
                                "parameter2": "some",
                            }
                        ]
                    },
                ],
                [
                    [
                        "c[0]->parameter2",
                        "c[0]->parameter1->name",
                        "c[0]->parameter1->value",
                    ],
                    ["some", "size", "some_value"],
                    ["some", "size", "[1, 2, 3]"],
                ],
            ],
            # From non-hashable values to hashable
            [
                {},
                {},
                [
                    {"c": [456], "b": 321},
                    {"c": 123, "b": "text"},
                ],
                [["c", "b"], ["[456]", "321"], ["123", "text"]],
            ],
            [
                {},
                {},
                [
                    {"c": [456], "b": 321},
                    {"c": 123, "b": "text"},
                    {"c": [456], "b": 321},
                ],
                [["c", "b"], ["[456]", "321"], ["123", "text"], ["[456]", "321"]],
            ],
            [
                {},
                {},
                [
                    {"c": {"name": "size", "value": [1, 2, 3]}},
                    {"c": {"name": "size", "value": "XL"}},
                    {"c": {"name": "size", "value": [1, 2, 3]}},
                ],
                [
                    ["c->name", "c->value"],
                    ["size", "[1, 2, 3]"],
                    ["size", "XL"],
                    ["size", "[1, 2, 3]"],
                ],
            ],
            # Nested
            [
                {},
                {},
                [
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": [1, 2, 3]},
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": "some_value"},
                            "parameter2": "some",
                        }
                    },
                ],
                [
                    ["c->parameter1->name", "c->parameter1->value", "c->parameter2"],
                    ["size", "[1, 2, 3]", "some"],
                    ["size", "some_value", "some"],
                ],
            ],
            [
                {},
                {},
                [
                    {
                        "c": [
                            {
                                "parameter1": {"name": "size", "value": [1, 2, 3]},
                                "parameter2": "some",
                            }
                        ]
                    },
                    {
                        "c": [
                            {
                                "parameter1": {"name": "size", "value": "some_value"},
                                "parameter2": "some",
                            }
                        ]
                    },
                ],
                [
                    [
                        "c[0]->parameter2",
                        "c[0]->parameter1->name",
                        "c[0]->parameter1->value",
                    ],
                    ["some", "size", "[1, 2, 3]"],
                    ["some", "size", "some_value"],
                ],
            ],
            # Unsupported type
            [
                {},
                {},
                [
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": [1, 2, 3]},
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": "some_value"},
                            "parameter2": StatsCollector(),
                        }
                    },
                ],
                [
                    ["c->parameter1->name", "c->parameter1->value", "c->parameter2"],
                    ["size", "[1, 2, 3]", "some"],
                    [
                        "size",
                        "some_value",
                        "StatsCollector"
                        "(named_columns_limit=20, cut_separator='->', _stats={}, _invalid_properties={})",
                    ],
                ],
            ],
            [
                {},
                {},
                [
                    {
                        "c": {
                            "parameter1": {
                                "name": "size",
                                "value": StatsCollector(),
                            },
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": "some_value"},
                            "parameter2": "some",
                        }
                    },
                ],
                [
                    ["c->parameter1->name", "c->parameter1->value", "c->parameter2"],
                    [
                        "size",
                        "StatsCollector"
                        "(named_columns_limit=20, cut_separator='->', _stats={}, _invalid_properties={})",
                        "some",
                    ],
                    ["size", "some_value", "some"],
                ],
            ],
            # Mixed types, should be skipped
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": [[1, 2], "text", (5, 6)]},
                    {"c": [[1, 2], (5, 6), 100, {"test": "some"}]},
                ],
                [
                    [],
                    [],
                    [],
                ],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": 123},
                    {"c": [[1, 2], "text", (5, 6)]},
                    {"c": [[1, 2], (5, 6), 100, {"test": "some"}]},
                ],
                [[], [], [], []],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": [[1, 2], "text", (5, 6)]},
                    {"c": [[1, 2], (5, 6), 100, {"test": "some"}]},
                    {"c": 123},
                ],
                [[], [], [], []],
            ],
            # From array of dicts to dict
            [
                {},
                {"stringify_invalid": False},
                [
                    {
                        "c": [
                            {"name": "size", "value": [123]},
                            {"name": "color", "value": "blue"},
                        ]
                    },
                    {
                        "c": [
                            {"name": "size", "value": "L"},
                            {"name": "color", "value": "green"},
                        ]
                    },
                    {"c": {"name": "color"}},
                    {"c": {"name": "width"}},
                ],
                [[], [], [], [], []],
            ],
            # From dict to array of dicts
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": {"name": "color"}},
                    {"c": {"name": "width"}},
                    {
                        "c": [
                            {"name": "size", "value": [123]},
                            {"name": "color", "value": "blue"},
                        ]
                    },
                    {
                        "c": [
                            {"name": "size", "value": "L"},
                            {"name": "color", "value": "green"},
                        ]
                    },
                ],
                [[], [], [], [], []],
            ],
            # From hashable array to non-hashable array
            # Non-stable fields should be skipped
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": [1, "text", 3]},
                    {"c": [[1, 2], "another_text", {"test": "some"}]},
                ],
                [[], [], []],
            ],
            # From non-hashable array to hashable array
            # Non-stable fields should be skipped
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": [[1, 2], "another_text", {"test": "some"}]},
                    {"c": [1, "text", 3]},
                ],
                [[], [], []],
            ],
            # From hashable values to non-hashable
            # Non-stable fields should be skipped
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": 123, "b": "text"},
                    {"c": [456], "b": 321},
                ],
                [["b"], ["text"], ["321"]],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": 123, "b": "text"},
                    {"c": [456], "b": 321},
                    {"c": 123, "b": "text"},
                ],
                [["b"], ["text"], ["321"], ["text"]],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": {"name": "size", "value": "XL"}},
                    {"c": {"name": "size", "value": [1, 2, 3]}},
                    {"c": {"name": "size", "value": [1, 2, 3]}},
                ],
                [["c->name"], ["size"], ["size"], ["size"]],
            ],
            # Nested
            [
                {},
                {"stringify_invalid": False},
                [
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": "some_value"},
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": [1, 2, 3]},
                            "parameter2": "some",
                        }
                    },
                ],
                [
                    ["c->parameter2", "c->parameter1->name"],
                    ["some", "size"],
                    ["some", "size"],
                ],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {
                        "c": [
                            {
                                "parameter1": {"name": "size", "value": "some_value"},
                                "parameter2": "some",
                            }
                        ]
                    },
                    {
                        "c": [
                            {
                                "parameter1": {"name": "size", "value": [1, 2, 3]},
                                "parameter2": "some",
                            }
                        ]
                    },
                ],
                [
                    ["c[0]->parameter2", "c[0]->parameter1->name"],
                    ["some", "size"],
                    ["some", "size"],
                ],
            ],
            # From non-hashable values to hashable
            # Non-stable fields should be skipped
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": [456], "b": 321},
                    {"c": 123, "b": "text"},
                ],
                [["b"], ["321"], ["text"]],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": [456], "b": 321},
                    {"c": 123, "b": "text"},
                    {"c": [456], "b": 321},
                ],
                [["b"], ["321"], ["text"], ["321"]],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {"c": {"name": "size", "value": [1, 2, 3]}},
                    {"c": {"name": "size", "value": "XL"}},
                    {"c": {"name": "size", "value": [1, 2, 3]}},
                ],
                [["c->name"], ["size"], ["size"], ["size"]],
            ],
            # Nested
            [
                {},
                {"stringify_invalid": False},
                [
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": [1, 2, 3]},
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": "some_value"},
                            "parameter2": "some",
                        }
                    },
                ],
                [
                    ["c->parameter1->name", "c->parameter2"],
                    ["size", "some"],
                    ["size", "some"],
                ],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {
                        "c": [
                            {
                                "parameter1": {"name": "size", "value": [1, 2, 3]},
                                "parameter2": "some",
                            }
                        ]
                    },
                    {
                        "c": [
                            {
                                "parameter1": {"name": "size", "value": "some_value"},
                                "parameter2": "some",
                            }
                        ]
                    },
                ],
                [
                    ["c[0]->parameter2", "c[0]->parameter1->name"],
                    ["some", "size"],
                    ["some", "size"],
                ],
            ],
            # Unsupported type
            [
                {},
                {"stringify_invalid": False},
                [
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": [1, 2, 3]},
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": "some_value"},
                            "parameter2": StatsCollector(),
                        }
                    },
                ],
                [["c->parameter1->name"], ["size"], ["size"]],
            ],
            [
                {},
                {"stringify_invalid": False},
                [
                    {
                        "c": {
                            "parameter1": {
                                "name": "size",
                                "value": StatsCollector(),
                            },
                            "parameter2": "some",
                        }
                    },
                    {
                        "c": {
                            "parameter1": {"name": "size", "value": "some_value"},
                            "parameter2": "some",
                        }
                    },
                ],
                [
                    ["c->parameter1->name", "c->parameter2"],
                    ["size", "some"],
                    ["size", "some"],
                ],
            ],
        ],
    )
    def test_multiple_invalid_items(
        self,
        field_options: Dict[str, FieldOption],
        export_options: Dict,
        items,
        expected,
    ):
        csv_stats_col = StatsCollector(named_columns_limit=50)
        csv_stats_col.process_items(items)

        csv_exporter = Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
            field_options=field_options,
            **export_options,
        )
        exp_items = [csv_exporter.export_item_as_row(item) for item in items]
        assert [csv_exporter._get_renamed_headers()] + exp_items == expected

    @pytest.mark.parametrize(
        "items, exception_type, exception_pattern",
        [
            # Initiasl items are not list
            [
                {"some": "data"},
                TypeError,
                r"Initial items data must be array, not <class 'dict'>.",
            ],
            # Mixed initial items types
            [
                [{"some": "data"}, [1, 2, 3]],
                TypeError,
                r"All elements of the array must be of the same type instead of "
                r"\{(?:<class 'dict'>|, |<class 'list'>)+\}\.",
            ],
            # Array of arrays
            [
                [[1, 2, 3]],
                TypeError,
                r"Items must be dicts \(not arrays\) to be supported.",
            ],
            # Unsupported types
            [
                [123],
                TypeError,
                r"Unsupported item type \(<class 'int'>\).",
            ],
            # Arrays of arrays
            [
                [
                    [{"c": "value"}],
                ],
                TypeError,
                r"Items must be dicts \(not arrays\) to be supported.",
            ],
            [
                [
                    [[["value"]]],
                ],
                TypeError,
                r"Items must be dicts \(not arrays\) to be supported.",
            ],
        ],
    )
    def test_stats_exceptions(
        self,
        items: List[Dict],
        exception_type: TypeError,
        exception_pattern: str,
    ):
        with pytest.raises(exception_type, match=exception_pattern) as _:  # NOQA
            csv_stats_col = StatsCollector()
            csv_stats_col.process_items(items)

    @pytest.mark.parametrize(
        "items, warning_pattern",
        [
            # No items provided
            [
                [],
                r".*No items provided.",
            ],
            # Value changed type from hashable to non-hashable
            [
                [
                    {"c": {"name": "color", "value": "green"}},
                    {"c": {"name": "color", "value": [1, 2]}},
                ],
                r".*Field \(.*\) was processed as hashable but later got non-hashable value: \(.*\)",
            ],
            [
                [
                    {"c": "some"},
                    {"c": {"name": "color", "value": [1, 2]}},
                ],
                r".*Field \(.*\) was processed as hashable but later got non-hashable value: \(.*\)",
            ],
            # Value changed type from non-hashable to hashable
            [
                [
                    {"c": {"name": "color", "value": [1, 2]}},
                    {"c": {"name": "color", "value": "green"}},
                ],
                r".*Field \(.*\) was processed as non-hashable but later got hashable value: \(.*\)",
            ],
            [
                [
                    {"c": {"name": "color", "value": [1, 2]}},
                    {"c": "some"},
                ],
                r".*Field \(.*\) was processed as non-hashable but later got hashable value: \(.*\)",
            ],
            # Value changed type from dict to array
            [
                [
                    {"c": {"name": "color", "value": "blue"}},
                    {"c": [{"name": "color", "value": "green"}]},
                ],
                r".*Field \(.*?\) value changed the type from \"object\" to <class 'list'>.*",
            ],
            # Value changed from array to dict
            [
                [
                    {"c": [{"name": "color", "value": "blue"}]},
                    {"c": {"name": "color", "value": "green"}},
                ],
                r".*Field \(.*?\) value changed the type from \"array\" to <class 'dict'>.*",
            ],
        ],
    )
    def test_stats_warnings(
        self,
        caplog,
        items: List[Dict],
        warning_pattern: str,
    ):
        with caplog.at_level(logging.WARNING):
            csv_stats_col = StatsCollector(named_columns_limit=50)
            csv_stats_col.process_items(items)
        assert re.match(warning_pattern, caplog.text)

    @pytest.mark.parametrize(
        "field_options, export_options, items, warning_pattern, named_columns_limit",
        [
            # Arrays of simple elements can't be named
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {"c": [1, 2, 3]},
                ],
                r".*Field \".*?\" doesn't have any properties \(.*?\), so \"named\" option can't be applied.*",
                50,
            ],
            # No `name` field to use
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {"c": {"name1": "color", "value": "blue"}},
                ],
                r".*Field \".*?\" doesn't have name property \".*?\", so \"named\" option can't be applied.*",
                50,
            ],
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {"c": [{"name1": "color", "value": "blue"}]},
                ],
                r".*Field \".*?\" doesn't have name property \".*?\", so \"named\" option can't be applied.*",
                50,
            ],
            # Non-hashable dict can't be named (no names/values collected)
            [
                {"c": FieldOption(named=True, name="name", grouped=False)},
                {},
                [
                    {"c": {"name": "color", "value": "blue", "list": [1, 2, 3]}},
                ],
                r".*Field \".*?\" doesn't have any properties \(.*?\), so \"named\" option can't be applied.*",
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
                r".*Field \".*?\" values for name property \".*?\" were limited by \"named_columns_limit\" when "
                r"collecting stats, so \"named\" option can't be applied.*",
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
                r".*Field \".*?\" values for name property \".*?\" were limited by \"named_columns_limit\" when "
                r"collecting stats, so \"named\" option can't be applied.*",
                2,
            ],
            # Incorrect headers_order
            [
                {},
                {"headers_order": ["name", 123]},
                [{"name": "value", "another_name": "another_value"}],
                r".*Headers provided through headers_order must be strings, not <class 'int'>.*",
                50,
            ],
            # Incorrect headers_filters
            [
                {},
                {"headers_filters": ["name", 123]},
                [{"name": "value", "another_name": "another_value"}],
                r".*Regex statements provided through headers_filters must be strings, not <class 'int'>.*",
                50,
            ],
        ],
    )
    def test_export_warnings(
        self,
        caplog,
        field_options: Dict[str, FieldOption],
        export_options: Dict,
        items: List[Dict],
        warning_pattern: str,
        named_columns_limit: int,
    ):
        csv_stats_col = StatsCollector(named_columns_limit=named_columns_limit)
        csv_stats_col.process_items(items)
        with caplog.at_level(logging.WARNING):
            Exporter(
                stats=csv_stats_col._stats,
                invalid_properties=csv_stats_col._invalid_properties,
                field_options=field_options,
                **export_options,
            )
        assert re.match(warning_pattern, caplog.text)

    @pytest.mark.parametrize(
        "field_options, export_options, items, named_columns_limit",
        [
            # If both grouped and named - everything is a single cell, so no limits would be applied
            [
                {"c": FieldOption(named=True, name="name", grouped=True)},
                {},
                [
                    {"c": [{"name": "color", "value": "blue"}]},
                    {"c": [{"name": "color", "value": "green"}]},
                    {"c": [{"name": "color", "value": "cyan"}]},
                ],
                2,
            ]
        ],
    )
    def test_no_exceptions(
        self,
        field_options: Dict[str, FieldOption],
        export_options: Dict,
        items: List[Dict],
        named_columns_limit: int,
    ):
        csv_stats_col = StatsCollector(named_columns_limit=named_columns_limit)
        csv_stats_col.process_items(items)
        Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
            field_options=field_options,
            **export_options,
        )

    def test_buffer_io(self):
        item_list = [
            {"c": {"name": "color", "value": "green"}},
            {"c": {"name": "color", "value": "blue"}},
        ]
        csv_stats_col = StatsCollector()
        csv_stats_col.process_items(item_list)
        csv_exporter = Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
        )
        buffer = io.StringIO()
        csv_exporter.export_csv_full(item_list, buffer)
        assert buffer.getvalue() == "c->name,c->value\r\ncolor,green\r\ncolor,blue\r\n"

    def test_file_io(self, tmpdir):
        item_list = [
            {"c": {"name": "color", "value": "green"}},
            {"c": {"name": "color", "value": "blue"}},
        ]
        csv_stats_col = StatsCollector()
        csv_stats_col.process_items(item_list)
        csv_exporter = Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
        )
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
        csv_stats_col = StatsCollector()
        csv_stats_col.process_items(item_list)
        csv_exporter = Exporter(
            stats=csv_stats_col._stats,
            invalid_properties=csv_stats_col._invalid_properties,
        )
        filename = tmpdir.join("custom.csv")
        # Test path-like objects
        csv_exporter.export_csv_full(item_list, filename)
        with open(filename, "r") as f:
            assert f.read() == "c->name,c->value\ncolor,green\ncolor,blue\n"
        # Stringify path to make sure exporter works with regular string paths also
        csv_exporter.export_csv_full(item_list, str(filename))
        with open(str(filename), "r") as f:
            assert f.read() == "c->name,c->value\ncolor,green\ncolor,blue\n"
