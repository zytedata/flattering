import codecs
import csv
import json

import pytest  # NOQA
from pkg_resources import resource_stream, resource_string

from ..csv_export import CSVExporter, CSVStatsCollector


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
        csv_exporter._prepare_for_export()
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


# @pytest.mark.parametrize(
#     "field_options, array_limits, items, expected",
#     [
#         [
#             {},
#             {},
#             [{"c": {"name": "color", "value": "green"}}],
#             [["c->name", "c->value"], ["color", "green"]],
#         ],
#         # Failing
#         # [
#         #     FieldOption(named=True, name="name"), {},
#         #     [{'c': {'name': 'color', 'value': 'green'}}],
#         #     [...]
#         # ],
#         # Failing
#         # [
#         #     FieldOption(named=True, name="name", grouped=True), {},
#         #     [{'c': [{'name': 'color', 'value': 'green', "dfsfsd": ["432"]]}],
#         #     [['c'],
#         #      ['color: green']]
#         # ],
#         # Failing
#         # [
#         #     {"c": FieldOption(grouped=True, named=False)}, {},
#         #     [{'c': {'name': 'color', 'value': 'green'}}],
#         #     [['c'],
#         #      ['name: color\nvalue: green']]
#         # ],
#         [
#             {},
#             {},
#             [
#                 {
#                     "c": [
#                         {"name": "color", "value": "green"},
#                         {"name": "size", "value": "XL"},
#                     ]
#                 }
#             ],
#             [
#                 ["c[0]->name", "c[0]->value", "c[1]->name", "c[1]->value"],
#                 ["color", "green", "size", "XL"],
#             ],
#         ],
#         [
#             {"c": FieldOption(grouped=False, named=True, name="name")},
#             {},
#             [
#                 {
#                     "c": [
#                         {"name": "color", "value": "green"},
#                         {"name": "size", "value": "XL"},
#                     ]
#                 }
#             ],
#             [["c->color->value", "c->size->value"], ["green", "XL"]],
#         ],
#         [
#             {"c": FieldOption(grouped=True, named=False)},
#             {},
#             [
#                 {
#                     "c": [
#                         {"name": "color", "value": "green"},
#                         {"name": "size", "value": "XL"},
#                     ]
#                 }
#             ],
#             [["c->name", "c->value"], ["color\nsize", "green\nXL"]],
#         ],
#         [
#             {"c": FieldOption(grouped=True, named=True, name="name")},
#             {},
#             [
#                 {
#                     "c": [
#                         {"name": "color", "value": "green"},
#                         {"name": "size", "value": "XL"},
#                     ]
#                 }
#             ],
#             [["c"], ["color: green\nsize: XL"]],
#         ],
#     ],
# )
# def test_csv(field_options, array_limits, items, expected):
#     csv_stats_col = CSVStatsCollector(named_columns_limit=50)
#     csv_stats_col.process_items(items)
#
#     csv_exporter = CSVExporter(
#         default_stats=csv_stats_col.stats,
#         field_options=field_options,
#         array_limits=array_limits,
#     )
#
#     csv_exporter._prepare_for_export()
#     headers = csv_exporter._get_renamed_headers(
#         csv_exporter._headers, csv_exporter.headers_renaming
#     )
#     exp_items = [csv_exporter.export_item_as_row(item) for item in items]
#     assert [headers] + exp_items == expected
