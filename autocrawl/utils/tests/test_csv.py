import codecs
import csv
import json

import pytest
from pkg_resources import resource_stream, resource_string

from ..csv_export import CSVExporter


class TestCSV:
    @pytest.mark.parametrize(
        "case_name, named_properties, array_limits",
        [
            ("articles_xod_test", {}, {}),
            ("items_recursive_test", {"named_array_field": "name"}, {}),
            (
                "products_full_schema_test",
                {
                    "gtin": "type",
                    "additionalProperty": "name",
                    "ratingHistogram": "ratingOption",
                },
                {"offers": 1},
            ),
            (
                "products_simple_xod_test",
                {"gtin": "type", "additionalProperty": "name"},
                {"offers": 1},
            ),
            (
                "products_xod_test",
                {"gtin": "type", "additionalProperty": "name"},
                {"offers": 1},
            ),
        ],
    )
    def test_csv_export(self, case_name, named_properties, array_limits):
        # Load item list from JSON (simulate API response)
        item_list = json.loads(
            resource_string(__name__, f"assets/{case_name}.json").decode("utf-8")
        )
        csv_exporter = CSVExporter(named_properties, array_limits)
        # Collect stats
        for it in item_list:
            csv_exporter.process_item(it)
        csv_exporter.flatten_headers()
        # Compare with pre-processed data
        csv_data = list(
            csv.reader(
                codecs.getreader("utf-8")(
                    resource_stream(__name__, f"assets/{case_name}.csv")
                )
            )
        )
        assert len([csv_exporter.flat_headers] + item_list) == len(csv_data)
        # Comparing row by row
        for item, row in zip(item_list, csv_data[1:]):
            # Stringify all values because to match string data from csv
            assert [str(x) for x in csv_exporter.export_item(item)] == row