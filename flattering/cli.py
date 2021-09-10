import argparse
import json

from flattering import Exporter, StatsCollector  # NOQA


def main():
    class Formatter(
        argparse.RawTextHelpFormatter, argparse.RawDescriptionHelpFormatter
    ):
        pass

    parser = argparse.ArgumentParser(
        description="Export JSON as CSV.", formatter_class=Formatter
    )
    parser.add_argument(
        "--path", metavar="path", type=str, help="the path to JSON file;", required=True
    )
    parser.add_argument(
        "--outpath",
        metavar="output path",
        type=str,
        help="where to save CSV output;",
        required=True,
    )
    parser.add_argument(
        "-s",
        "-skip",
        action="store_true",
        help="skip columns with invalid data instead of stringifying;",
    )
    parser.add_argument(
        "--namedcolumnslimit",
        "--ncl",
        metavar="named columns limit",
        type=str,
        help="maximum number of columns that could be created based on properties names (named fields);",
    )
    parser.add_argument(
        "--cs",
        metavar="cut separator",
        type=str,
        help="separator for intenal paths;\n"
        'if your properties names include default separator "->" - replace it with a custom one;',
    )
    parser.add_argument(
        "--gs",
        metavar="grouped separator",
        type=str,
        help='separator to use when grouping values in a single cell with "grouped" field option;\n'
        'default separator is "\\n";',
    )
    parser.add_argument(
        "--fieldoptions",
        "--fo",
        metavar="field options",
        type=json.loads,
        help="options to format fields;\n"
        "fields values could be grouped into single cells, or "
        "have named columns based on properties names, or both;\n"
        'example: \'{"gtin": "grouped": False, "named": True, "name":"type"};\'',
    ),
    parser.add_argument(
        "--arraylimits",
        "--al",
        metavar="array limits",
        type=json.loads,
        help="limit for the arrays to export only first N elements;\n"
        "example: '{\"offers\": 1};'",
    ),
    parser.add_argument(
        "--headersrenaming",
        "--hr",
        metavar="headers renaming",
        type=json.loads,
        help="regexp rules to rename existing colulmns;\n"
        'example: \'[".*_price", "regularPrice"]\';',
    ),
    parser.add_argument(
        "--headersorder",
        "--ho",
        metavar="headers order",
        type=json.loads,
        help="list to sort CSV headers; if header in the list - it will be sorted, "
        "if not - it will be appeneded in a natural order;\n"
        'example: \'["name", "offers[0]->price", "url"]\';',
    )
    parser.add_argument(
        "--headersfilters",
        "--hf",
        metavar="headers filters",
        type=json.loads,
        help="list of regex statements; headers that match any of these statements would be skipped;\n"
        'example: \'["name.*", "_key"]\';',
    )

    args = vars(parser.parse_args())

    stats_args = {}
    for arg, arg_name in [
        ("namedcolumnslimit", "named_columns_limit"),
        ("cs", "cut_separator"),
    ]:
        if args.get(arg) is not None:
            stats_args[arg_name] = arg
    csv_sc = StatsCollector(**stats_args)
    with open(args["path"], "r") as f:
        items_list = json.loads(f.read())
    csv_sc.process_items(items_list)

    export_args = {}
    for arg, arg_name in [
        ("cs", "cut_separator"),
        ("gs", "grouped_separator"),
        ("fieldoptions", "field_options"),
        ("arraylimits", "array_limits"),
        ("headersrenaming", "headers_renaming"),
        ("headersorder", "headers_order"),
        ("headersfilters", "headers_filters"),
    ]:
        if args.get(arg) is not None:
            stats_args[arg_name] = arg
    if args.get("s") is not None:
        export_args["stringify_invalid"] = not (args["s"])
    export_args["stats"] = csv_sc.stats["stats"]
    export_args["invalid_properties"] = csv_sc.stats["invalid_properties"]
    csv_exp = Exporter(**export_args)
    csv_exp.export_csv_full(items_list, args["outpath"])


if __name__ == "__main__":
    main()
