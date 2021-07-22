import argparse
import json


class Formatter(argparse.RawTextHelpFormatter,
                argparse.RawDescriptionHelpFormatter):
    pass


parser = argparse.ArgumentParser(description='Export JSON as CSV.', formatter_class=Formatter) # NOQA
parser.add_argument('--path', metavar='path', type=str, help='the path to JSON file;')
parser.add_argument('--outpath', metavar='output path', type=str, help='where to save CSV output;')
parser.add_argument('-s', '-stringify',
                    action='store_true',
                    help='stringify invalid data instead of skipping;')
parser.add_argument('--ncl',
                    metavar='named columns limit',
                    type=str,
                    help='maximum number of columns that could be created based on properties names (named fields);')
parser.add_argument('--cs',
                    metavar='cut separator',
                    type=str,
                    help='separator for intenal paths;\n'
                         'if your properties names include default separator "->" - replace it with a custom one;')
parser.add_argument('--gs',
                    metavar='grouped separator',
                    type=str,
                    help='separator to use when grouping values in a single cell with "grouped" field option;\n'
                         'default separator is "\\n";')
parser.add_argument('--fieldoptions', '--fo',
                    metavar='field options',
                    type=json.loads,
                    help='options to format fields;\n'
                         'fields values could be grouped into single cells, or '
                         'have named columns based on properties names, or both;\n'
                         'example: \'{"gtin": "grouped": False, "named": True, "name":"type"};\''),
parser.add_argument('--arraylimits', '--al',
                    metavar='array limits',
                    type=json.loads,
                    help='limit for the arrays to export only first N elements;\n'
                         'example: \'{"offers": 1};\''),
parser.add_argument('--headersrenaming', '--hr',
                    metavar='headers renaming',
                    type=json.loads,
                    help='regexp rules to rename existing colulmns;\n'
                         'example: \'[".*_price", "regularPrice"]\';'),
parser.add_argument('--headersorder', '--ho',
                    metavar='headers order',
                    type=json.loads,
                    help='list to sort CSV headers; if header in the list - it will be sorted, '
                         'if not - it will be appeneded in a natural order;\n'
                         'example: \'["name", "offers[0]->price", "url"]\';')
parser.add_argument('--headersfilters', '--hf',
                    metavar='headers filters',
                    type=json.loads,
                    help='list of regex statements; headers that match any of these statements would be skipped;\n'
                         'example: \'["name.*", "_key"]\';')

args = parser.parse_args()
print(args)