import argparse
import io
import os

parser = argparse.ArgumentParser(description='Parse feed columns and values of the first item')
parser.add_argument(
    'path_to_file',
    help='path to the feed file'
)
parser.add_argument(
    '-d', '--delimiter',
    help='delimiter symbol to split feed lines',
    default=','
)
args = parser.parse_args()


def split_raw_line(raw_line):
    item = []
    i = 0
    join_next = False
    for _item in raw_line:
        if _item.startswith('"'):
            item.append(_item)
            if _item.endswith('"'):
                i += 1
            else:
                join_next = True
        else:
            if not join_next:
                item.append(_item)
                i += 1
            else:
                item[i] += _item
                if _item.endswith('"'):
                    join_next = False
                    i += 1
    return item


def get_output_file_name(input_file):
    folder_path, file_name = os.path.split(input_file)
    split_name = file_name.split('.')
    split_name[-2] = split_name[-2] + '_parsed'
    split_name[-1] = 'txt'
    output_file = os.path.join(folder_path, '.'.join(split_name))
    return output_file


if __name__ == '__main__':
    file = args.path_to_file
    delimiter = args.delimiter
    output = get_output_file_name(file)

    with io.open(file, 'r', encoding='utf-8') as feed:
        columns = feed.readline().split(delimiter)
        raw_item = feed.readline().split(delimiter)
        item = split_raw_line(raw_item)
        with io.open(output, 'w', encoding='utf-8') as output_file:
            for c, i in zip(columns, item):
                line = c + '\t' + i + '\n'
                output_file.write(line)
