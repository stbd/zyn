#!/usr/bin/env python3

import os.path
import argparse


PATH_FILE = os.path.dirname(os.path.abspath(__file__))
PATH_CLIENT = PATH_FILE + '/../../../zyn/src/node/client.rs'


def _parse_errors():
    errors = []
    with open(PATH_CLIENT, 'r') as fp:
        for line in fp:
            line = line.strip()
            lc_line = line.lower()
            elements = line.split()
            if not(
                    len(elements) == 3
                    and 'error' in lc_line
                    and '=' in lc_line
            ):
                continue

            try:
                error_code = int(elements[-1].replace(',', ''))
            except ValueError:
                continue

            if '::' in lc_line:
                name = elements[0].split('::')[1]
            else:
                name = elements[0]

            errors.append((name, error_code))
    return errors


def write_module_python(errors, path):
    print('Writing errors in Python to "{}"'.format(path))
    with open(path, 'w') as fp:
        for (name, code) in errors:
            fp.write('{} = {}\n'.format(name, code))

        fp.write('\n\ndef error_to_string(error):\n')
        fp.write('    if error == {}:\n'.format(errors[0][1]))
        fp.write('        return "{}"\n'.format(errors[0][0]))

        for (name, code) in errors[1:]:
            fp.write('    elif error == {}:\n'.format(code))
            fp.write('        return "{}"\n'.format(name))

        fp.write('    else:\n')
        fp.write('        raise RuntimeError("Unknown error")\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'language',
        choices=['python'],
        help='Target language',
    )
    parser.add_argument(
        'path-output',
        help='Path to generated file'
    )

    args = vars(parser.parse_args())
    errors = _parse_errors()

    print('Parsed {} error codes'.format(len(errors)))
    if args['language'] == 'python':
        write_module_python(errors, args['path-output'])


if __name__ == '__main__':
    main()
