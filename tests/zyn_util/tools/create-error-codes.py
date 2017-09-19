#!/usr/bin/env python3

import os.path

PATH_FILE = os.path.dirname(os.path.abspath(__file__))
PATH_CLIENT = PATH_FILE + '/../../../zyn/src/node/client.rs'
PATH_OUTPUT = PATH_FILE + '/../errors.py'

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

print('Parsed {} error codes'.format(len(errors)))
print('Writing to "{}"'.format(PATH_OUTPUT))
with open(PATH_OUTPUT, 'w') as fp:
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
