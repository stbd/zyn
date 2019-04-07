import subprocess
import sys


def execute_script(path_script, path_working_dir):
    print('Executing script "{}"'.format(path_script))
    process = subprocess.Popen(
        [
            path_script
        ],
        cwd=path_working_dir,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    process.wait()
    if process.returncode != 0:
        raise RuntimeError('Packing Zyn source failed')


def build_image(image_tag, path_image, path_working_dir):
    print('Building image "{}"'.format(path_image))
    build_cmd = [
        'docker',
        'build',
        '-t',
        image_tag,
        path_image,
    ]

    process = subprocess.Popen(
        build_cmd,
        cwd=path_working_dir,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    process.wait()
    if process.returncode != 0:
        raise RuntimeError('Building image failed')
