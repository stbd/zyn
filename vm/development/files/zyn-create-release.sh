#!/usr/bin/env bash
set -euo pipefail

function usage() {
    echo "Usage: $(basename $0) <release-type> <version>"
    echo
    echo "where <release-type> is one of"
    echo "* system - tag commit for system release"
    echo "* docker-zyn - create Docker image of Zyn server"
    echo "* docker-web - create Docker image of web-client"
    echo "* py - create release of Python package"
    echo
    echo "Tag conventions:"
    echo "Release: <x>.<y>"
    echo "Release candidate: <x>.<y>-rc-<num>"
}

if [ $# -ne 2 ]; then
    usage
    exit 1
fi

release_type=$1
version=$2
tag_image_server=stbd/zyn:$version
tag_image_web=stbd/zyn-client-web:$version

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"

if [ "$release_type" == "system" ] ; then

    tag=v$version
    cat<<EOF | git tag -a "$tag" -F -
Zyn version $version

 * Server image:      $tag_image_server
 * Web client image:  $tag_image_web
EOF
    echo "Git tag \"$tag\" create, please push with \"git push origin $tag\" to publish tag"

elif [ "$release_type" == "docker-zyn" ] ; then

    docker build \
           -t "$tag_image_server" \
           -f "$zyn_project_root/docker/dockerfile-zyn" \
           "$zyn_project_root"

    echo "Docker image \"$tag_image_server\" created, please run \"docker push $tag_image_server\" to publish image"

elif [ "$release_type" == "docker-web" ] ; then

    docker build \
           -t "$tag_image_web" \
           -f "$zyn_project_root/docker/dockerfile-web-client" \
           "$zyn_project_root"

    echo "Docker image \"$tag_image_web\" created, please run \"docker push $tag_image_web\" to publish image"

elif [ "$release_type" == "py" ] ; then

    path_workdir="$(mktemp -d)"
    echo "Using workdir $path_workdir"
    ZYN_PY_VERSION=$version pip wheel --no-deps -w "$path_workdir" "$zyn_project_root/py"
    generated_file="$(find "$path_workdir" -name 'PyZyn*whl')"

    path_output="$PWD/$(basename $generated_file)"
    mv "$generated_file" "$path_output"
    rm -rf "$path_workdir"

    echo "PyZyn generated to \"$path_output\""

else

    echo
    echo "ERROR: unknown release type\"$release_type\""
    echo
    usage
    exit 1

fi
