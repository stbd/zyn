# Notes on on how to build 3pp libraries

* Install Node and Yarn with zyn-install-web-tools.sh

## Libaries

### Pdf.js

git clone https://github.com/mozilla/pdf.js.git
cd pdf.js
git checkout <version>

npm install -g gulp-cli
npm install
gulp generic
cp build/generic/build/pdf.js ../zyn/tests/zyn_util/web-static-files/3pp/pdfjs/pdf.js
cp build/generic/build/pdf.worker.js ../zyn/tests/zyn_util/web-static-files/3pp/pdfjs/

### Showdownjs

 * wget https://cdn.jsdelivr.net/npm/showdown@<version tag>/dist/showdown.min.js
 * Copy to 3pp

### Utf8

 * Download from https://github.com/mathiasbynens/utf8.js/releases
 * Unpack and copy to 3pp