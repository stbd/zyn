# Notes on on how to build 3pp libraries

* Install Node and Yarn with zyn-install-node.sh

## Libaries

todo: fix

git clone https://github.com/mozilla/pdf.js.git
cd pdf.js

npm install -g gulp-cli
cp build/generic/build/pdf.js ../zyn/tests/zyn_util/web-static-files/3pp/pdfjs/pdf.js
cp build/generic/build/pdf.worker.js ../zyn/tests/zyn_util/web-static-files/3pp/pdfjs/