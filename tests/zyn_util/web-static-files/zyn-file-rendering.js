
function _render_image(mime_type, base64_content) {
    var html = '<img src="data:' + mime_type + ';';
    html += 'base64,' + base64_content + '">';
    return html;
}

function zyn_render_file_content(content_element, current_file) {

    while (content_element.firstChild) {
        content_element.removeChild(content_element.firstChild);
    }

    if (current_file.file_extension() == 'md') {
        var converter = new showdown.Converter({
            'simplifiedAutoLink': true,
        });
        var html = converter.makeHtml(current_file.decoded());
        content_element.innerHTML = html;

    } else if (current_file.file_extension() == 'jpg') {
        content_element.innerHTML = _render_image("image/jpeg",  btoa(current_file.bytes()));

    } else if (current_file.file_extension() == 'pdf') {

        var root = document.createElement('div');
        // console.log('Loading pdf');

        var loadingTask = pdfjsLib.getDocument({data: current_file.bytes()});
        loadingTask.promise.then(function(pdf) {
            // console.log('PDF loaded');

            var pageNumber = 1;
            for (var pageNumber = 1; pageNumber <= pdf.numPages; pageNumber++) {
                pdf.getPage(pageNumber).then(function(page) {
                    // console.log('Page loaded');

                    var scale = 1.5;
                    var viewport = page.getViewport(scale);

                    var canvas = document.createElement('canvas');
                    root.appendChild(canvas);
                    var context = canvas.getContext('2d');
                    canvas.height = viewport.height;
                    canvas.width = viewport.width;
                    var renderContext = {
                        canvasContext: context,
                        viewport: viewport
                    };
                    var renderTask = page.render(renderContext);
                    renderTask.then(function () {
                        // console.log('Page rendered');
                    });
                });
            }
        }, function (reason) {
            // PDF loading error
            console.error(reason);
        });
        content_element.appendChild(root);

    } else {
        content_element.innerHTML = '<pre>' + current_file.decoded() + '</pre>';
    }
}
