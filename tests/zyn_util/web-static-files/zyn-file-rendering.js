function zyn_render_file_content(content_element, current_file) {

    while (content_element.firstChild) {
        content_element.removeChild(content_element.firstChild);
    }

    if (current_file.file_extension() == 'md') {
        var converter = new showdown.Converter({
            'simplifiedAutoLink': true,
        });
        var decoded = utf8.decode(current_file.bytes());
        var html = converter.makeHtml(decoded);
        content_element.innerHTML = html;
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
        content_element.innerHTML = '<pre>' + current_file.bytes() + '</pre>';
    }
}
