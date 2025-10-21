import { Base } from './file.mjs';
import {getDocument, GlobalWorkerOptions} from 'pdfjs-dist';


class PdfFile extends Base {
  static filename_extension = '.pdf';
  static is_editable = false;

  constructor(open_rsp, client, filename, mode) {
    super(open_rsp, client, filename);
    this._content = null;

    this._client.ui().show_loading_modal('Loading file content...')

    if (open_rsp.size === 0) {
      this._content = '';
      this.render()
    } else {
      this.read_file_content(
        0,
        open_rsp.size,
        (data, revision) => {
          this._revision = revision
          this._content = data;
          this.render();
        }
      );
    }
  }

  render() {

    console.log(`Rendering PDF`)
    var scale = 1.5;
    let ui = this._client.ui();

    GlobalWorkerOptions.workerSrc = `${this._client.ui().get_browser_url().origin}/static/pdf.worker.mjs`

    getDocument({data: this._content}).promise.then(function(pdf) {

      console.log('PDF loaded');
      let pages = [];
      let page_number = 1;
      const number_of_pages = pdf.numPages;

      const render_page = (page) => {

        let canvas = ui.create_canvas();
        let context = canvas.getContext('2d');
        var viewport = page.getViewport({scale: scale});
        var render_context = {
          canvasContext: context,
          viewport: viewport
        };
        page.render(render_context);
        pages.push(canvas);

        page_number += 1;
        if (page_number <= number_of_pages) {
          pdf.getPage(page_number).then(render_page);
        } else {
          let content = ui.get_file_content();
          content.innerHTML = '';
          for (const c of pages) {
            content.appendChild(c);
          }
          ui.hide_modals();
        }
      };

      pdf.getPage(page_number).then(render_page);

    }, function (reason) {
      console.error(reason);
      ui.hide_modals();
    });
  }
}

export { PdfFile };
