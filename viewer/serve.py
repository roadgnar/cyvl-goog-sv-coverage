"""Simple HTTP server with Range request support for PMTiles."""
import http.server
import os
import shutil

class RangeHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            return super().do_GET()

        file_size = os.path.getsize(path)
        range_header = self.headers.get('Range')

        if range_header:
            try:
                range_spec = range_header.replace('bytes=', '')
                start_str, end_str = range_spec.split('-')
                start = int(start_str) if start_str else 0
                end = int(end_str) if end_str else file_size - 1
                end = min(end, file_size - 1)
                length = end - start + 1

                self.send_response(206)
                self.send_header('Content-Type', self.guess_type(path))
                self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
                self.send_header('Content-Length', str(length))
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()

                with open(path, 'rb') as f:
                    f.seek(start)
                    self.wfile.write(f.read(length))
                return
            except Exception as e:
                self.send_error(416, str(e))
                return

        self.send_response(200)
        self.send_header('Content-Type', self.guess_type(path))
        self.send_header('Content-Length', str(file_size))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        with open(path, 'rb') as f:
            shutil.copyfileobj(f, self.wfile)

if __name__ == '__main__':
    http.server.test(HandlerClass=RangeHTTPRequestHandler, port=8085)
