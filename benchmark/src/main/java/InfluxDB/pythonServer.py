from http.server import SimpleHTTPRequestHandler, HTTPServer
import threading
import os

print("cd:", os.getcwd())

class CustomHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/stop':
            print("Server wird beendet...")
            threading.Thread(target=self.server.shutdown).start()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Server stopped")
        else:
            super().do_GET()

def run(server_class=HTTPServer, handler_class=CustomHandler):
    #os.chdir("../../../..")
    server_address = ('', 8001)
    httpd = server_class(server_address, handler_class)
    print("Server started on http://localhost:8001")
    httpd.serve_forever()

if __name__ == '__main__':
    run()
