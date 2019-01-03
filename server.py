import SimpleHTTPServer
import SocketServer
import json
import argparse
from google.cloud import pubsub_v1
from fake_retail_data import basket_orders
import os


ap = argparse.ArgumentParser()

ap.add_argument("-e", "--env", required=True,help="local or gcp env")
ap.add_argument("-t", "--topic", required=True,help="topic")
ap.add_argument("-ep", "--project", required=True,help="project")

args = vars(ap.parse_args())

topic_name = args['topic']
project_id = args['project']


if args['env'] != 'gcp':
    os.environ['GOOGLE_CLOUD_DISABLE_GRPC'] = 'true'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './sa.json'

    if os.environ.get('PUBSUB_EMULATOR_HOST'):
        del os.environ['PUBSUB_EMULATOR_HOST']


PORT = 8000

class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def do_POST(self):
      length = int(self.headers.getheader('content-length'))
      message = json.loads(self.rfile.read(length))

      bask = basket_orders()
      basket_rows = bask.basket_orders()

      for r in bask.basket:
          #print r
          message_future = pubsub_client.publish(topic_path, data=r.encode('utf-8'))

      self.send_response(200)
      self.end_headers()
      #print message
      '''
      content_len = int(self.headers.getheader('content-length', 0))
      post_body = self.rfile.read(content_len)
      self.send_response(200)
      self.end_headers()
      json_body= json.dumps(post_body)
      print json_body,self.client_address[0]
      '''

    def do_GET(self):
        print 'here'
        bask = basket_orders()
        basket_rows = bask.basket_orders()

        for r in bask.basket:
            print r
            message_future = pubsub_client.publish(topic_path, data=r.encode('utf-8'))

        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        # Send the html message
        #self.wfile.write("Hello World !")
        return

    def callback(message_future):
        # When timeout is unspecified, the exception method waits indefinitely.
        if message_future.exception(timeout=3):
            print('Publishing message on {} threw an Exception {}.'.format(topic_name, message_future.exception()))
        else:
            print(message_future.result())


batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=512000,
    max_latency=1,  # One second
    max_messages=500
)

pubsub_client = pubsub_v1.PublisherClient(batch_settings)
topic_path = pubsub_client.topic_path(project_id, topic_name)

Handler = ServerHandler

httpd = SocketServer.TCPServer(("", PORT), Handler)

print "serving at port", PORT
httpd.serve_forever()