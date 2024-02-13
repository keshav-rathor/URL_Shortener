from flask import Flask, request, jsonify
from flask_restful import Api, Resource
from kazoo.client import KazooClient
from pymongo import MongoClient
import string
import itertools

app = Flask(__name__)
api = Api(app)

class URLShortener:
    def __init__(self, zk_hosts):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        self.counter_path = "/counter"
        self.counter_node = self.zk.Counter(self.counter_path)
        self.base62_chars = string.digits + string.ascii_letters
        self.base62_base = len(self.base62_chars)

    def generate_short_url(self, long_url):
        counter_value = self.counter_node.next()
        short_id = self.encode_base62(counter_value)
        short_url = "https://short.url/" + short_id  # Replace "https://short.url/" with your predefined URL prefix
        return short_url

    def encode_base62(self, num):
        if num == 0:
            return self.base62_chars[0]
        encoded = ""
        while num > 0:
            num, remainder = divmod(num, self.base62_base)
            encoded = self.base62_chars[remainder] + encoded
        return encoded

class ShortenURL(Resource):
    def __init__(self, zk_hosts, mongo_uri):
        self.shortener = URLShortener(zk_hosts)
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client.url_shortener

    def post(self):
        long_url = request.json.get('long_url')
        if not long_url:
            return jsonify({'error': 'Missing long_url parameter'}), 400

        short_url = self.shortener.generate_short_url(long_url)
        self.save_to_mongodb(short_url, long_url)

        return jsonify({'short_url': short_url}), 201

    def save_to_mongodb(self, short_url, long_url):
        urls_collection = self.db.urls
        urls_collection.insert_one({'short_url': short_url, 'long_url': long_url})

api.add_resource(ShortenURL, '/shorten')

if __name__ == '__main__':
    print("I am working")
    zk_hosts = 'localhost:2181'  # Zookeeper hosts
    mongo_uri = 'mongodb://keshavrathor1998:toor1234@ac-ywneu7n-shard-00-00.2r72v4i.mongodb.net:27017,ac-ywneu7n-shard-00-01.2r72v4i.mongodb.net:27017,ac-ywneu7n-shard-00-02.2r72v4i.mongodb.net:27017/?replicaSet=atlas-13shfj-shard-0&ssl=true&authSource=admin'  # MongoDB URI
    app.run(debug=True)  # Run Flask app
