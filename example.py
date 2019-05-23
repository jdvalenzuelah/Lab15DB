'''
	Universidad del Valle de Guatemala
	Base de Datos
		Marcos Gutierrez		17909
		David Valenzuela		171001
		Fernando Hengstenberg	17699	
'''

from pymongo import MongoClient

client = MongoClient()
client = MongoClient('localhost', 27017)
db = client.lab15
collection = db.products

products = {
		'"sku"': "00e8da9b",
	  	"type": "Audio Album",
	  	"title": "A Love Supreme",
	  	"description": "by John Coltrane",
	  	"asin": "B0000A118M",

		"shipping": {
			"weight": 6,
		"dimensions": {
			"width": 10,
			"height": 10,
			"depth": 1
    	},
    },

  "pricing": {
  	"list": 1200,
    "retail": 1100,
    "savings": 100,
    "pct_savings": 8
  },

  "details": {
    "title": "A Love Supreme [Original Recording Reissued]",
    "artist": "John Coltrane",
    "genre": [ "Jazz", "General" ],
    "tracks": [
      "A Love Supreme Part I: Acknowledgement",
      "A Love Supreme Part II - Resolution",
      "A Love Supreme, Part III: Pursuance",
      "A Love Supreme, Part IV-Psalm"
    ],
  },
}

# Insertamos la colección
collection.insert_one(products)

query = db.products.find(
	{
		'type':'Audio Album',
    	'details.genre': 'Jazz'
    }
)

query = query.sort(
	[('details.issue_date', -1)]
)

