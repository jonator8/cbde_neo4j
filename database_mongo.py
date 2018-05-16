import sys
import random
import pymongo
from pprint import pprint

from pymongo import MongoClient
from datetime import datetime, timedelta

class CreateData:

    db = None;
    collection_partsupp = None
    collection_lineitem = None
    nations = ["cat", "es", "uk", "ger", "fr", "it"]
    regions = ["eu", "usa", "oce"]
    customers = []
    suppliers_lineitem = []
    parts = []
    suppliers_partsupp = []

    def __init__(self):
        client = MongoClient()
        self.db = client.lab5
        self.collection_partsupp = self.db.partsupp
        self.collection_lineitem = self.db.lineitem

    def create_customers(self, n):
        for i in range(n):
            yield {
                    "mktsegment" : "mkt%d" % random.randint(1,5),
                    "nationName" : random.choice(self.nations),
                    "regionName" : random.choice(self.regions)
            }
    def create_suppliers_lineitem(self, n):
        for i in range(n):
            yield {
                "nationName": random.choice(self.nations),
                "regionName": random.choice(self.regions)
            }

    def create_lineitem(self, n):
        for i in range(n):
            ORDERKEY = random.randint(0, 10)

            yield {
                "_id": str(ORDERKEY)+"_"+str(i),
                "lineNumber": i,
                "orderKey": ORDERKEY,
                "lineStatus": random.randint(0, 5),
                "quantity": random.randint(1, 10),
                "extendedPrice": random.randint(10, 100),
                "returnFlag": random.randint(0, 1),
                "discount": 0.1,
                "tax": 0.21,
                "shipDate": datetime.today() - timedelta(days=i),
                "order":  {
                    "suppPriority" : 1,
			        "orderDate" : datetime.today() - timedelta(days=i)
                },
                "customer": random.choice(self.customers),
                "supplier": random.choice(self.suppliers_lineitem)
            }

    def create_partsupp(self, n):
        for i in range(n):
            PART = random.choice(self.parts)
            SUPP = random.choice(self.suppliers_partsupp)

            yield {
                "_id": str(i)+"_"+str(PART['partKey'])+"_"+str(SUPP['suppKey']),
                "supplyCost": random.randint(1, 150),
                "part": PART,
                "supplier": SUPP
            }

    def create_part(self, n):
        for i in range(n):
            yield {
                "partKey": i,
                "MFGR": "mfgr%d" % random.randint(1,20),
                "type": "type%d" % random.randint(1,5),
                "size": "%d" % random.randint(1,10)
            }

    def create_supplier_partsupp(self, n):
        for i in range(n):
            yield {
                "suppKey": i,
                "name": "supp%d" % random.randint(1,500),
                "address": "address%d" % random.randint(1,500),
                "phone": "%d" % random.randint(600000000,799999999),
                "acctbal": "%d" % random.randint(1,100),
                "comment": "comment %d" % random.randint(1,500),
                "nationName": random.choice(self.nations),
                "regionName": random.choice(self.regions)
            }

    def create_data(self):
        print "create_data"
        self.customers = list(self.create_customers(15))
        self.suppliers_lineitem = list(self.create_suppliers_lineitem(15))
        self.lineitem = list(self.create_lineitem(6000))
        print self.collection_lineitem.insert_many(self.lineitem)
        self.parts = list(self.create_part(15))
        self.suppliers_partsupp = list(self.create_supplier_partsupp(15))
        self.partsupp = list(self.create_partsupp(60000))
        print self.collection_partsupp.insert_many(self.partsupp)

    def delete_data(self):
        print "delete_data"
        print self.collection_partsupp.delete_many({})
        print self.collection_lineitem.delete_many({})

    def query1(self):
        print "Query1"
        DATE = datetime.today() - timedelta(days=20)
        print DATE

        MATCH = {"shipDate": {"$lte": DATE}}
        GROUP = {
            '_id' : {
                'returnFlag': '$returnFlag',
                'lineStatus': '$lineStatus',
            },
            "count_order": { "$sum": 1 },
            "sum_qty": { "$sum": '$quantity' },
            "sum_base_price": { "$sum": '$extendedPrice' },
            "sum_disc_price": {
                "$sum": {
                    '$multiply': [
                        '$extendedPrice',
                        {'$subtract': [1,'$discount']}
                    ]
                }
            },
            "sum_charge": {
                "$sum": {
                    '$multiply': [
                        '$extendedPrice',
                        {'$subtract': [1,'$discount']},
                        {'$add': [1, '$tax']},
                    ]
                }
            },
            "avg_qty": { "$avg": '$quantity' },
            "avg_disc": { "$avg": '$discount' },
            "avg_price": { "$avg": '$extendedPrice' },
        }
        SORT = { '_id.returnFlag': 1 , "_id.lineStatus": 1 }

        responses = self.collection_lineitem.aggregate(
            [{"$match": MATCH},
             {'$group': GROUP},
             {'$sort': SORT}]
        )
        for response in responses:
            pprint(response)

    def query2(self):
        print "Query 2"
        SIZE = "5"
        TYPE = "type3"
        REGION = "oce"

        GROUP = {
            '_id': None,
            "min_supplyCost": {
                "$min": '$supplyCost'
                },
        }

        MATCH = {
            "supplier.regionName": REGION
        }

        responses = self.collection_partsupp.aggregate([
                {"$match": MATCH},
                {'$group': GROUP}
        ]);
        MIN = list(responses)[0]['min_supplyCost'];
        MATCH = {
            "$and": [
                    {
                        "supplyCost": MIN
                    },
                    {
                        "part.size": SIZE
                    },
                    {
                        "part.type": TYPE
                    },
                    {
                        "supplier.regionName": REGION
                    },
                ]
        }

        SORT = { "supplier.acctbal": -1, "supplier.nationName": 1, "supplier.name": 1, "part.partKey": 1 }
        PROJECT = { "supplier.acctbal": 1, "supplier.name": 1, "supplier.nationName": 1,
            "part.partKey": 1, "part.MFGR": 1, "supplier.address":1, "supplier.phone":1, "supplier.comment":1 }

        responses = self.collection_partsupp.aggregate(
            [
                 {"$match": MATCH},
                 {"$project": PROJECT},
                 {'$sort': SORT}
             ]
        )
        for response in responses:
            pprint(response)

    def query3(self):
        print "Query3"

        DATE = datetime.today() - timedelta(days=10)
        print DATE

        DATE2 = datetime.today() - timedelta(days=20)
        print DATE2

        MATCH = {
            "$and": [
                {
                    "customer.mktsegment": {
                        "$eq": random.choice(["mkt1","mkt2", "mkt3","mkt4", "mkt5"])
                    }
                },
                {
                    "order.orderDate": {
                        "$lt": DATE
                    }
                } ,
                {
                    "shipDate": {
                        "$gt": DATE2
                    }
                }
            ]
        }
        GROUP = {
            "_id": {
                "orderKey": "$orderKey",
                "orderDate": "$order.orderDate",
                "suppPriority": "$order.suppPriority",
            },
            "revenue": {
                "$sum": {
                    "$multiply": [
                        "$extendedPrice" ,
                        {
                            "$subtract": [1,"$discount"]
                        }
                    ]
                }
            }
        }
        SORT = { 'revenue': -1 , "order.orderDate":1 }

        responses = self.collection_lineitem.aggregate(
            [{"$match": MATCH},
             {'$group': GROUP},
             {'$sort': SORT}]
        )
        for response in responses:
            pprint(response)

    def query4(self):
        print "Query 4"
        REGION = "oce"
        DATE1 = datetime.today() - timedelta(days=200)
        DATE2 = datetime.today() + timedelta(days = 365)

        MATCH = {
            "$and": [
 					{
 						"$expr": {
 							"$eq": ["$customer.nationName","$supplier.nationName"]
 						}
 					},
 					{
 						"customer.regionName": REGION
 					},
 					{
 						"order.orderDate": {
 							"$lt": DATE2
 						}
 					},
 					{
 						"order.orderDate": {
 							"$gt": DATE1
 						}
 					}
 				]
        }

        GROUP = {
            '_id' : '$supplier.nationName',
            "revenue": {
                "$sum": {
                    '$multiply': [
                        '$extendedPrice',
                        {'$subtract': [1,'$discount']}
                    ]
                }
            },
        }

        SORT = { "revenue": -1 }

        responses = self.collection_lineitem.aggregate(
            [{"$match": MATCH},
             {'$group': GROUP},
             {'$sort': SORT}]
        )
        for response in responses:
            pprint(response)


# usage
# python database.py create_data
# python database.py delete_data
# python database.py query1
# python database.py query2
# python database.py query3
# python database.py query4

cd = CreateData();
print {
    "create_data": cd.create_data,
    "delete_data": cd.delete_data,
    "query1": cd.query1,
    "query2": cd.query2,
    "query3": cd.query3,
    "query4": cd.query4,
}[sys.argv[1]]()
