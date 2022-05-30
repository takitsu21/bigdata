import redis
import csv
from dateutil.parser import parse
import pandas as pd
import sys

db = redis.Redis(host='localhost', port=6379, db=0)

start = int(sys.argv[1]) if len(sys.argv) == 2 else 0

if start == 0:
    db.flushdb()
    db.flushall()

if start < 1:
    print("1.Feedback...")
    with open("./DATA/Feedback/Feedback.csv", "r") as f:
        reader = csv.reader(f, quotechar='"', delimiter='|',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # asin | PersonId | feedback
            db.hset(row[0], row[1], row[2])

if start < 2:
    print("2.BrandByProduct...")
    with open("./DATA/Product/BrandByProduct.csv", "r") as f:
        reader = csv.reader(f, quotechar='"', delimiter=',',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # brand | asin
            db.hset(row[1], "brand", row[0])

if start < 3:
    print("3.Product...")
    with open("./DATA/Product/Product.csv", "r") as f:
        next(f)
        reader = csv.reader(f, quotechar='"', delimiter=',',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # asin | title | price | imgUrl
            db.hset(row[0], "title", row[1])
            db.hset(row[0], "price", float(row[2]))
            db.hset(row[0], "imgUrl", row[3])

if start < 4:
    print("4.Customer...")
    with open("./DATA/Customer/person_0_0.csv", "r") as f:
        next(f)
        reader = csv.reader(f, quotechar='"', delimiter='|',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # personId | firstName | lastName | gender | birthday | creationDate | locationIP | browserUsed | place
            db.hset(row[0], "firstName", row[1])
            db.hset(row[0], "lastName", row[2])
            db.hset(row[0], "gender", row[3])
            db.hset(row[0], "birthday", parse(row[4]).timestamp())
            db.hset(row[0], "creationDate", parse(row[5]).timestamp())
            db.hset(row[0], "locationIP", row[6])
            db.hset(row[0], "browserUsed", row[7])
            db.hset(row[0], "place", int(row[8]))

if start < 5:
    print("5.Vendor...")
    with open("./DATA/Vendor/Vendor.csv", "r") as f:
        next(f)
        reader = csv.reader(f, quotechar='"', delimiter=',',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # Vendor | Country | Industry
            db.hset(row[0], "Country", row[1])
            db.hset(row[0], "Industry", row[2])

if start < 6:
    print("6.Order...")
    df = pd.read_json('./DATA/Order/Order.json', lines=True)
    for index, row in df.iterrows():
        OrderId = row["OrderId"]
        PersonId = row["PersonId"]
        db.rpush(f"{PersonId}_Orders", OrderId)
        db.hset(OrderId, "PersonId", row["PersonId"])
        # On peut pas faire parse(row["OrderDate"]).timestamp() car il y a des 29 fevriers...
        db.hset(OrderId, "OrderDate", row["OrderDate"])
        db.hset(OrderId, "TotalPrice", float(row["TotalPrice"]))

        asins = []
        for product in row["Orderline"]:
            asins.append(product["asin"])
        db.rpush(f"{OrderId}_Orderline", *asins)
