import xml.etree.ElementTree as ET
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
        Customers = []
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
            db.hset(row[0], "place", float(row[8]))
            Customers.append(row[0])
        db.rpush("Customers", *Customers)

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
    Orders = []
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
        Orders.append(OrderId)
    db.rpush("Orders", *Orders)


if start < 7:
    print("7.Invoice...")
    tree = ET.parse(open("./DATA/Invoice/Invoice.xml", "r"))
    for invoice in tree.findall('Invoice.xml'):
        OrderId = invoice.find("OrderId").text
        db.hset(OrderId, "PersonId",
                invoice.find("PersonId").text)
        db.hset(OrderId, "OrderDate", invoice.find("OrderDate").text)
        db.hset(OrderId, "TotalPrice",
                float(invoice.find("TotalPrice").text))

        asins = []
        for Orderline in invoice.findall("Orderline"):
            asins.append(Orderline.find("asin").text)
        db.rpush(f"{OrderId}_Orderline", *asins)

if start < 8:
    print("8.person_hasInterest_tag_0_0.csv...")
    with open("./DATA/SocialNetwork/person_hasInterest_tag_0_0.csv", "r") as f:
        next(f)
        reader = csv.reader(f, quotechar='"', delimiter='|',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # Person.id | Tag.id
            db.rpush(f"{row[0]}_Tags", row[1])

if start < 9:
    print("9.person_knows_person_0_0...")
    with open("./DATA/SocialNetwork/person_knows_person_0_0.csv", "r") as f:
        next(f)
        reader = csv.reader(f, quotechar='"', delimiter='|',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # Person.id | Person.id | creationDate
            db.rpush(f"{row[0]}_Knows", row[1])

if start < 10:
    print("10.post_0_0...")
    for chunk in pd.read_csv("./DATA/SocialNetwork/post_0_0.csv", sep="|", chunksize=20000):
        for index, row in chunk.iterrows():
            # id | imageFile | creationDate | locationIP | browserUsed | language | content | length
            db.hset(row[0], "imageFile", row[1])
            db.hset(row[0], "creationDate", parse(row[2]).timestamp())
            db.hset(row[0], "locationIP", row[3])
            db.hset(row[0], "browserUsed", row[4])
            db.hset(row[0], "language", row[5])
            db.hset(row[0], "content", row[6])
            db.hset(row[0], "length", float(row[7]))


if start < 11:
    print("11.post_hasCreator_person_0_0...")
    with open("./DATA/SocialNetwork/post_hasCreator_person_0_0.csv", "r") as f:
        next(f)
        reader = csv.reader(f, quotechar='"', delimiter='|',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # Post.id | Person.id
            db.rpush(f"{row[1]}_Posts", row[0])

if start < 12:
    print("12.post_hasTag_tag_0_0...")
    with open("./DATA/SocialNetwork/post_hasTag_tag_0_0.csv", "r") as f:
        next(f)
        reader = csv.reader(f, quotechar='"', delimiter='|',
                            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            # Post.id | Tag.id
            db.rpush(f"{row[0]}_Tags", row[1])
