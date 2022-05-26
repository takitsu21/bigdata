import redis
import csv

db = redis.Redis(host='localhost', port=6379, db=0)

db.flushdb()
db.flushall()

print("Feedback...")
with open("./DATA/Feedback/Feedback.csv", "r") as f:
    reader = csv.reader(f, quotechar='"', delimiter='|',
                        quoting=csv.QUOTE_ALL, skipinitialspace=True)
    for row in reader:
        # asin PersonId feedback
        db.hset(row[0], row[1], row[2])


print("BrandByProduct...")
with open("./DATA/Product/BrandByProduct.csv", "r") as f:
    reader = csv.reader(f, quotechar='"', delimiter=',',
                        quoting=csv.QUOTE_ALL, skipinitialspace=True)
    for row in reader:
        # brand asin
        db.hset(row[1], "brand", row[0])

print("Product...")
with open("./DATA/Product/Product.csv", "r") as f:
    next(f)
    reader = csv.reader(f, quotechar='"', delimiter=',',
                        quoting=csv.QUOTE_ALL, skipinitialspace=True)
    for row in reader:
        # asin title price imgUrl
        db.hset(row[0], "title", row[1])
        db.hset(row[0], "price", float(row[2]))
        db.hset(row[0], "imgUrl", row[3])
