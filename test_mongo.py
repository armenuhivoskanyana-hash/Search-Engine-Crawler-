import pymongo

# Replace with your actual connection string
MONGODB_CONNECTION = "mongodb+srv://armine_voskanyan1_db:Ami%406799@crawler.hachyvv.mongodb.net/?retryWrites=true&w=majority&appName=crawler&ssl=true&ssl_cert_reqs=CERT_NONE"

try:
    client = pymongo.MongoClient(MONGODB_CONNECTION)
    client.admin.command('ping')
    print("✓ MongoDB connection successful!")
except Exception as e:
    print(f"✗ MongoDB connection failed: {e}")