from pymongo import MongoClient

# MongoDB connection
client = MongoClient(
    "mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/"
)

# Select database
db = client["verifone"]

print("\n📊 Verifone DB – Collection-wise Record Count\n")

total_docs = 0

# Get all collection names
collections = db.list_collection_names()

for col_name in collections:
    collection = db[col_name]

    # Fast & accurate count
    count = collection.count_documents({})

    print(f"📁 {col_name:<25} → {count:,} records")
    total_docs += count

print("\n" + "-" * 50)
print(f"📦 TOTAL RECORDS IN DB → {total_docs:,}")
print("-" * 50)
