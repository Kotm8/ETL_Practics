from pymongo import AsyncMongoClient
from app.core.config import MONGO_INITDB_DATABASE, MONGODB_URL

client = AsyncMongoClient(MONGODB_URL)
db = client[MONGO_INITDB_DATABASE]
