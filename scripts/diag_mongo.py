from pymongo import MongoClient
import sys

def test_connection(username, password, auth_db, port):
    uri = f"mongodb://{username}:{password}@localhost:{port}/?authSource={auth_db}"
    print(f"Testing: {uri}")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        print("SUCCESS!")
        return True
    except Exception as e:
        print(f"FAILED: {e}")
        return False

combinations = [
    ("user", "pass", "admin", 27017),
    ("admin", "admin", "admin", 27017),
    ("root", "root", "admin", 27017),
    ("user", "pass", "bird_db", 27017),
    ("admin", "admin_pass", "admin", 27017),
    ("", "", "admin", 27017) # No auth
]

for u, p, a, pt in combinations:
    if test_connection(u, p, a, pt):
        print(f"\nFOUND WORKING CONFIG: {u}:{p} on {pt} (authSource={a})")
        sys.exit(0)

print("\nNo working configuration found with common variants.")
sys.exit(1)
