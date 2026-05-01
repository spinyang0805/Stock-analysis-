import os
import json
from google.cloud import firestore
from google.oauth2 import service_account

# Read service account JSON from env
FIREBASE_KEY = os.getenv("FIREBASE_KEY")

try:
    if FIREBASE_KEY:
        info = json.loads(FIREBASE_KEY)
        credentials = service_account.Credentials.from_service_account_info(info)
        db = firestore.Client(credentials=credentials, project=info.get("project_id"))
    else:
        db = None
except Exception as e:
    print("Firebase init error:", e)
    db = None
