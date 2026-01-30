from pymongo import MongoClient
from datetime import datetime
 
# Connect to MongoDB
client = MongoClient('mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/')
db = client['verifone']
collection = db['logs']
collection_cpjr = db['cpjr']
collection_dr = db['day_reports']
collection_dm = db['deptmerge']
collection_gls = db['gblivescan']
collection_gms = db['gbmopsales']
collection_gsd = db['gbscandata']
collection_gball = db['gbsummaryall']
collection_ism = db['ism']
collection_itg_weekly = db['itg_weekly']
collection_livesales = db['livesales']
collection_mcm = db['mcm']
collection_merge = db['merge']
collection_msm = db['msm']
collection_scan_data = db['scan_data']
collection_vfdailysales = db['vfdailysales']
collection_vflivescan = db['vflivescan']
collection_vfsummaryall = db['vfsummaryall']
collection_weekly = db['weekly']

# Define the storeid to match (from your screenshot)
# Note : No need of configuring the storeid
storeid = "65d83ff360d8fb8e5b10b00d" # JB
# storeid = "66337c59c31dc3e37229f275" # SAM
# storeid = "66337debc31dc3e37229f27a" # Vishal
# Define Cut-Off date
cutoff_date = datetime.strptime("2025-11-30", "%Y-%m-%d")
 
# Find the document
doc = collection.find_one({'storeid': storeid})


# ------------------------Thread-1----------------------------------------

# Proceed only if document and Thread-1 exists
if doc and 'live_logs' in doc and 'Thread-1' in doc['live_logs']:
    thread_1_logs = doc['live_logs']['Thread-1']
    
    if thread_1_logs:
        # Sort the logs by timestamp descending (assuming each log has a 'timestamp' field)
        sorted_logs = sorted(thread_1_logs, key=lambda x: x.get('timestamp', ''), reverse=True)
        
        # Keep only the latest log
        latest_log = [sorted_logs[0]]
 
        # Update the document
        collection.update_one(
            {'_id': doc['_id']},
            {'$set': {'live_logs.Thread-1': latest_log}}
        )
 
        print("Thread-1 cleaned, only latest log retained.")
    else:
        print("Thread-1 is empty.")
else:
    print("Document or Thread-1 not found.")

# ------------------------Thread-2----------------------------------------

# Proceed only if document and Thread-2 exists
if doc and 'live_logs' in doc and 'Thread-2' in doc['live_logs']:
    thread_2_logs = doc['live_logs']['Thread-2']
    
    if thread_2_logs:
        # Sort the logs by timestamp descending (assuming each log has a 'timestamp' field)
        sorted_logs = sorted(thread_2_logs, key=lambda x: x.get('timestamp', ''), reverse=True)
        
        # Keep only the latest log
        latest_log = [sorted_logs[0]]
 
        # Update the document
        collection.update_one(
            {'_id': doc['_id']},
            {'$set': {'live_logs.Thread-2': latest_log}}
        )
 
        print("Thread-2 cleaned, only latest log retained.")
    else:
        print("Thread-2 is empty.")
else:
    print("Document or Thread-2 not found.")

# ------------------------collection_cpjr----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_cpjr.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# # -------------------------collection_dr---------------------------------------


# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_dr.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# # -------------------------deptmerge---------------------------------------


# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_dm.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")


# # ----------------------------gblivescan---------------------------------
# # Define the cutoff date
# cutoff = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents where SaleEvent.0.EventEndDate < cutoff
result = collection_gls.delete_many({
    "SaleEvent.0.EventEndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# # -------------------------gbmopsales---------------------------------------


# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_gms.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# # -------------------------------------gbscandata--------------------------------

# # Set cutoff date
# cutoff_date = "2025-03-01"
 
# Run update on all documents to pull matching array elements
result = collection_gsd.update_many(
    {},
    {
        "$pull": {
            "SaleEvent": {
                "EventEndDate": {"$lt": cutoff_date}
            }
        }
    }
)
 
print(f"Modified {result.modified_count} documents.")
 
# # ------------------------gbsummaryall------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_gball.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# # ------------------------ism------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_ism.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# # ------------------------itg_weekly------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_itg_weekly.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# # ----------------livesales----------------------------

# # Define cutoff date
# cutoff_date = "2025-04-01"
 
# Delete documents where the first SaleEvent's EventEndDate is older than cutoff
result = collection_livesales.delete_many({
    "SaleEvent.0.EventEndDate": {"$lt": cutoff_date}
})
 
print(f"Deleted {result.deleted_count} documents.")


# # -----------------------------mcm------------------------------
# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_mcm.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")



# # -----------------------------merge------------------------------
# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_merge.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")


# # -----------------------------msm------------------------------
# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_msm.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# --------------------------collection_scan_data------------------
# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with week_ending_date < cutoff
result = collection_scan_data.delete_many({
    "week_ending_date": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# ----------------------------collection_vfdailysales---------------
# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_vfdailysales.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# -------------------collection_vflivescan-------------------
# # Define cutoff date as string
# cutoff_date = "2025-03-01"
 
# Delete documents where SaleEvent.EventEndDate is before the cutoff
result = collection_vflivescan.delete_many({
    "SaleEvent.EventEndDate": {"$lt": cutoff_date}
})
 
print(f"Deleted {result.deleted_count} documents.")

# # ----------------------------collection_vfsummaryall---------------
# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_vfsummaryall.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")

# ----------------------------collection_weekly---------------
# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")
 
# Delete documents with EndDate < cutoff
result = collection_weekly.delete_many({
    "EndDate": {"$lt": cutoff_date.strftime("%Y-%m-%d")}
})
 
print(f"Deleted {result.deleted_count} documents.")


# ------------------------autoCollect----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["autoCollect"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:autoCollectPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------fpDispenser----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["fpDispenser"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:fpDispenserPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------pluPromo----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["pluPromo"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:pluPromoPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------summary----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["summary"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:summaryPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------hourly----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["hourly"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:hourlyPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------department----------------------------------------

from datetime import datetime

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["department"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:departmentPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------tax----------------------------------------

from datetime import datetime

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["tax"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:taxPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------popDisc----------------------------------------

from datetime import datetime

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["popDisc"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:popDiscPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------dcrStat----------------------------------------

from datetime import datetime

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["dcrStat"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:dcrstPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------netProd----------------------------------------

from datetime import datetime

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["netProd"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:netProdPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------send_item_to_pos----------------------------------------
from datetime import timezone

# Ensure cutoff_date is timezone-aware
cutoff_date_aware = cutoff_date.replace(tzinfo=timezone.utc)

# Prepare list of IDs to delete
ids_to_delete = []

for doc in db["send_item_to_pos"].find({}, {"_id": 1, "timeStamp": 1}):
    ts = doc.get("timeStamp")
    
    # Handle datetime or string timestamps
    if isinstance(ts, datetime):
        ts_dt = ts
    elif isinstance(ts, str):
        try:
            ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue  # Skip invalid strings
    else:
        continue  # Skip missing timestamp

    # Make timezone-aware if naive
    if ts_dt.tzinfo is None:
        ts_dt = ts_dt.replace(tzinfo=timezone.utc)

    # Compare with cutoff_date
    if ts_dt < cutoff_date_aware:
        ids_to_delete.append(doc["_id"])

# Delete all matching documents
if ids_to_delete:
    result = db["send_item_to_pos"].delete_many({"_id": {"$in": ids_to_delete}})
    print(f"Deleted {result.deleted_count} documents.")
else:
    print("No documents to delete.")


# ------------------------network----------------------------------------

from datetime import datetime

# Delete documents where periodBeginDate < cutoff
result = db["network"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:networkPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from network.")

# ------------------------blendProduct----------------------------------------

from datetime import datetime

# Delete documents where periodBeginDate < cutoff
result = db["blendProduct"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:blendPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from blendProduct.")

# ------------------------fpHose----------------------------------------

from datetime import datetime

# Delete documents where periodBeginDate < cutoff
result = db["fpHose"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:fpHosePd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from fpHose.")

# ------------------------allProd----------------------------------------

from datetime import datetime

# Delete documents where periodBeginDate < cutoff
result = db["allProd"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:allProdPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from allProd.")

# ------------------------slPriceLvl----------------------------------------

from datetime import datetime

# Delete documents where periodBeginDate < cutoff
result = db["slPriceLvl"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:slPriceLvlPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from slPriceLvl.")

# ------------------------plu----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["plu"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:pluPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------fpHoseRunning----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["fpHoseRunning"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:fpHoseRunningPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------deal----------------------------------------

# # Define cutoff date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

# Delete documents where periodBeginDate < cutoff
result = db["deal"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:dealPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------verifonels----------------------------------------

# Delete documents where BeginDate < cutoff
# cutoff_date is a datetime.date or datetime.datetime object
from datetime import datetime

# Example cutoff_date
# cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d")

result = db["verifonels"].delete_many({
    "$expr": {
        "$lt": [
            { "$dateFromString": { "dateString": "$BeginDate" } },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------tank----------------------------------------

# Delete documents where periodBeginDate < cutoff
# cutoff_date is a datetime.datetime object
result = db["tank"].delete_many({
    "$expr": {
        "$lt": [
            { "$toDate": "$pd:tankPd.vs:period.periodBeginDate" },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents.")

# ------------------------tankRec----------------------------------------

# Delete documents where periodBeginDate < cutoff_date
result = db["tankRec"].delete_many({
    "$expr": {
        "$lt": [
            { "$toDate": "$pd:tankRecPd.vs:period.periodBeginDate" },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from tankRec.")

# ------------------------fpHoseTest----------------------------------------

# Delete documents where periodBeginDate < cutoff_date
result = db["fpHoseTest"].delete_many({
    "$expr": {
        "$lt": [
            { "$toDate": "$pd:fptHosePd.vs:period.periodBeginDate" },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from fpHoseTest.")

# ------------------------prPriceLvl----------------------------------------

result = db["prPriceLvl"].delete_many({
    "$expr": {
        "$and": [
            { "$ne": ["$pd:prPriceLvlPd.vs:period.periodBeginDate", None] },
            { "$ne": ["$pd:prPriceLvlPd.vs:period.periodBeginDate", ""] },
            {
                "$lt": [
                    { "$toDate": "$pd:prPriceLvlPd.vs:period.periodBeginDate" },
                    cutoff_date
                ]
            }
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from prPriceLvl.")

# ------------------------auto_log----------------------------------------

from datetime import datetime

# Delete documents where weekendingdate < cutoff_date
# Make sure cutoff_date is a datetime object
result = db["auto_log"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$weekendingdate",
                    "format": "%Y-%m-%d"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from auto_log.")

# ------------------------tankMonitor----------------------------------------

from datetime import datetime

# Example cutoff date
# cutoff_date = datetime.strptime("2024-01-01", "%Y-%m-%d")

result = db["tankMonitor"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:tankMonitorPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from tankMonitor.")

# ------------------------tierProduct----------------------------------------

from datetime import datetime

# Example cutoff date
# cutoff_date = datetime.strptime("2024-01-01", "%Y-%m-%d")

result = db["tierProduct"].delete_many({
    "$expr": {
        "$lt": [
            {
                "$dateFromString": {
                    "dateString": "$pd:tierProductPd.vs:period.periodBeginDate"
                }
            },
            cutoff_date
        ]
    }
})

print(f"Deleted {result.deleted_count} documents from tierProduct.")

# ------------------------livesales----------------------------------------

cutoff_str = cutoff_date.strftime("%Y-%m-%d")

# Delete documents where any VoidEvent, RefundEvent, or SaleEvent is older than cutoff
result = db["livesales"].delete_many({
    "$or": [
        {"VoidEvent": {"$elemMatch": {"EventStartDate": {"$lt": cutoff_str}}}},
        {"RefundEvent": {"$elemMatch": {"EventStartDate": {"$lt": cutoff_str}}}},
        {"SaleEvent": {"$elemMatch": {"EventStartDate": {"$lt": cutoff_str}}}}
    ]
})

print(f"Deleted {result.deleted_count} documents from livesales.")


# ------------------------vflivescan----------------------------------------

cutoff_str = cutoff_date.strftime("%Y-%m-%d")

# Delete documents where SaleEvent.EventStartDate is older than cutoff
result = db["vflivescan"].delete_many({
    "SaleEvent.EventStartDate": {"$lt": cutoff_str}
})

print(f"Deleted {result.deleted_count} documents from vflivescan.")
