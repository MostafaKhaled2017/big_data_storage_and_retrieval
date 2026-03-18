#!/usr/bin/env python3
"""Run assignment data analysis queries on MongoDB."""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from bson.decimal128 import Decimal128
from pymongo import MongoClient
from pymongo.errors import OperationFailure


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output"


def log(message: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [to_jsonable(v) for v in value]
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def q1_pipeline() -> list[dict[str, Any]]:
    return [
        {
            "$lookup": {
                "from": "campaigns",
                "localField": "campaign_ref.campaign_key",
                "foreignField": "_id",
                "as": "campaign",
            }
        },
        {"$unwind": "$campaign"},
        {
            "$match": {"user_id": {"$ne": None}}
        },
        {
            "$group": {
                "_id": {
                    "campaign_id": "$campaign.campaign_id",
                    "message_type": "$campaign.campaign_type",
                    "channel": "$campaign.channel",
                    "topic": "$campaign.topic",
                    "user_id": "$user_id",
                },
                "did_purchase": {
                    "$max": {"$cond": [{"$eq": ["$engagement.is_purchased", True]}, 1, 0]}
                },
            }
        },
    ]


def run_q1(db: Any) -> list[dict[str, Any]]:
    log("q1: ensuring helper indexes.")
    try:
        db.messages.create_index(
            [("campaign_ref.campaign_key", 1), ("user_id", 1), ("engagement.is_purchased", 1)],
            name="idx_q1_campaign_key_user_purchase",
        )
        db.friendships.create_index([("user_id", 1), ("friend_id", 1)], name="idx_q1_friend_u_f")
        db.friendships.create_index([("friend_id", 1), ("user_id", 1)], name="idx_q1_friend_f_u")
    except Exception:
        # If index exists with different options or user has limited rights, continue.
        pass

    log("q1: building campaign-user purchase table.")
    cursor = db.messages.aggregate(q1_pipeline(), allowDiskUse=True, batchSize=20000)

    stats: dict[tuple[Any, Any, Any, Any], dict[str, Any]] = {}
    purchasers_by_campaign: dict[tuple[Any, Any], set[str]] = defaultdict(set)
    purchased_rows: list[tuple[tuple[Any, Any, Any, Any], tuple[Any, Any], str]] = []

    count_rows = 0
    for doc in cursor:
        count_rows += 1
        _id = doc["_id"]
        campaign_id = _id.get("campaign_id")
        message_type = _id.get("message_type")
        channel = _id.get("channel")
        topic = _id.get("topic")
        user_id = _id.get("user_id")
        did_purchase = int(doc.get("did_purchase", 0))

        key = (campaign_id, message_type, channel, topic)
        campaign_key = (campaign_id, message_type)

        if key not in stats:
            stats[key] = {
                "campaign_id": campaign_id,
                "message_type": message_type,
                "channel": channel,
                "topic": topic,
                "recipients": 0,
                "purchasers": 0,
                "social_purchasers": 0,
            }

        stats[key]["recipients"] += 1
        if did_purchase == 1 and user_id is not None:
            stats[key]["purchasers"] += 1
            purchasers_by_campaign[campaign_key].add(str(user_id))
            purchased_rows.append((key, campaign_key, str(user_id)))

    log(f"q1: campaign-user table ready ({count_rows} rows).")
    log("q1: loading friendship graph into memory.")
    neighbors: dict[str, set[str]] = defaultdict(set)
    for edge in db.friendships.find({}, {"_id": 0, "user_id": 1, "friend_id": 1}, batch_size=50000):
        u = edge.get("user_id")
        v = edge.get("friend_id")
        if u is None or v is None:
            continue
        us = str(u)
        vs = str(v)
        if us == vs:
            continue
        neighbors[us].add(vs)
        neighbors[vs].add(us)
    log(f"q1: friendship graph loaded ({len(neighbors)} users with at least one friend).")

    log("q1: computing social purchaser counts.")
    for key, campaign_key, user_id in purchased_rows:
        user_neighbors = neighbors.get(user_id)
        if not user_neighbors:
            continue
        if user_neighbors.intersection(purchasers_by_campaign[campaign_key]):
            stats[key]["social_purchasers"] += 1

    rows: list[dict[str, Any]] = []
    for row in stats.values():
        recipients = int(row["recipients"])
        purchasers = int(row["purchasers"])
        social_purchasers = int(row["social_purchasers"])
        conversion = round((purchasers / recipients) * 100.0, 2) if recipients else 0.0
        support = round((social_purchasers / purchasers) * 100.0, 2) if purchasers else 0.0

        cid = row["campaign_id"]
        try:
            sort_campaign = int(cid)
        except Exception:
            sort_campaign = 0

        rows.append(
            {
                "campaign_id": cid,
                "message_type": row["message_type"],
                "channel": row["channel"],
                "topic": row["topic"],
                "recipients": recipients,
                "purchasers": purchasers,
                "conversion_rate_pct": conversion,
                "social_purchasers": social_purchasers,
                "social_support_share_pct": support,
                "_sort_campaign_id": sort_campaign,
            }
        )

    rows.sort(key=lambda r: (-r["conversion_rate_pct"], -r["purchasers"], -r["_sort_campaign_id"]))
    top_rows = rows[:20]
    for row in top_rows:
        row.pop("_sort_campaign_id", None)
    return top_rows


def run_q2(db: Any, target_user_limit: int = 100) -> list[dict[str, Any]]:
    log(f"q2: selecting top {target_user_limit} active users from events.")
    target_users = [
        doc["_id"]
        for doc in db.events.aggregate(
            [
                {"$group": {"_id": "$user_id", "activity": {"$sum": 1}}},
                {"$sort": {"activity": -1}},
                {"$limit": target_user_limit},
            ],
            allowDiskUse=True,
        )
    ]
    if not target_users:
        log("q2: no target users found.")
        return []

    log(f"q2: found {len(target_users)} target users, running recommendation aggregation.")
    pipeline: list[dict[str, Any]] = [
        {"$match": {"user_id": {"$in": target_users}}},
        {"$project": {"_id": 0, "target_user_id": "$user_id", "friend_user_id": "$friend_id"}},
        {
            "$unionWith": {
                "coll": "friendships",
                "pipeline": [
                    {"$match": {"friend_id": {"$in": target_users}}},
                    {"$project": {"_id": 0, "target_user_id": "$friend_id", "friend_user_id": "$user_id"}},
                ],
            }
        },
        {
            "$lookup": {
                "from": "events",
                "localField": "friend_user_id",
                "foreignField": "user_id",
                "as": "friend_events",
            }
        },
        {"$unwind": "$friend_events"},
        {"$match": {"friend_events.event_type": {"$in": ["view", "cart", "purchase"]}}},
        {
            "$group": {
                "_id": {
                    "target_user_id": "$target_user_id",
                    "product_id": "$friend_events.product_id",
                },
                "weighted_score": {
                    "$sum": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": ["$friend_events.event_type", "purchase"]}, "then": 3},
                                {"case": {"$eq": ["$friend_events.event_type", "cart"]}, "then": 2},
                            ],
                            "default": 1,
                        }
                    }
                },
                "interactions": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "events",
                "let": {"uid": "$_id.target_user_id", "pid": "$_id.product_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$user_id", "$$uid"]},
                                    {"$eq": ["$product_id", "$$pid"]},
                                ]
                            }
                        }
                    },
                    {"$limit": 1},
                ],
                "as": "own_events",
            }
        },
        {"$match": {"own_events": {"$size": 0}}},
        {"$lookup": {"from": "products", "localField": "_id.product_id", "foreignField": "_id", "as": "product"}},
        {"$unwind": {"path": "$product", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "_id": 0,
                "target_user_id": "$_id.target_user_id",
                "product_id": "$_id.product_id",
                "weighted_score": 1,
                "interactions": 1,
                "category_code": "$product.category_code",
                "brand": "$product.brand",
            }
        },
        {"$sort": {"target_user_id": 1, "weighted_score": -1, "interactions": -1, "product_id": 1}},
        {
            "$group": {
                "_id": "$target_user_id",
                "rows": {
                    "$push": {
                        "product_id": "$product_id",
                        "weighted_score": "$weighted_score",
                        "interactions": "$interactions",
                        "category_code": "$category_code",
                        "brand": "$brand",
                    }
                },
            }
        },
        {"$project": {"rows": {"$slice": ["$rows", 5]}}},
        {"$unwind": {"path": "$rows", "includeArrayIndex": "rank"}},
        {
            "$project": {
                "_id": 0,
                "target_user_id": "$_id",
                "product_id": "$rows.product_id",
                "weighted_score": "$rows.weighted_score",
                "interactions": "$rows.interactions",
                "category_code": "$rows.category_code",
                "brand": "$rows.brand",
                "rank_in_user": {"$add": ["$rank", 1]},
            }
        },
        {"$sort": {"target_user_id": 1, "rank_in_user": 1}},
    ]
    rows = list(db.friendships.aggregate(pipeline, allowDiskUse=True))
    log("q2: aggregation finished.")
    return rows


def build_keywords_from_q2(q2_rows: list[dict[str, Any]], top_n: int = 3) -> list[str]:
    counter: Counter[str] = Counter()
    for row in q2_rows:
        category_code = (row.get("category_code") or "").lower()
        for token in re.split(r"[^a-z0-9]+", category_code):
            if len(token) > 2:
                counter[token] += 1
    return [token for token, _ in counter.most_common(top_n)]


def run_q3(db: Any, keywords: list[str]) -> list[dict[str, Any]]:
    if not keywords:
        log("q3: no keywords extracted from q2; returning empty result.")
        return []

    results: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    try:
        log("q3: ensuring text index on products (category_code, brand).")
        db.products.create_index(
            [("category_code", "text"), ("brand", "text")],
            name="idx_products_fulltext",
            default_language="none",
        )
        use_text_search = True
    except Exception:
        log("q3: text index unavailable; falling back to regex search.")
        use_text_search = False

    for keyword in keywords:
        log(f"q3: searching products for keyword '{keyword}'.")
        if use_text_search:
            try:
                cursor = (
                    db.products.find(
                        {"$text": {"$search": keyword}},
                        {
                            "_id": 1,
                            "category_code": 1,
                            "brand": 1,
                            "score": {"$meta": "textScore"},
                        },
                    )
                    .sort([("score", {"$meta": "textScore"})])
                    .limit(20)
                )
                docs = list(cursor)
            except OperationFailure:
                docs = list(
                    db.products.find(
                        {"category_code": {"$regex": re.escape(keyword), "$options": "i"}},
                        {"_id": 1, "category_code": 1, "brand": 1},
                    ).limit(20)
                )
        else:
            docs = list(
                db.products.find(
                    {"category_code": {"$regex": re.escape(keyword), "$options": "i"}},
                    {"_id": 1, "category_code": 1, "brand": 1},
                ).limit(20)
            )

        for doc in docs:
            key = (keyword, str(doc.get("_id")))
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "keyword": keyword,
                    "product_id": doc.get("_id"),
                    "category_code": doc.get("category_code"),
                    "brand": doc.get("brand"),
                    "text_score": doc.get("score"),
                }
            )

    results.sort(key=lambda r: (r["keyword"], -(r["text_score"] or 0), str(r["product_id"])))
    return results


def time_query(name: str, fn: Any, heartbeat_seconds: int = 30) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    started = time.perf_counter()
    stop_event = threading.Event()

    def heartbeat() -> None:
        while not stop_event.wait(heartbeat_seconds):
            elapsed = time.perf_counter() - started
            log(f"[{name}] still running... {elapsed:.1f} sec elapsed")

    worker = threading.Thread(target=heartbeat, daemon=True)
    worker.start()
    try:
        rows = fn()
    finally:
        stop_event.set()
        worker.join(timeout=1)
    elapsed = time.perf_counter() - started
    print(f"\n[{name}]", flush=True)
    print(f"Execution time: {elapsed:.4f} sec", flush=True)
    print(f"Rows returned : {len(rows)}", flush=True)
    preview = rows[:5]
    if preview:
        print("Preview:", flush=True)
        print(json.dumps(to_jsonable(preview), indent=2, ensure_ascii=True), flush=True)
    return (
        {
            "execution_time_sec": elapsed,
            "row_count": len(rows),
            "rows": to_jsonable(rows),
        },
        rows,
    )


def main() -> None:
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("TARGET_DB", "customer_campaign_analytics")
    heartbeat_seconds = int(os.getenv("MONGO_PROGRESS_SECONDS", "30"))

    log("Running MongoDB analysis")
    log(f"DB: {db_name} @ {mongo_uri}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with MongoClient(mongo_uri, serverSelectionTimeoutMS=10000) as client:
        log("Checking MongoDB connection with ping.")
        client.admin.command("ping")
        log("MongoDB ping OK.")
        db = client[db_name]
        required = ["users", "clients", "products", "events", "campaigns", "messages", "friendships"]
        for coll in required:
            count = db[coll].estimated_document_count()
            log(f"Collection '{coll}': {count} docs")

        q1_result, _ = time_query("q1", lambda: run_q1(db), heartbeat_seconds=heartbeat_seconds)
        q2_result, q2_rows = time_query("q2", lambda: run_q2(db), heartbeat_seconds=heartbeat_seconds)

        keywords = build_keywords_from_q2(q2_rows)
        log(f"[q3] Keywords from q2 products: {keywords}")
        q3_result, _ = time_query("q3", lambda: run_q3(db, keywords), heartbeat_seconds=heartbeat_seconds)

    output_payload = {
        "database": "mongodb",
        "connection": {
            "uri": mongo_uri,
            "database": db_name,
        },
        "queries": {
            "q1": q1_result,
            "q2": q2_result,
            "q3": q3_result,
        },
    }
    out_path = OUTPUT_DIR / "analysis_mongodb.json"
    out_path.write_text(json.dumps(output_payload, indent=2, ensure_ascii=True), encoding="utf-8")
    log(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
