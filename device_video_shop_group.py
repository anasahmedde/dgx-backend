# device_video_shop_group.py
# Run: uvicorn device_video_shop_group:app --host 0.0.0.0 --port 8005 --reload
# Enhanced with: rotation, fit_mode, content_type, logs, counter reset, online threshold
# FIX: Add GET /video/{video_name} and GET /video/{video_name}/presign for dashboard preview

import os
import io
import csv
from contextlib import contextmanager
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple

from migrations.dvsg_schema import ensure_dvsg_schema
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse
from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import ThreadedConnectionPool, PoolError
import boto3

load_dotenv()

# ---------- Settings ----------
DB_HOST = os.getenv("PGHOST", "172.31.17.177")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "dgx")
DB_USER = os.getenv("PGUSER", "app_user")
DB_PASS = os.getenv("PGPASSWORD", "strongpassword")
DB_MIN_CONN = int(os.getenv("PG_MIN_CONN", "1"))
DB_MAX_CONN = int(os.getenv("PG_MAX_CONN", "50"))

AWS_REGION = os.getenv("AWS_REGION")
PRESIGN_EXPIRES = int(os.getenv("PRESIGN_EXPIRES", "900"))  # 15 min

# Online threshold in seconds (1 minute)
ONLINE_THRESHOLD_SECONDS = 60

# Treat any of these as "no group"
NO_GROUP_SENTINELS = {"_none", "none", "null", "(none)", ""}

app = FastAPI(title="Device-Video-Shop-Group Service", version="3.0.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


# ---------- Pydantic models ----------
class DeviceCountsOut(BaseModel):
    mobile_id: str
    daily_count: Optional[int] = None
    monthly_count: Optional[int] = None


class DeviceTemperatureUpdateIn(BaseModel):
    temperature: float


class DeviceDailyCountUpdateIn(BaseModel):
    daily_count: int


class DeviceMonthlyCountUpdateIn(BaseModel):
    monthly_count: int


class LinkCreate(BaseModel):
    mobile_id: str
    video_name: str
    shop_name: str
    gname: Optional[str] = None
    display_order: int = 0


class LinkOut(BaseModel):
    id: int
    did: int
    vid: int
    sid: int
    gid: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    mobile_id: str
    video_name: str
    s3_link: Optional[str] = None
    shop_name: str
    gname: Optional[str] = None
    is_online: Optional[bool] = None
    temperature: Optional[float] = None
    daily_count: Optional[int] = None
    monthly_count: Optional[int] = None
    rotation: Optional[int] = 0
    content_type: Optional[str] = "video"
    fit_mode: Optional[str] = "cover"
    display_duration: Optional[int] = 10
    display_order: Optional[int] = 0


class PresignedURLItem(BaseModel):
    link_id: int
    video_name: str
    url: str
    expires_in: int
    filename: str
    rotation: int = 0
    content_type: str = "video"
    fit_mode: str = "cover"
    display_duration: int = 10
    display_order: int = 0


class PresignedURLListOut(BaseModel):
    mobile_id: str
    items: List[PresignedURLItem]
    count: int


class DeviceDownloadStatusOut(BaseModel):
    mobile_id: str
    download_status: bool
    total_links: int
    downloaded_count: int


class DeviceDownloadStatusSetIn(BaseModel):
    status: bool


class GroupVideoUpdateIn(BaseModel):
    video_name: str


class GroupVideoUpdateOut(BaseModel):
    gid: Optional[int] = None
    gname: Optional[str] = None
    vid: int
    video_name: str
    inserted_count: int
    deleted_count: int
    remaining_count: int
    devices_marked: int


class GroupVideosUpdateIn(BaseModel):
    video_names: List[str]


class GroupVideosUpdateOut(BaseModel):
    gid: Optional[int] = None
    gname: Optional[str] = None
    vids: List[int]
    video_names: List[str]
    inserted_count: int
    deleted_count: int
    updated_count: int
    devices_marked: int


class DeviceOnlineStatusOut(BaseModel):
    mobile_id: str
    is_online: bool


class DeviceOnlineUpdateIn(BaseModel):
    is_online: bool = True


class DeviceCreateIn(BaseModel):
    mobile_id: str
    group_name: Optional[str] = None
    shop_name: Optional[str] = None


class VideoRotationUpdateIn(BaseModel):
    rotation: int = Field(..., ge=0, le=270)


class VideoFitModeUpdateIn(BaseModel):
    fit_mode: str


# NEW: Video metadata output (used by GET /video/{video_name})
class VideoOut(BaseModel):
    id: int
    video_name: str
    s3_link: Optional[str] = None
    rotation: int = 0
    content_type: str = "video"
    fit_mode: str = "cover"
    display_duration: int = 10
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # optional helpers for preview
    url: Optional[str] = None
    presigned_url: Optional[str] = None
    expires_in: Optional[int] = None
    filename: Optional[str] = None


# ---------- PG pool ----------
pg_pool: Optional[ThreadedConnectionPool] = None


def pg_init_pool():
    global pg_pool
    if pg_pool is None:
        pg_pool = ThreadedConnectionPool(
            DB_MIN_CONN, DB_MAX_CONN,
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS, connect_timeout=5,
        )


def pg_close_pool():
    global pg_pool
    if pg_pool:
        pg_pool.closeall()
        pg_pool = None


@contextmanager
def pg_conn():
    global pg_pool
    if pg_pool is None:
        pg_init_pool()
    conn = None
    try:
        try:
            conn = pg_pool.getconn()
        except PoolError:
            raise HTTPException(status_code=503, detail="Database connection pool exhausted")
        yield conn
    finally:
        if conn is not None:
            if pg_pool is not None:
                try:
                    pg_pool.putconn(conn)
                except PoolError:
                    try:
                        conn.close()
                    except:
                        pass
            else:
                try:
                    conn.close()
                except:
                    pass


# ---------- Counter reset logic ----------
def _check_and_reset_counters(conn, did: int) -> Tuple[bool, bool]:
    """Check and reset daily/monthly counters if needed. Returns (daily_reset, monthly_reset)."""
    today = date.today()
    month_start = today.replace(day=1)
    daily_reset = False
    monthly_reset = False

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_daily_reset, last_monthly_reset
            FROM public.device WHERE id = %s;
            """,
            (did,)
        )
        row = cur.fetchone()
        if not row:
            return False, False

        last_daily = row[0]
        last_monthly = row[1]

        updates = []
        params = []

        # Reset daily if last reset was before today
        if last_daily is None or last_daily < today:
            updates.append("daily_count = 0")
            updates.append("last_daily_reset = %s")
            params.append(today)
            daily_reset = True

        # Reset monthly if last reset was before this month
        if last_monthly is None or last_monthly < month_start:
            updates.append("monthly_count = 0")
            updates.append("last_monthly_reset = %s")
            params.append(month_start)
            monthly_reset = True

        if updates:
            params.append(did)
            cur.execute(
                f"UPDATE public.device SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s;",
                tuple(params)
            )

    return daily_reset, monthly_reset


def _log_event(conn, did: int, log_type: str, value: float = None):
    """Log an event (temperature or door_open) to device_logs table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.device_logs (did, log_type, value)
            VALUES (%s, %s, %s);
            """,
            (did, log_type, value)
        )


# ---------- SQL helpers ----------
READ_JOIN_SQL = """
SELECT l.id, l.did, l.vid, l.sid, l.gid, l.created_at, l.updated_at,
       d.mobile_id, d.is_online,
       d.temperature, d.daily_count, d.monthly_count,
       v.video_name, v.s3_link, s.shop_name, g.gname,
       v.rotation, v.content_type, v.fit_mode, v.display_duration,
       l.display_order
FROM public.device_video_shop_group l
JOIN public.device d ON d.id = l.did
JOIN public.video  v ON v.id = l.vid
JOIN public.shop   s ON s.id = l.sid
LEFT JOIN public."group" g ON g.id = l.gid
"""


def _row_to_link_dict(row) -> Dict[str, Any]:
    temp_val = row[9]
    if temp_val is not None:
        temp_val = float(temp_val)
    return {
        "id": row[0],
        "did": row[1],
        "vid": row[2],
        "sid": row[3],
        "gid": row[4],
        "created_at": row[5],
        "updated_at": row[6],
        "mobile_id": row[7],
        "is_online": row[8],
        "temperature": temp_val,
        "daily_count": row[10],
        "monthly_count": row[11],
        "video_name": row[12],
        "s3_link": row[13],
        "shop_name": row[14],
        "gname": row[15],
        "rotation": row[16] if len(row) > 16 else 0,
        "content_type": row[17] if len(row) > 17 else "video",
        "fit_mode": row[18] if len(row) > 18 else "cover",
        "display_duration": row[19] if len(row) > 19 else 10,
        "display_order": row[20] if len(row) > 20 else 0,
    }


def get_device_id_by_mobile(conn, mobile_id: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;",
            (mobile_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None


def fetch_link_by_id(conn, link_id: int) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(READ_JOIN_SQL + " WHERE l.id = %s;", (link_id,))
        row = cur.fetchone()
        return _row_to_link_dict(row) if row else None


def fetch_links_for_mobile(conn, mobile_id: str, limit: int, offset: int) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            READ_JOIN_SQL + """
            WHERE d.mobile_id = %s
            ORDER BY l.display_order ASC, l.updated_at DESC, l.id DESC
            LIMIT %s OFFSET %s;
            """,
            (mobile_id, limit, offset),
        )
        rows = cur.fetchall()
        return [_row_to_link_dict(r) for r in rows]


def list_links(conn, mobile_id, video_name, shop_name, gname, did, vid, sid, gid, limit, offset):
    where, params = [], []
    if mobile_id:
        where.append("d.mobile_id ILIKE %s")
        params.append(f"%{mobile_id}%")
    if video_name:
        where.append("v.video_name ILIKE %s")
        params.append(f"%{video_name}%")
    if shop_name:
        where.append("s.shop_name ILIKE %s")
        params.append(f"%{shop_name}%")
    if gname:
        where.append("g.gname ILIKE %s")
        params.append(f"%{gname}%")
    if did is not None:
        where.append("l.did = %s")
        params.append(did)
    if vid is not None:
        where.append("l.vid = %s")
        params.append(vid)
    if sid is not None:
        where.append("l.sid = %s")
        params.append(sid)
    if gid is not None:
        where.append("l.gid = %s")
        params.append(gid)

    sql = READ_JOIN_SQL + (" WHERE " + " AND ".join(where) if where else "")
    sql += " ORDER BY l.display_order ASC, l.id DESC LIMIT %s OFFSET %s;"
    params.extend([limit, offset])

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        return [_row_to_link_dict(r) for r in rows]


def delete_link_row(conn, link_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.device_video_shop_group WHERE id = %s RETURNING id;",
            (link_id,),
        )
        rows = cur.fetchall()
    return len(rows)


def _aggregate_from_links(conn, did: int) -> Tuple[int, int, bool]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int,
                   COALESCE(SUM(CASE WHEN dl_status THEN 1 ELSE 0 END)::int, 0)
            FROM public.device_video_shop_group
            WHERE did = %s;
            """,
            (did,),
        )
        total, done = cur.fetchone()
        return total, done, (total > 0 and done == total)


def _write_device_flag(conn, did: int, value: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.device
               SET download_status = %s, updated_at = NOW()
             WHERE id = %s;
            """,
            (value, did),
        )


def _enforce_single_group_shop_for_device(conn, did: int, target_gid: Optional[int], target_sid: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.device_video_shop_group
             WHERE did = %s
               AND (gid IS DISTINCT FROM %s OR sid IS DISTINCT FROM %s)
            RETURNING id;
            """,
            (did, target_gid, target_sid),
        )
        return cur.rowcount or 0


# ---------- S3 helpers ----------
def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    if not s3_uri or not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")
    without = s3_uri[5:]
    parts = without.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Invalid S3 URI")
    return parts[0], parts[1]


def presign_get_object(s3_uri: str, expires_in: int = PRESIGN_EXPIRES) -> Tuple[str, str]:
    """
    Existing presign used for downloads (kept as-is).
    """
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
    filename = key.split("/")[-1] or "download.mp4"

    # Detect content type from extension
    ext = filename.lower().split('.')[-1] if '.' in filename else 'mp4'
    content_types = {
        'mp4': 'video/mp4', 'webm': 'video/webm', 'mov': 'video/quicktime',
        'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
        'gif': 'image/gif', 'webp': 'image/webp',
        'html': 'text/html', 'htm': 'text/html',
        'pdf': 'application/pdf',
    }
    content_type = content_types.get(ext, 'video/mp4')

    params = {
        "Bucket": bucket,
        "Key": key,
        "ResponseContentDisposition": f'attachment; filename="{filename}"',
        "ResponseContentType": content_type,
    }
    url = s3.generate_presigned_url("get_object", Params=params, ExpiresIn=expires_in)
    return url, filename


def presign_get_object_inline(s3_uri: str, expires_in: int = PRESIGN_EXPIRES) -> Tuple[str, str]:
    """
    NEW: Inline presign for preview/playback in browser.
    """
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
    filename = key.split("/")[-1] or "video.mp4"

    ext = filename.lower().split('.')[-1] if '.' in filename else 'mp4'
    content_types = {
        'mp4': 'video/mp4', 'webm': 'video/webm', 'mov': 'video/quicktime',
        'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
        'gif': 'image/gif', 'webp': 'image/webp',
        'html': 'text/html', 'htm': 'text/html',
        'pdf': 'application/pdf',
    }
    content_type = content_types.get(ext, 'video/mp4')

    params = {
        "Bucket": bucket,
        "Key": key,
        "ResponseContentDisposition": f'inline; filename="{filename}"',
        "ResponseContentType": content_type,
    }
    url = s3.generate_presigned_url("get_object", Params=params, ExpiresIn=expires_in)
    return url, filename


# NEW: video fetch helper (for GET /video/{video_name})
def fetch_video_by_name(conn, video_name: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, video_name, s3_link, rotation, content_type, fit_mode, display_duration, created_at, updated_at
            FROM public.video
            WHERE video_name = %s
            ORDER BY id DESC
            LIMIT 1;
            """,
            (video_name,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "video_name": row[1],
            "s3_link": row[2],
            "rotation": int(row[3] or 0),
            "content_type": row[4] or "video",
            "fit_mode": row[5] or "cover",
            "display_duration": int(row[6] or 10),
            "created_at": row[7],
            "updated_at": row[8],
        }


# ---------- Group video operations ----------
def _resolve_group_id(conn, gname: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            'SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;',
            (gname,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Group not found: {gname}")
        return int(row[0])


def _resolve_video_id(conn, video_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.video WHERE video_name = %s ORDER BY id DESC LIMIT 1;",
            (video_name,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Video not found: {video_name}")
        return int(row[0])


def _resolve_video_ids(conn, names: List[str]) -> List[int]:
    unique = list(dict.fromkeys([n.strip() for n in names if n.strip()]))
    if not unique:
        raise HTTPException(status_code=400, detail="video_names must be non-empty")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT video_name, id FROM public.video WHERE video_name = ANY(%s);",
            (unique,),
        )
        found = dict(cur.fetchall() or [])
    missing = [n for n in unique if n not in found]
    if missing:
        raise HTTPException(status_code=404, detail=f"Videos not found: {', '.join(missing)}")
    return [found[n] for n in unique]


def _set_group_video(conn, gid: int, vid: int):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT did FROM public.device_video_shop_group WHERE gid = %s;", (gid,))
        dev_ids = [r[0] for r in cur.fetchall()]

        cur.execute("""
        WITH targets AS (
            SELECT did, sid, gid FROM public.device_video_shop_group WHERE gid = %s GROUP BY did, sid, gid
        ),
        ins AS (
            INSERT INTO public.device_video_shop_group (did, vid, sid, gid)
            SELECT t.did, %s, t.sid, t.gid FROM targets t
            ON CONFLICT DO NOTHING RETURNING id
        ),
        del AS (
            DELETE FROM public.device_video_shop_group l
            USING targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid = t.gid AND l.vid <> %s
            RETURNING l.id
        ),
        upd AS (
            UPDATE public.device_video_shop_group l SET updated_at = NOW()
            FROM targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid = t.gid AND l.vid = %s
            RETURNING l.id
        )
        SELECT (SELECT COUNT(*) FROM ins)::int, (SELECT COUNT(*) FROM del)::int, (SELECT COUNT(*) FROM upd)::int;
        """, (gid, vid, vid, vid))
        inserted, deleted, remaining = cur.fetchone()

        cur.execute('SELECT gname FROM public."group" WHERE id = %s;', (gid,))
        gname = cur.fetchone()[0]
        cur.execute("SELECT video_name FROM public.video WHERE id = %s;", (vid,))
        video_name = cur.fetchone()[0]

        devices_marked = 0
        if dev_ids:
            cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = ANY(%s);", (dev_ids,))
            devices_marked = cur.rowcount or 0

    return inserted, deleted, remaining, gname, video_name, devices_marked


def _set_group_videos(conn, gid: int, vids: List[int]):
    vids = list(dict.fromkeys(vids))
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT did FROM public.device_video_shop_group WHERE gid = %s;", (gid,))
        dev_ids = [r[0] for r in cur.fetchall()]

        cur.execute("""
        WITH targets AS (
            SELECT did, sid, gid FROM public.device_video_shop_group WHERE gid = %s GROUP BY did, sid, gid
        ),
        ins AS (
            INSERT INTO public.device_video_shop_group (did, vid, sid, gid)
            SELECT t.did, x, t.sid, t.gid FROM targets t, UNNEST(%s::bigint[]) AS x
            ON CONFLICT DO NOTHING RETURNING id
        ),
        del AS (
            DELETE FROM public.device_video_shop_group l
            USING targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid = t.gid
              AND NOT (l.vid = ANY(%s::bigint[]))
            RETURNING l.id
        ),
        upd AS (
            UPDATE public.device_video_shop_group l SET updated_at = NOW()
            FROM targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid = t.gid
              AND l.vid = ANY(%s::bigint[])
            RETURNING l.id
        )
        SELECT (SELECT COUNT(*) FROM ins)::int, (SELECT COUNT(*) FROM del)::int, (SELECT COUNT(*) FROM upd)::int;
        """, (gid, vids, vids, vids))
        inserted, deleted, updated = cur.fetchone()

        cur.execute('SELECT gname FROM public."group" WHERE id = %s;', (gid,))
        gname = cur.fetchone()[0]
        cur.execute("SELECT video_name FROM public.video WHERE id = ANY(%s::bigint[]) ORDER BY video_name;", (vids,))
        video_names = [r[0] for r in cur.fetchall()]

        devices_marked = 0
        if dev_ids:
            cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = ANY(%s);", (dev_ids,))
            devices_marked = cur.rowcount or 0

    return inserted, deleted, updated, gname, video_names, devices_marked


def _set_nogroup_video(conn, vid: int):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT did FROM public.device_video_shop_group WHERE gid IS NULL;")
        dev_ids = [r[0] for r in cur.fetchall()]

        cur.execute("""
        WITH targets AS (SELECT did, sid FROM public.device_video_shop_group WHERE gid IS NULL GROUP BY did, sid),
        ins AS (INSERT INTO public.device_video_shop_group (did, vid, sid, gid) SELECT t.did, %s, t.sid, NULL FROM targets t ON CONFLICT DO NOTHING RETURNING id),
        del AS (DELETE FROM public.device_video_shop_group l USING targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid IS NULL AND l.vid <> %s RETURNING l.id),
        upd AS (UPDATE public.device_video_shop_group l SET updated_at = NOW() FROM targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid IS NULL AND l.vid = %s RETURNING l.id)
        SELECT (SELECT COUNT(*) FROM ins)::int, (SELECT COUNT(*) FROM del)::int, (SELECT COUNT(*) FROM upd)::int;
        """, (vid, vid, vid))
        inserted, deleted, remaining = cur.fetchone()

        cur.execute("SELECT video_name FROM public.video WHERE id = %s;", (vid,))
        video_name = cur.fetchone()[0]

        devices_marked = 0
        if dev_ids:
            cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = ANY(%s);", (dev_ids,))
            devices_marked = cur.rowcount or 0

    return inserted, deleted, remaining, video_name, devices_marked


def _set_nogroup_videos(conn, vids: List[int]):
    vids = list(dict.fromkeys(vids))
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT did FROM public.device_video_shop_group WHERE gid IS NULL;")
        dev_ids = [r[0] for r in cur.fetchall()]

        cur.execute("""
        WITH targets AS (SELECT did, sid FROM public.device_video_shop_group WHERE gid IS NULL GROUP BY did, sid),
        ins AS (INSERT INTO public.device_video_shop_group (did, vid, sid, gid) SELECT t.did, x, t.sid, NULL FROM targets t, UNNEST(%s::bigint[]) AS x ON CONFLICT DO NOTHING RETURNING id),
        del AS (DELETE FROM public.device_video_shop_group l USING targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid IS NULL AND NOT (l.vid = ANY(%s::bigint[])) RETURNING l.id),
        upd AS (UPDATE public.device_video_shop_group l SET updated_at = NOW() FROM targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid IS NULL AND l.vid = ANY(%s::bigint[]) RETURNING l.id)
        SELECT (SELECT COUNT(*) FROM ins)::int, (SELECT COUNT(*) FROM del)::int, (SELECT COUNT(*) FROM upd)::int;
        """, (vids, vids, vids))
        inserted, deleted, updated = cur.fetchone()

        cur.execute("SELECT video_name FROM public.video WHERE id = ANY(%s::bigint[]) ORDER BY video_name;", (vids,))
        video_names = [r[0] for r in cur.fetchall()]

        devices_marked = 0
        if dev_ids:
            cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = ANY(%s);", (dev_ids,))
            devices_marked = cur.rowcount or 0

    return inserted, deleted, updated, video_names, devices_marked


# ---------- Link creation ----------
def create_link_by_names(conn, payload: LinkCreate) -> Dict[str, Any]:
    did = get_device_id_by_mobile(conn, payload.mobile_id)
    if did is None:
        raise HTTPException(status_code=404, detail=f"Device not found: {payload.mobile_id}")

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.video WHERE video_name = %s ORDER BY id DESC LIMIT 1;", (payload.video_name,))
        vrow = cur.fetchone()
        if not vrow:
            raise HTTPException(status_code=404, detail=f"Video not found: {payload.video_name}")
        vid = int(vrow[0])

        cur.execute("SELECT id FROM public.shop WHERE shop_name = %s ORDER BY id DESC LIMIT 1;", (payload.shop_name,))
        srow = cur.fetchone()
        if not srow:
            raise HTTPException(status_code=404, detail=f"Shop not found: {payload.shop_name}")
        sid = int(srow[0])

        gid = None
        gname_in = (payload.gname or "").strip()
        if gname_in and gname_in.lower() not in NO_GROUP_SENTINELS:
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname_in,))
            grow = cur.fetchone()
            if not grow:
                raise HTTPException(status_code=404, detail=f"Group not found: {gname_in}")
            gid = int(grow[0])

        _enforce_single_group_shop_for_device(conn, did, gid, sid)

        if gid is None:
            cur.execute("""
                INSERT INTO public.device_video_shop_group (did, vid, sid, gid, display_order)
                VALUES (%s, %s, %s, NULL, %s)
                ON CONFLICT (did, vid, sid) WHERE (gid IS NULL)
                DO UPDATE SET updated_at = NOW(), display_order = EXCLUDED.display_order
                RETURNING id;
            """, (did, vid, sid, payload.display_order))
        else:
            cur.execute("""
                INSERT INTO public.device_video_shop_group (did, vid, sid, gid, display_order)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (did, vid, sid, gid) WHERE (gid IS NOT NULL)
                DO UPDATE SET updated_at = NOW(), display_order = EXCLUDED.display_order
                RETURNING id;
            """, (did, vid, sid, gid, payload.display_order))

        lrow = cur.fetchone()
        cur.execute(READ_JOIN_SQL + " WHERE l.id = %s;", (lrow[0],))
        full = cur.fetchone()
        _write_device_flag(conn, did, False)

    return _row_to_link_dict(full)


# ---------- startup/shutdown ----------
@app.on_event("startup")
def on_startup():
    pg_init_pool()
    with pg_conn() as conn:
        ensure_dvsg_schema(conn)


@app.on_event("shutdown")
def on_shutdown():
    pg_close_pool()


# ---------- health endpoints ----------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db_health")
def db_health():
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user;")
            user = cur.fetchone()[0]
            cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='device_video_shop_group');")
            has_link = cur.fetchone()[0]
        return {"ok": True, "db_user": user, "dvsg_table_exists": has_link}


@app.get("/pool_stats")
def pool_stats():
    global pg_pool
    if pg_pool is None:
        return {"status": "no_pool"}
    return {"minconn": pg_pool.minconn, "maxconn": pg_pool.maxconn, "used": len(pg_pool._used), "free": len(pg_pool._pool)}


# ---------- NEW: Video preview endpoints (fix 404 for UI) ----------
@app.get("/video/{video_name}", response_model=VideoOut)
def get_video(video_name: str = Path(...), presign: bool = Query(True), expires_in: int = Query(PRESIGN_EXPIRES, ge=60, le=604800)):
    """
    Returns video metadata from public.video.
    If presign=true:
      - if s3_link is https://... -> returns it directly
      - if s3_link is s3://bucket/key -> returns presigned URL (inline)
    """
    with pg_conn() as conn:
        v = fetch_video_by_name(conn, video_name)

    if not v:
        raise HTTPException(status_code=404, detail="Video not found")

    s3_link = (v.get("s3_link") or "").strip()
    if not s3_link:
        raise HTTPException(status_code=404, detail="Video s3_link is not set")

    # If already a public URL, return it
    if s3_link.startswith("http://") or s3_link.startswith("https://"):
        v["url"] = s3_link
        v["presigned_url"] = s3_link
        v["expires_in"] = 0
        v["filename"] = s3_link.split("/")[-1] or "video"
        return VideoOut(**v)

    # Otherwise presign s3://...
    if presign:
        try:
            url, filename = presign_get_object_inline(s3_link, expires_in=expires_in)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid s3_link format. Expected s3://bucket/key or https://...")
        v["url"] = url
        v["presigned_url"] = url
        v["expires_in"] = expires_in
        v["filename"] = filename

    return VideoOut(**v)


@app.get("/video/{video_name}/presign")
def presign_video(video_name: str = Path(...), expires_in: int = Query(PRESIGN_EXPIRES, ge=60, le=604800)):
    """
    UI expects this endpoint.
    Returns {url, presigned_url, expires_in}.
    """
    with pg_conn() as conn:
        v = fetch_video_by_name(conn, video_name)

    if not v:
        raise HTTPException(status_code=404, detail="Video not found")

    s3_link = (v.get("s3_link") or "").strip()
    if not s3_link:
        raise HTTPException(status_code=404, detail="Video s3_link is not set")

    # Public URL
    if s3_link.startswith("http://") or s3_link.startswith("https://"):
        filename = s3_link.split("/")[-1] or "video"
        return {
            "video_name": video_name,
            "s3_link": s3_link,
            "url": s3_link,
            "presigned_url": s3_link,
            "expires_in": 0,
            "filename": filename,
        }

    # Presign s3://...
    try:
        url, filename = presign_get_object_inline(s3_link, expires_in=expires_in)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid s3_link format. Expected s3://bucket/key or https://...")
    return {
        "video_name": video_name,
        "s3_link": s3_link,
        "url": url,
        "presigned_url": url,
        "expires_in": expires_in,
        "filename": filename,
    }


# ---------- Device counts ----------
@app.get("/device/{mobile_id}/counts", response_model=DeviceCountsOut)
def get_device_counts(mobile_id: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, daily_count, monthly_count FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]
        # Check and reset counters
        _check_and_reset_counters(conn, did)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT daily_count, monthly_count FROM public.device WHERE id = %s;", (did,))
            row = cur.fetchone()
            return DeviceCountsOut(mobile_id=mobile_id, daily_count=row[0], monthly_count=row[1])


# ---------- Group videos ----------
@app.get("/group/{gname}/videos")
def list_group_videos_by_name(gname: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            if gname.strip().lower() in NO_GROUP_SENTINELS:
                cur.execute("""
                    SELECT DISTINCT v.id, v.video_name
                    FROM public.device_video_shop_group l
                    JOIN public.video v ON v.id = l.vid
                    WHERE l.gid IS NULL ORDER BY v.video_name;
                """)
                rows = cur.fetchall() or []
                return {"gid": None, "gname": None, "vids": [r[0] for r in rows], "video_names": [r[1] for r in rows], "count": len(rows)}

            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            grow = cur.fetchone()
            if not grow:
                raise HTTPException(status_code=404, detail=f"Group not found: {gname}")
            gid = int(grow[0])

            cur.execute("""
                SELECT DISTINCT v.id, v.video_name
                FROM public.device_video_shop_group l
                JOIN public.video v ON v.id = l.vid
                WHERE l.gid = %s ORDER BY v.video_name;
            """, (gid,))
            rows = cur.fetchall() or []
            return {"gid": gid, "gname": gname, "vids": [r[0] for r in rows], "video_names": [r[1] for r in rows], "count": len(rows)}


# ---------- Temperature, daily, monthly updates ----------
@app.post("/device/{mobile_id}/temperature_update")
def set_device_temperature(mobile_id: str, body: DeviceTemperatureUpdateIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]

                cur.execute("""
                    UPDATE public.device SET temperature = %s, updated_at = NOW()
                    WHERE id = %s RETURNING temperature;
                """, (body.temperature, did))
                temp = cur.fetchone()[0]

                # Log temperature
                _log_event(conn, did, 'temperature', body.temperature)

            conn.commit()
            return {"mobile_id": mobile_id, "temperature": float(temp)}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/device/{mobile_id}/daily_update")
def set_device_daily_count(mobile_id: str, body: DeviceDailyCountUpdateIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]

            _check_and_reset_counters(conn, did)

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.device SET daily_count = %s, updated_at = NOW()
                    WHERE id = %s RETURNING daily_count;
                """, (body.daily_count, did))
                count = cur.fetchone()[0]

            conn.commit()
            return {"mobile_id": mobile_id, "daily_count": int(count)}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/device/{mobile_id}/monthly_update")
def set_device_monthly_count(mobile_id: str, body: DeviceMonthlyCountUpdateIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]

            _check_and_reset_counters(conn, did)

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.device SET monthly_count = %s, updated_at = NOW()
                    WHERE id = %s RETURNING monthly_count;
                """, (body.monthly_count, did))
                count = cur.fetchone()[0]

            conn.commit()
            return {"mobile_id": mobile_id, "monthly_count": int(count)}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/device/{mobile_id}/door_open")
def record_door_open(mobile_id: str):
    """Record a door open event - increments both daily and monthly counts, logs the event."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]

            # Check and reset counters first
            _check_and_reset_counters(conn, did)

            with conn.cursor() as cur:
                # Increment both counters
                cur.execute("""
                    UPDATE public.device
                    SET daily_count = daily_count + 1,
                        monthly_count = monthly_count + 1,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING daily_count, monthly_count;
                """, (did,))
                daily, monthly = cur.fetchone()

                # Log the door open event
                _log_event(conn, did, 'door_open', 1)

            conn.commit()
            return {"mobile_id": mobile_id, "daily_count": daily, "monthly_count": monthly}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Link CRUD ----------
@app.post("/link", response_model=LinkOut)
def create_link(req: LinkCreate):
    with pg_conn() as conn:
        try:
            row = create_link_by_names(conn, req)
            conn.commit()
            return row
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.get("/link/{link_id}", response_model=LinkOut)
def get_link(link_id: int = Path(..., ge=1)):
    with pg_conn() as conn:
        row = fetch_link_by_id(conn, link_id)
        if not row:
            raise HTTPException(status_code=404, detail="Link not found")
        return row


@app.get("/links")
def list_links_route(
    mobile_id: Optional[str] = Query(None),
    video_name: Optional[str] = Query(None),
    shop_name: Optional[str] = Query(None),
    gname: Optional[str] = Query(None),
    did: Optional[int] = Query(None, ge=1),
    vid: Optional[int] = Query(None, ge=1),
    sid: Optional[int] = Query(None, ge=1),
    gid: Optional[int] = Query(None, ge=1),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    with pg_conn() as conn:
        rows = list_links(conn, mobile_id, video_name, shop_name, gname, did, vid, sid, gid, limit, offset)
        return {"count": len(rows), "items": rows, "limit": limit, "offset": offset}


@app.delete("/link/{link_id}")
def delete_link_route(link_id: int = Path(..., ge=1)):
    with pg_conn() as conn:
        try:
            row = fetch_link_by_id(conn, link_id)
            if not row:
                raise HTTPException(status_code=404, detail="Link not found")
            did = int(row["did"])
            deleted = delete_link_row(conn, link_id)
            if deleted == 0:
                raise HTTPException(status_code=404, detail="Link not found")
            total, done, all_done = _aggregate_from_links(conn, did)
            _write_device_flag(conn, did, all_done)
            conn.commit()
            return {"deleted_count": deleted, "device_download_status": all_done, "total_links": total, "downloaded_count": done}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/link/{link_id}/delete")
def delete_link_route_fallback(link_id: int = Path(..., ge=1)):
    return delete_link_route(link_id)


# ---------- Device video endpoints ----------
@app.get("/device/{mobile_id}/video", response_model=LinkOut)
def get_latest_video_for_device(mobile_id: str):
    with pg_conn() as conn:
        rows = fetch_links_for_mobile(conn, mobile_id, limit=1, offset=0)
        if not rows:
            raise HTTPException(status_code=404, detail="No video linked to this device")
        return rows[0]


@app.get("/device/{mobile_id}/videos")
def list_videos_for_device(mobile_id: str, limit: int = Query(200, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with pg_conn() as conn:
        rows = fetch_links_for_mobile(conn, mobile_id, limit, offset)
        return {"count": len(rows), "items": rows, "limit": limit, "offset": offset, "mobile_id": mobile_id}


@app.get("/device/{mobile_id}/videos/downloads", response_model=PresignedURLListOut)
def list_download_urls_for_device(mobile_id: str, limit: int = Query(200, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with pg_conn() as conn:
        rows = fetch_links_for_mobile(conn, mobile_id, limit, offset)
    if not rows:
        raise HTTPException(status_code=404, detail="No video linked to this device")

    items = []
    for r in rows:
        if not r.get("s3_link"):
            continue
        url, filename = presign_get_object(r["s3_link"], PRESIGN_EXPIRES)
        items.append(PresignedURLItem(
            link_id=r["id"],
            video_name=r["video_name"],
            url=url,
            expires_in=PRESIGN_EXPIRES,
            filename=filename,
            rotation=r.get("rotation", 0),
            content_type=r.get("content_type", "video"),
            fit_mode=r.get("fit_mode", "cover"),
            display_duration=r.get("display_duration", 10),
            display_order=r.get("display_order", 0),
        ))
    if not items:
        raise HTTPException(status_code=404, detail="No downloadable videos for this device")
    return PresignedURLListOut(mobile_id=mobile_id, items=items, count=len(items))


@app.get("/device/{mobile_id}/video/{link_id}/download")
def download_video_for_link(mobile_id: str, link_id: int):
    with pg_conn() as conn:
        row = fetch_link_by_id(conn, link_id)
        if not row or row["mobile_id"] != mobile_id:
            raise HTTPException(status_code=404, detail="Video not found for this device")
        if not row.get("s3_link"):
            raise HTTPException(status_code=404, detail="No s3_link for this video")
    url, _ = presign_get_object(row["s3_link"], PRESIGN_EXPIRES)
    return RedirectResponse(url=url, status_code=302)


# ---------- Download status ----------
@app.get("/device/{mobile_id}/download_status", response_model=DeviceDownloadStatusOut)
def get_device_download_status(mobile_id: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, download_status FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            drow = cur.fetchone()
            if not drow:
                raise HTTPException(status_code=404, detail="Device not found")
            did, dev_flag = int(drow[0]), bool(drow[1])
        total, done, _ = _aggregate_from_links(conn, did)
        return DeviceDownloadStatusOut(mobile_id=mobile_id, download_status=dev_flag, total_links=total, downloaded_count=done)


@app.post("/device/{mobile_id}/download_update", response_model=DeviceDownloadStatusOut)
def set_device_download_flag(mobile_id: str, body: DeviceDownloadStatusSetIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                drow = cur.fetchone()
                if not drow:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = int(drow[0])
            _write_device_flag(conn, did, bool(body.status))
            total, done, _ = _aggregate_from_links(conn, did)
            conn.commit()
            return DeviceDownloadStatusOut(mobile_id=mobile_id, download_status=bool(body.status), total_links=total, downloaded_count=done)
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Online status with 1-minute threshold ----------
@app.get("/device/{mobile_id}/online", response_model=DeviceOnlineStatusOut)
def get_device_online_status(mobile_id: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT is_online, last_online_at,
                       EXTRACT(EPOCH FROM (NOW() - last_online_at)) as seconds_ago
                FROM public.device WHERE mobile_id = %s LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")

            is_online = row[0]
            seconds_ago = row[2]

            # If last heartbeat was more than threshold seconds ago, mark offline
            if seconds_ago is not None and seconds_ago > ONLINE_THRESHOLD_SECONDS:
                is_online = False
                cur.execute("UPDATE public.device SET is_online = FALSE WHERE mobile_id = %s;", (mobile_id,))
                conn.commit()

            return DeviceOnlineStatusOut(mobile_id=mobile_id, is_online=bool(is_online))


@app.post("/device/{mobile_id}/online_update")
def set_device_online_status(mobile_id: str, body: DeviceOnlineUpdateIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.device
                    SET is_online = %s, last_online_at = NOW(), updated_at = NOW()
                    WHERE mobile_id = %s
                    RETURNING is_online;
                """, (body.is_online, mobile_id))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
            conn.commit()
            return {"mobile_id": mobile_id, "is_online": bool(row[0])}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Device creation with group/shop linking ----------
@app.post("/device/create")
def create_device_with_linking(body: DeviceCreateIn):
    """Create a new device and optionally link to group and shop."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Check if device exists
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;", (body.mobile_id,))
                existing = cur.fetchone()

                if existing:
                    did = existing[0]
                else:
                    # Create new device
                    cur.execute("""
                        INSERT INTO public.device (mobile_id, download_status, is_online, last_online_at)
                        VALUES (%s, FALSE, FALSE, NOW())
                        RETURNING id;
                    """, (body.mobile_id,))
                    did = cur.fetchone()[0]

                result = {
                    "device_id": did,
                    "mobile_id": body.mobile_id,
                    "created": not bool(existing),
                    "linked_to_group": False,
                    "linked_to_shop": False,
                    "gname": None,
                    "shop_name": None,
                }

                # If group and shop provided, create a link
                if body.group_name and body.shop_name:
                    cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (body.group_name,))
                    grow = cur.fetchone()
                    if not grow:
                        conn.rollback()
                        raise HTTPException(status_code=404, detail=f"Group not found: {body.group_name}")
                    gid = grow[0]

                    cur.execute("SELECT id FROM public.shop WHERE shop_name = %s ORDER BY id DESC LIMIT 1;", (body.shop_name,))
                    srow = cur.fetchone()
                    if not srow:
                        conn.rollback()
                        raise HTTPException(status_code=404, detail=f"Shop not found: {body.shop_name}")
                    sid = srow[0]

                    # Get first video from group (if any)
                    cur.execute("""
                        SELECT DISTINCT v.id
                        FROM public.device_video_shop_group dvsg
                        JOIN public.video v ON v.id = dvsg.vid
                        WHERE dvsg.gid = %s
                        LIMIT 1;
                    """, (gid,))
                    vrow = cur.fetchone()

                    if vrow:
                        vid = vrow[0]
                        cur.execute("""
                            INSERT INTO public.device_video_shop_group (did, vid, sid, gid)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (did, vid, sid, gid) WHERE (gid IS NOT NULL)
                            DO UPDATE SET updated_at = NOW()
                            RETURNING id;
                        """, (did, vid, sid, gid))

                    result["linked_to_group"] = True
                    result["linked_to_shop"] = True
                    result["gname"] = body.group_name
                    result["shop_name"] = body.shop_name

            conn.commit()
            return result
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Group video endpoints ----------
@app.post("/group/{gname}/video", response_model=GroupVideoUpdateOut)
def set_group_video_by_name(gname: str, body: GroupVideoUpdateIn):
    with pg_conn() as conn:
        try:
            if gname.strip().lower() in NO_GROUP_SENTINELS:
                vid = _resolve_video_id(conn, body.video_name)
                ins, dele, rem, vname, marked = _set_nogroup_video(conn, vid)
                conn.commit()
                return GroupVideoUpdateOut(gid=None, gname=None, vid=vid, video_name=vname, inserted_count=ins, deleted_count=dele, remaining_count=rem, devices_marked=marked)
            gid = _resolve_group_id(conn, gname)
            vid = _resolve_video_id(conn, body.video_name)
            ins, dele, rem, gname_res, vname_res, marked = _set_group_video(conn, gid, vid)
            conn.commit()
            return GroupVideoUpdateOut(gid=gid, gname=gname_res, vid=vid, video_name=vname_res, inserted_count=ins, deleted_count=dele, remaining_count=rem, devices_marked=marked)
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/group/{gname}/videos", response_model=GroupVideosUpdateOut)
def set_group_videos_by_names(gname: str, body: GroupVideosUpdateIn):
    with pg_conn() as conn:
        try:
            vids = _resolve_video_ids(conn, body.video_names)
            if gname.strip().lower() in NO_GROUP_SENTINELS:
                ins, dele, upd, vnames, marked = _set_nogroup_videos(conn, vids)
                conn.commit()
                return GroupVideosUpdateOut(gid=None, gname=None, vids=vids, video_names=vnames, inserted_count=ins, deleted_count=dele, updated_count=upd, devices_marked=marked)
            gid = _resolve_group_id(conn, gname)
            ins, dele, upd, gname_res, vnames, marked = _set_group_videos(conn, gid, vids)
            conn.commit()
            return GroupVideosUpdateOut(gid=gid, gname=gname_res, vids=vids, video_names=vnames, inserted_count=ins, deleted_count=dele, updated_count=upd, devices_marked=marked)
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Video rotation and fit mode ----------
@app.post("/video/{video_name}/rotation")
def set_video_rotation(video_name: str, body: VideoRotationUpdateIn):
    if body.rotation not in [0, 90, 180, 270]:
        raise HTTPException(status_code=400, detail="Rotation must be 0, 90, 180, or 270")
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.video SET rotation = %s, updated_at = NOW()
                    WHERE video_name = %s RETURNING id, rotation;
                """, (body.rotation, video_name))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Video not found")
            conn.commit()
            return {"video_name": video_name, "rotation": row[1]}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/video/{video_name}/fit_mode")
def set_video_fit_mode(video_name: str, body: VideoFitModeUpdateIn):
    valid_modes = ["contain", "cover", "fill", "none"]
    if body.fit_mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"fit_mode must be one of: {', '.join(valid_modes)}")
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.video SET fit_mode = %s, updated_at = NOW()
                    WHERE video_name = %s RETURNING id, fit_mode;
                """, (body.fit_mode, video_name))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Video not found")
            conn.commit()
            return {"video_name": video_name, "fit_mode": row[1]}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Logs and Reports ----------
@app.get("/device/{mobile_id}/logs")
def get_device_logs(
    mobile_id: str,
    log_type: Optional[str] = Query(None, description="Filter by log_type: temperature, door_open"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Get logs for a device with optional filters."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]

            where = ["did = %s"]
            params = [did]

            if log_type:
                where.append("log_type = %s")
                params.append(log_type)
            if start_date:
                where.append("logged_at >= %s::date")
                params.append(start_date)
            if end_date:
                where.append("logged_at < (%s::date + interval '1 day')")
                params.append(end_date)

            params.extend([limit, offset])

            cur.execute(f"""
                SELECT id, log_type, value, logged_at
                FROM public.device_logs
                WHERE {' AND '.join(where)}
                ORDER BY logged_at DESC
                LIMIT %s OFFSET %s;
            """, tuple(params))
            rows = cur.fetchall()

            items = [{"id": r[0], "log_type": r[1], "value": r[2], "logged_at": r[3]} for r in rows]
            return {"mobile_id": mobile_id, "count": len(items), "items": items}


@app.get("/device/{mobile_id}/logs/download")
def download_device_logs(
    mobile_id: str,
    log_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Download device logs as CSV."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]

            where = ["did = %s"]
            params = [did]

            if log_type:
                where.append("log_type = %s")
                params.append(log_type)
            if start_date:
                where.append("logged_at >= %s::date")
                params.append(start_date)
            if end_date:
                where.append("logged_at < (%s::date + interval '1 day')")
                params.append(end_date)

            cur.execute(f"""
                SELECT log_type, value, logged_at
                FROM public.device_logs
                WHERE {' AND '.join(where)}
                ORDER BY logged_at DESC;
            """, tuple(params))
            rows = cur.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["mobile_id", "log_type", "value", "logged_at"])
    for r in rows:
        writer.writerow([mobile_id, r[0], r[1], r[2].isoformat() if r[2] else ""])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=logs_{mobile_id}.csv"}
    )


@app.get("/devices/logs/summary")
def get_all_devices_logs_summary(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get summary of logs for all devices."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            date_filter = ""
            params = []
            if start_date:
                date_filter += " AND dl.logged_at >= %s::date"
                params.append(start_date)
            if end_date:
                date_filter += " AND dl.logged_at < (%s::date + interval '1 day')"
                params.append(end_date)

            cur.execute(f"""
                SELECT d.mobile_id,
                       COUNT(CASE WHEN dl.log_type = 'door_open' THEN 1 END) as door_open_count,
                       AVG(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as avg_temperature,
                       MIN(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as min_temperature,
                       MAX(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as max_temperature
                FROM public.device d
                LEFT JOIN public.device_logs dl ON dl.did = d.id {date_filter}
                GROUP BY d.id, d.mobile_id
                ORDER BY d.mobile_id;
            """, tuple(params))
            rows = cur.fetchall()

            items = [{
                "mobile_id": r[0],
                "door_open_count": r[1] or 0,
                "avg_temperature": round(float(r[2]), 2) if r[2] else None,
                "min_temperature": float(r[3]) if r[3] else None,
                "max_temperature": float(r[4]) if r[4] else None,
            } for r in rows]
            return {"count": len(items), "items": items}


@app.get("/devices/logs/summary/download")
def download_all_devices_logs_summary(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Download summary of all devices as CSV."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            date_filter = ""
            params = []
            if start_date:
                date_filter += " AND dl.logged_at >= %s::date"
                params.append(start_date)
            if end_date:
                date_filter += " AND dl.logged_at < (%s::date + interval '1 day')"
                params.append(end_date)

            cur.execute(f"""
                SELECT d.mobile_id, d.daily_count, d.monthly_count,
                       COUNT(CASE WHEN dl.log_type = 'door_open' THEN 1 END) as door_open_count,
                       AVG(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as avg_temp,
                       MIN(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as min_temp,
                       MAX(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as max_temp
                FROM public.device d
                LEFT JOIN public.device_logs dl ON dl.did = d.id {date_filter}
                GROUP BY d.id, d.mobile_id, d.daily_count, d.monthly_count
                ORDER BY d.mobile_id;
            """, tuple(params))
            rows = cur.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["mobile_id", "daily_count", "monthly_count", "door_open_count", "avg_temperature", "min_temperature", "max_temperature"])
    for r in rows:
        writer.writerow([r[0], r[1], r[2], r[3] or 0, round(float(r[4]), 2) if r[4] else "", r[5] or "", r[6] or ""])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=devices_summary.csv"}
    )


# ---------- Admin: Mark offline devices ----------
@app.post("/admin/mark_offline_devices")
def mark_offline_devices():
    """Mark devices as offline if their last_online_at is older than threshold. For cron job."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE public.device
                    SET is_online = FALSE, updated_at = NOW()
                    WHERE is_online = TRUE
                      AND (last_online_at IS NULL
                           OR last_online_at < NOW() - INTERVAL '{ONLINE_THRESHOLD_SECONDS} seconds')
                    RETURNING mobile_id;
                """)
                rows = cur.fetchall()
                marked = [r[0] for r in rows]
            conn.commit()
            return {"marked_offline": len(marked), "devices": marked}
        except:
            conn.rollback()
            raise


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "device_video_shop_group:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8005")),
        reload=True,
    )

