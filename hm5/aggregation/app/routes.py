import logging
from datetime import date, timedelta
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

# Will be set from main.py
ch_client = None
pg_writer = None
s3_exporter = None
agg_scheduler = None


def set_dependencies(ch, pg, s3, scheduler):
    global ch_client, pg_writer, s3_exporter, agg_scheduler
    ch_client = ch
    pg_writer = pg
    s3_exporter = s3
    agg_scheduler = scheduler


logger = logging.getLogger(__name__)


@router.get("/health")
async def health():
    checks = {
        "clickhouse": ch_client.ping() if ch_client else False,
        "postgres": pg_writer.ping() if pg_writer else False,
        "s3": s3_exporter.ping() if s3_exporter else False,
    }
    overall = all(checks.values())
    return {"status": "ok" if overall else "degraded", "checks": checks}


@router.get("/top-movies")
async def top_movies(
    limit: int = Query(10, ge=1, le=100),
    days: int = Query(7, ge=1, le=90),
):
    if ch_client is None:
        raise HTTPException(status_code=503, detail="ClickHouse not available")
    try:
        as_of = date.today()
        rows = ch_client.top_movies_last_7_days(limit=limit, as_of=as_of)
        return {"period_days": days, "as_of": as_of.isoformat(), "movies": rows}
    except Exception as e:
        logger.exception("Failed to fetch top movies")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/daily-aggregates")
async def daily_aggregates(
    target_date: date = Query(default=None),
):
    if ch_client is None:
        raise HTTPException(status_code=503, detail="ClickHouse not available")
    target = target_date or date.today()
    try:
        rows = ch_client.daily_aggregates(target)
        return {"date": target.isoformat(), "count": len(rows), "aggregates": rows}
    except Exception as e:
        logger.exception("Failed to fetch daily aggregates")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trigger-aggregation")
async def trigger_aggregation():
    if agg_scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not available")
    try:
        await agg_scheduler.run_aggregation()
        return {"status": "completed"}
    except Exception as e:
        logger.exception("Manual aggregation failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export-parquet")
async def export_parquet(
    target_date: date = Query(default=None),
):
    if ch_client is None or s3_exporter is None:
        raise HTTPException(status_code=503, detail="Services not available")
    target = target_date or date.today()
    try:
        rows = ch_client.daily_aggregates(target)
        path = s3_exporter.export_parquet(rows, target)
        return {"date": target.isoformat(), "rows": len(rows), "s3_path": path}
    except Exception as e:
        logger.exception("Export failed")
        raise HTTPException(status_code=500, detail=str(e))