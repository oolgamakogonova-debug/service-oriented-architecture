import logging
from datetime import date, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.clickhouse_queries import ClickHouseClient
from app.postgres_writer import PostgresWriter
from app.s3_exporter import S3Exporter
from app.config import AGGREGATION_INTERVAL_SECONDS

logger = logging.getLogger(__name__)


class AggregationScheduler:
    def __init__(self):
        self.ch = ClickHouseClient()
        self.pg = PostgresWriter()
        self.s3 = S3Exporter()
        self.scheduler = AsyncIOScheduler()

    def start(self):
        self.scheduler.add_job(
            self.run_aggregation,
            "interval",
            seconds=AGGREGATION_INTERVAL_SECONDS,
            next_run_time=None,
            id="aggregation_job",
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.start()
        logger.info(
            "Aggregation scheduler started (every %d sec)",
            AGGREGATION_INTERVAL_SECONDS,
        )

    def shutdown(self):
        self.scheduler.shutdown(wait=False)

    async def run_aggregation(self):
        run_id = self.pg.start_run()
        logger.info("Aggregation run #%d started", run_id)
        try:
            # Aggregate today and yesterday (covers events near midnight)
            today = date.today()
            total_rows = 0
            s3_paths = []
            for d in [today - timedelta(days=1), today]:
                rows = self.ch.daily_aggregates(d)
                if not rows:
                    logger.info("No data for %s", d)
                    continue
                written = self.pg.upsert_aggregates(rows)
                total_rows += written
                path = self.s3.export_parquet(rows, d)
                if path:
                    s3_paths.append(path)

            self.pg.finish_run(
                run_id,
                status="success",
                rows_processed=total_rows,
                s3_path=",".join(s3_paths) if s3_paths else None,
            )
            logger.info(
                "Aggregation run #%d completed: %d rows", run_id, total_rows
            )
        except Exception as e:
            logger.exception("Aggregation run #%d failed", run_id)
            self.pg.finish_run(
                run_id, status="failed", error_message=str(e)
            )
            raise