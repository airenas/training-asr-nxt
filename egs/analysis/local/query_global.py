import argparse
import sys
import duckdb

from preparation.logger import logger


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Starting prepare user segments")
    parser.add_argument("--input", nargs='?', required=True, help="jsonl list")
    args = parser.parse_args(args=argv)
    logger.info(f"input: {args.input}")
    logger.info("===========================================================")
    result = duckdb.query(f"""
        SELECT 
            sum(duration + duration_non_lt) as total_duration,
            median(duration + duration_non_lt) AS median_duration, 
        avg(duration + duration_non_lt) as avg_duration,
        count(*) as speakers
        FROM '{args.input}'
    """).fetchone()

    total_duration, median_duration, avg_duration, speakers = result
    logger.info(f"Total duration (all): {total_duration/3600:.2f}h, avg duration: {avg_duration/3600:.2f}h, median: {median_duration/60:.2f}m, Speakers: {speakers}")
    logger.info("===========================================================")

    result = duckdb.query(f"""
        SELECT sum(duration) as total_duration, 
        avg(duration) as avg_duration,
        count(*) as speakers
        FROM '{args.input}'
        WHERE duration > 0
    """).fetchone()

    total_duration, avg_duration, speakers = result
    logger.info(f"Total duration (LT): {total_duration/3600:.2f}h, avg duration: {avg_duration/3600:.2f}h, Speakers: {speakers}")
    logger.info("===========================================================")

    result = duckdb.query(f"""
        SELECT sum(duration_non_lt) as total_duration, avg(duration_non_lt) as avg_duration, count(*) as speakers
        FROM '{args.input}' 
        WHERE duration_non_lt > 0
    """).fetchone()

    total_duration, avg_duration, speakers = result
    logger.info(f"Total duration (non LT): {total_duration/3600:.2f}h, avg duration: {avg_duration/3600:.2f}h, Speakers: {speakers}")
    logger.info("===========================================================")

    top = 50
    logger.info(f"top {top} LT speakers")
    result = duckdb.query(f"""
        SELECT sp as speaker, sum(duration) as total_duration
        FROM '{args.input}' 
        WHERE duration > 0
        GROUP BY sp
        ORDER BY total_duration DESC
        LIMIT {top}
    """).fetchall()

    for (speaker, total_duration) in result:
        logger.info(f"{speaker}: {total_duration/3600:.2f}h")
    logger.info("===========================================================")
    top = 10
    logger.info(f"top {top} non LT speakers")
    result = duckdb.query(f"""
        SELECT sp as speaker, sum(duration_non_lt) as total_duration
        FROM '{args.input}' 
        WHERE duration_non_lt > 0
        GROUP BY sp
        ORDER BY total_duration DESC
        LIMIT {top}
    """).fetchall()

    for (speaker, total_duration) in result:
        logger.info(f"{speaker}: {total_duration/3600:.2f}h")
    logger.info("===========================================================")

    for h in [1, 2, 5, 10, 20, 50, 100, 200, 100000]:
        result = duckdb.query(f"""
                SELECT 
                    sum(least(duration, 3600*{h})) as total_duration, 
                    count_if(duration > 3600*{h}) AS trimmed_speakers,
                    sum(duration) as all_duration, 
                    count(*) as speakers
                FROM '{args.input}'
                WHERE duration > 0
            """).fetchone()
        total_duration, trimmed_speakers, all_duration, speakers = result
        logger.info(f"Total duration (LT) by taking max {h}h: {total_duration/3600:.2f}h, trimmed: {(all_duration-total_duration)/3600:.2f}h, Speakers: {speakers}, trimmed: {trimmed_speakers}")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
