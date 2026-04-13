import argparse
import sys

from preparation.logger import logger


def save_file(name, n):
    with open(name, "w") as f:
        for x in range(n):
            f.write(f"{x} 1\n")


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Make wav list")
    parser.add_argument("--n", nargs='?', required=True, type=int,  help="Number of clusters for dict.km.txt")
    parser.add_argument("--output", nargs='?', required=True, help="Dir to wanted output files")

    args = parser.parse_args(args=argv)

    logger.info(f"Output Test : {args.output}")
    logger.info(f"N           : {args.n}")

    save_file(args.output, args.n)

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
