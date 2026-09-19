import argparse

from celery import group

from tasks.images import extract_metadata, resize_image, watermark_image


def fanout(path: str) -> None:
    job = group(
        resize_image.s(path),
        watermark_image.s(path),
        extract_metadata.s(path),
    )
    result = job.apply_async()
    print(result.get(timeout=10))


def main() -> None:
    parser = argparse.ArgumentParser(prog="cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fanout_parser = subparsers.add_parser("fanout", help="Fan out image processing tasks via group()")
    fanout_parser.add_argument("path", help="Image path (stub value, no real file needed)")

    args = parser.parse_args()

    if args.command == "fanout":
        fanout(args.path)


if __name__ == "__main__":
    main()
