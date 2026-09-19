import argparse
from typing import Callable

from celery import group

from tasks.images import extract_metadata, resize_image, watermark_image
from tasks.messaging import send_whatsapp_message


def fanout(path: str) -> None:
    job = group(
        resize_image.s(path),
        watermark_image.s(path),
        extract_metadata.s(path),
    )
    result = job.apply_async()
    print(result.get(timeout=10))


def flaky(count: int) -> None:
    for i in range(count):
        message = {"recipient": f"user{i}", "text": f"message {i}"}
        result = send_whatsapp_message.delay(message)
        print(f"[{i}] ->", result.get(timeout=90))


def run_fanout(args: argparse.Namespace) -> None:
    fanout(args.path)


def run_flaky(args: argparse.Namespace) -> None:
    flaky(args.count)


COMMANDS: dict[str, Callable[[argparse.Namespace], None]] = {
    "fanout": run_fanout,
    "flaky": run_flaky,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fanout_parser = subparsers.add_parser("fanout", help="Fan out image processing tasks via group()")
    fanout_parser.add_argument("path", help="Image path (stub value, no real file needed)")

    flaky_parser = subparsers.add_parser("flaky", help="Send several messages via the flaky retrying task")
    flaky_parser.add_argument("count", type=int, nargs="?", default=5, help="Number of messages to send (default 5)")

    return parser


def main() -> None:
    args = build_parser().parse_args()
    COMMANDS[args.command](args)


if __name__ == "__main__":
    main()
