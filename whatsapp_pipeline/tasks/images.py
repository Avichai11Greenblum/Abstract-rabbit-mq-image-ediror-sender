import time

from celery_app import app


@app.task
def resize_image(path: str) -> str:
    time.sleep(1)
    return f"resized:{path}"


@app.task
def watermark_image(path: str) -> str:
    time.sleep(1)
    return f"watermarked:{path}"


@app.task
def extract_metadata(path: str) -> str:
    time.sleep(1)
    return f"metadata:{path}"
