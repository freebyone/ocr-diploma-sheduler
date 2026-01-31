from paddleocr import PaddleOCRVL
from src.models import Image
import asyncio

def ocr(img_path: str):
    pipeline = PaddleOCRVL()
    output = pipeline.predict(img_path)
    for res in output:
        res.print()
        res.save_to_json(save_path="output")
        res.save_to_markdown(save_path="output")
        return res

async def recognise(img: str):
    try:
        res = ocr(img)
    except Exception as e:
        raise e
    return res