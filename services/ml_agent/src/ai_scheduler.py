from paddleocr import PaddleOCRVL
from models import Image
import asyncio

def ocr(img_path: Image):
    pipeline = PaddleOCRVL()
    output = pipeline.predict(img_path)
    for res in output:
        res.print()
        res.save_to_json(save_path="output")
        res.save_to_markdown(save_path="output")

async def recognise(img: Image):
    try:
        res = ocr(img) #ПЕРПИСАТЬ НА ПОТОКИ И АСИНХРОН (МБ ОЧЕРЕДЬ ЗАПРОСОВ)
    except Exception as e:
        raise e
    return res