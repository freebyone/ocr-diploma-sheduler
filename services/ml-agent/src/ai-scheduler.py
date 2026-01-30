from paddleocr import PaddleOCRVL
pipeline = PaddleOCRVL()
output = pipeline.predict("https://i.pinimg.com/originals/07/bb/92/07bb92b723e735071d2e8d308b4e6a91.jpg")
for res in output:
    res.print()
    res.save_to_json(save_path="output")
    res.save_to_markdown(save_path="output")