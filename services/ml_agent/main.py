from fastapi import FastAPI, HTTPException
from src.ai_scheduler import recognise
from src.models import Image
from settings import global_settings
import json
import uvicorn


app = FastAPI()

@app.post("/recognise")
async def ocr(img: Image):
    try:
        ret = await recognise(str(img.img_url))
        ret = json.loads(ret)
        return ret
    except Exception as e:
        raise HTTPException(status_code=500,detail=f"Error in recognising... {e}")
    
if __name__ == "__main__":
    print('staer')
    uvicorn.run(app=app,host=global_settings.host, port=global_settings.port)