from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import uvicorn
from src.ai_scheduler import recognise
from settings import global_settings
import json

class Image(BaseModel):
    url: HttpUrl

app = FastAPI()

@app.post("/recognise")
async def ocr(img: Image):
    try:
        ret = await recognise(img)
        ret = json.loads(ret)
        return ret
    except Exception as e:
        raise HTTPException(status_code=500,detail=f"Error in recognising... {e}")
    
if __name__ == "__main__":
    print('staer')
    uvicorn.run(app=app,host=global_settings.host, port=global_settings.port)