from pydantic import BaseModel, HttpUrl

class Image(BaseModel):
    img_url: str
