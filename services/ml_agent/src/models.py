from pydantic import BaseModel, HttpUrl

class Image(BaseModel):
    url: str
