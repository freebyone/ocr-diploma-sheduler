
## OCR model with paddle

```
docker build -t ml_agent

docker run -it -p 81:8080 --gpus all -e port=8080 -e host=0.0.0.0 --name ocr_agent ml_agent
```



## OCR with ollama - model deepseek-ai/DeepSeek-OCR

```
docker-compose up -d ollama
```

## Fast start 

```
docker-compose up -d 
```

## Service in project

```
services-lite-version:
    postgres
    minio
    pdf-processor-lite
    ollama
    ocr-worker-lite
    word-generator
    xlsx-parser
    xlsx-processor
    llm-parser-service
```
