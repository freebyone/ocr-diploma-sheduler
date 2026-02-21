
## Create ml_agent with paddle

```
docker build -t ml_agent

docker run -it -p 81:8080 --gpus all -e port=8080 -e host=0.0.0.0 --name ocr_agent ml_agent
```



## ml_agent with ollama - model deepseek-ai/DeepSeek-OCR

```
docker-compose up -d ollama
```

## Fast start 

```
docker-compose up -d 
```

## Service in project

```
services:
    postgres
    kafka
    kafka-ui
    minio
    redis
    pdf-processor
    ollama
    ocr-worker
```