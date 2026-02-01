
## Create ml_agent with paddle

```
docker build -t ml_agent

docker run -it -p 81:8080 --gpus all -e port=8080 -e host=0.0.0.0 --name ocr_agent ml_agent

```



## Create new_ml_agent with ollama - model deepseek-ai/DeepSeek-OCR

```
FROM ollama/ollama
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]


#!/bin/bash
set -e
ollama start &
sleep 10
ollama pull deepseek-ocr
tail -f /dev/null

docker build -t deepseek-ocr-ollama .

docker run -d --gpus all --name ocr-container -p 11434:11434 deepseek-ocr-ollama
```
