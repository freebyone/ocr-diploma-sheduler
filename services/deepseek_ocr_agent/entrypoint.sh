#!/bin/bash
set -e
ollama start &
sleep 10
ollama pull deepseek-ocr
tail -f /dev/null