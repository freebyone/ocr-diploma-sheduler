FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    gnupg2 \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb && \
    dpkg -i cuda-keyring_1.1-1_all.deb && \
    apt-get update

RUN apt-get install -y --no-install-recommends \
    cuda-runtime-12-6 \
    cuda-libraries-12-6 \
    libcublas-12-6 \
    libcufft-12-6 \
    libcurand-12-6 \
    libcusolver-12-6 \
    libcusparse-12-6 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install opencv-python-headless==4.10.0.84 \
    numpy>=1.21.0 \
    pillow>=9.0.0 \
    scipy>=1.7.0

RUN pip install paddlepaddle-gpu==3.2.1 -i https://www.paddlepaddle.org.cn/packages/stable/cu126/ \
    && pip install -U "paddleocr[doc-parser]" \
    && pip install https://paddle-whl.bj.bcebos.com/nightly/cu126/safetensors/safetensors-0.6.2.dev0-cp38-abi3-linux_x86_64.whl

ENV PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=True

WORKDIR /app

COPY ./ /app/

RUN pip install -r req.txt

CMD ["python", "main.py"]