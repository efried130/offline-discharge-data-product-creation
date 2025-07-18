# Stage 0 - Create from Python3.9.7 image
FROM python:3.12-slim-bookworm AS stage0

# Stage 1 - Debian dependencies
FROM stage0 AS stage1
RUN apt update \
    && DEBIAN_FRONTEND=noninteractive apt install -y curl zip python3-dev gcc build-essential libhdf5-serial-dev netcdf-bin libnetcdf-dev libgdal-dev

# Stage 2 - Create virtual environment and install dependencies
FROM stage1 AS stage2
COPY requirements.txt /app/requirements.txt
RUN /usr/local/bin/python3 -m venv /app/env
RUN /app/env/bin/pip install -r /app/requirements.txt

# Stage 3 - Copy MetroMan code
FROM stage2 AS stage3
COPY ./offline /app/offline/

# Stage 4 - Execute algorithm
FROM stage3 AS stage4
COPY run_offline.py /app/run_offline.py
LABEL version="1.0" \
	description="Containerized Offline module." \
	"confluence.contact"="ntebaldi@umASs.edu" \
	"algorithm.contact"="wei.263@osu.edu"
ENTRYPOINT ["/app/env/bin/python3", "/app/run_offline.py"]