FROM python:3.11-slim

WORKDIR /app

# Install git so we can clone and merge the PRs
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Conditionally fetch and apply the 4 PRs for pytoyoda
RUN git clone https://github.com/pytoyoda/pytoyoda.git /tmp/pytoyoda && \
    cd /tmp/pytoyoda && \
    git fetch origin main pull/268/head:pr268 pull/269/head:pr269 pull/270/head:pr270 pull/271/head:pr271 && \
    if git merge-base --is-ancestor pr270 origin/main && git merge-base --is-ancestor pr271 origin/main; then \
        echo "PRs are already merged, skipping patch"; \
    else \
        git config user.email "docker@build.local" && \
        git config user.name "Docker Build" && \
        git checkout -b custom-main origin/main && \
        git merge --no-edit pr268 pr269 pr270 pr271 && \
        pip install --no-cache-dir --force-reinstall . ; \
    fi && \
    rm -rf /tmp/pytoyoda

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]