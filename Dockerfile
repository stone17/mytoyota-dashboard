FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install git, fetch PRs, apply logic, install pytoyoda, and clean up in a single layer
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/* && \
    git clone https://github.com/pytoyoda/pytoyoda.git /tmp/pytoyoda && \
    cd /tmp/pytoyoda && \
    git fetch origin main pull/268/head:pr268 pull/269/head:pr269 pull/270/head:pr270 pull/271/head:pr271 && \
    if git merge-base --is-ancestor pr268 origin/main && \
       git merge-base --is-ancestor pr269 origin/main && \
       git merge-base --is-ancestor pr270 origin/main && \
       git merge-base --is-ancestor pr271 origin/main; then \
        echo "All PRs merged. Installing from upstream main." && \
        git checkout origin/main; \
    else \
        echo "PRs not merged. Applying patches." && \
        git config user.email "docker@build.local" && \
        git config user.name "Docker Build" && \
        git checkout -b custom-main origin/main && \
        git merge --no-edit pr268 pr269 pr270 pr271; \
    fi && \
    pip install --no-cache-dir --force-reinstall . && \
    cd /app && \
    rm -rf /tmp/pytoyoda && \
    apt-get purge -y --auto-remove git

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]