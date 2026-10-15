FROM apache/airflow:2.9.2

USER root

# Install dependencies required for SQL Server connectivity
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        apt-transport-https \
        gnupg2 \
        unixodbc-dev \
        docker.io \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft package repository
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" \
    > /etc/apt/sources.list.d/mssql-release.list

# Install Microsoft SQL Server ODBC Driver 18
RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python libraries
RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    pyodbc
