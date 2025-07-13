import base64
import requests
import pandas as pd
import time
import io
from datetime import date
import pyarrow.parquet as pq
import pyarrow as pa
import boto3

REGION = "us-east-2"
BUCKET = "raw-dados-ibov"

def encode_payload(page, page_size=120):
    return base64.b64encode(
        f'{{"language":"pt-br","pageNumber":{page},"pageSize":{page_size},"index":"IBOV","segment":"1"}}'.encode()
    ).decode()

def fetch_ibov_all():
    results, page = [], 1
    while True:
        url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{encode_payload(page)}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data = r.json().get("results", [])
        if not data:
            break
        results.extend(data)
        page += 1
        time.sleep(0.2)

    df = pd.DataFrame(results)
    df["data_coleta"] = pd.to_datetime(date.today())
    return df

def to_parquet_buffer(df):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buf)
    buf.seek(0)
    return buf

def upload_to_s3(buf, key):
    try:
        s3 = boto3.client("s3", region_name=REGION)
        s3.upload_fileobj(buf, BUCKET, key)
        print(f"✅ Upload concluído: s3://{BUCKET}/{key}")
    except Exception as e:
        print("❌ Falha no upload:", str(e))

# Executa diretamente no Glue
df = fetch_ibov_all()
print("📄 DataFrame coletado:", df.shape)
print(df.head())

if df.empty:
    print("⚠️ DataFrame vazio. Nenhum dado foi carregado da B3.")
else:
    key = f"ibov/date={date.today().isoformat()}/ibov.parquet"
    buffer = to_parquet_buffer(df)
    upload_to_s3(buffer, key)