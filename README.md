# Coleta e Armazenamento de Dados do IBOV no S3

Este script Python realiza a raspagem da carteira teórica do índice **IBOV** diretamente do endpoint público da B3, salva os dados no formato **Parquet** e envia para um bucket **S3** na AWS, em um caminho particionado pela data da coleta.

---

## 📌 Funcionalidades

1. **Raspagem de dados da B3**
   - Consulta o endpoint `GetPortfolioDay` da B3 usando paginação.
   - Decodifica o payload necessário via Base64.
   - Gera um `DataFrame` com as informações da carteira do IBOV.
   - Adiciona a coluna `data_coleta` com a data atual.

2. **Conversão para Parquet**
   - Utiliza `PyArrow` para converter o `DataFrame` em formato **Parquet**.
   - Mantém os dados em memória (`BytesIO`) para evitar escrita em disco.

3. **Envio ao S3**
   - Conecta ao bucket definido via `boto3`.
   - Envia o arquivo para `s3://<bucket>/ibov/date=YYYY-MM-DD/ibov.parquet`.
   - Exibe mensagens de sucesso ou erro.

---

## 🛠️ Pré-requisitos

- **Python 3.8+**
- Bibliotecas:
  ```bash
  pip install pandas requests pyarrow boto3

<img width="1187" height="545" alt="image" src="https://github.com/user-attachments/assets/edc42d99-fb21-4500-b250-c2b9855e0f6d" />
