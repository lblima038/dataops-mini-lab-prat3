import pandas as pd
import duckdb
import os
import logging
from datetime import datetime
import argparse

# Configurar logging
logging.basicConfig(
    filename='logs/etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Caminhos
raw_path = 'data/raw/'
db_path = 'data/ecommerce.db'

# Schema esperado para validação
expected_schemas = {
    'customers.csv': ['customer_id', 'customer_name', 'city', 'state', 'signup_date'],
    'products.csv': ['product_id', 'product_name', 'category', 'price'],
    'orders.csv': ['order_id', 'customer_id', 'order_date', 'status'],
    'order_items.csv': ['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price']
}

# Função para validação de schema
def validate_schema(file_path, expected_columns):
    try:
        df = pd.read_csv(file_path, nrows=5)  # Ler apenas primeiras linhas para validação
        if not all(col in df.columns for col in expected_columns):
            raise ValueError(f"Colunas faltando em {file_path}")
        logging.info(f"Schema válido para {file_path}")
        return True
    except Exception as e:
        logging.error(f"Erro na validação de schema para {file_path}: {e}")
        return False

# Função para extração com validação
def extract_data(date_filter=None):
    logging.info("Iniciando extração de dados")
    files = ['customers.csv', 'products.csv', 'orders.csv', 'order_items.csv']
    dataframes = {}

    for file in files:
        file_path = os.path.join(raw_path, file)
        if not validate_schema(file_path, expected_schemas[file]):
            raise ValueError(f"Schema inválido para {file}")

        df = pd.read_csv(file_path)
        dataframes[file.replace('.csv', '_df')] = df
        logging.info(f"Extraído {len(df)} registros de {file}")

    # Aplicar filtro de data se fornecido (para reprocessamento)
    if date_filter:
        orders_df = dataframes['orders_df']
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
        orders_df = orders_df[orders_df['order_date'] >= date_filter]
        dataframes['orders_df'] = orders_df
        logging.info(f"Aplicado filtro de data: {date_filter}")

    return dataframes['customers_df'], dataframes['products_df'], dataframes['orders_df'], dataframes['order_items_df']

# Função para transformação
def transform_data(customers_df, products_df, orders_df, order_items_df):
    logging.info("Iniciando transformação de dados")
    # Ajustar tipos
    customers_df['signup_date'] = pd.to_datetime(customers_df['signup_date'])
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])

    # Tratar nulos (se houver, drop)
    customers_df.dropna(inplace=True)
    products_df.dropna(inplace=True)
    orders_df.dropna(inplace=True)
    order_items_df.dropna(inplace=True)

    # Calcular total por item
    order_items_df['total_item'] = order_items_df['quantity'] * order_items_df['unit_price']

    # Junções para base consolidada
    # Primeiro, order_items com products
    consolidated = order_items_df.merge(products_df, on='product_id', how='left')
    # Depois, com orders
    consolidated = consolidated.merge(orders_df, on='order_id', how='left')
    # Depois, com customers
    consolidated = consolidated.merge(customers_df, on='customer_id', how='left')

    # Selecionar colunas relevantes para análise
    consolidated = consolidated[['order_item_id', 'order_id', 'customer_id', 'product_id',
                                 'order_date', 'customer_name', 'city', 'state',
                                 'product_name', 'category', 'quantity', 'unit_price', 'total_item', 'status']]

    # Adicionar particionamento por data (year_month)
    consolidated['year_month'] = consolidated['order_date'].dt.strftime('%Y-%m')

    logging.info("Transformação concluída")
    return customers_df, products_df, orders_df, order_items_df, consolidated

# Função para carga com idempotência
def load_data(customers_df, products_df, orders_df, order_items_df, consolidated_df, force_reload=False):
    logging.info("Iniciando carga de dados")
    # Conectar ao DuckDB
    con = duckdb.connect(db_path)

    # Verificar idempotência: se tabelas já existem e têm dados, pular inserção (a menos que force_reload)
    if not force_reload:
        tables = ['raw_customers', 'raw_products', 'raw_orders', 'raw_order_items', 'treated_customers', 'treated_products', 'treated_orders', 'treated_order_items', 'analytics_consolidated']
        for table in tables:
            if con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}'").fetchone()[0] > 0:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count > 0:
                    logging.warning(f"Tabela {table} já tem {count} registros. Pulando inserção para idempotência.")
                    con.close()
                    return

    # Criar tabelas brutas
    con.execute("CREATE OR REPLACE TABLE raw_customers AS SELECT * FROM customers_df")
    con.execute("CREATE OR REPLACE TABLE raw_products AS SELECT * FROM products_df")
    con.execute("CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM orders_df")
    con.execute("CREATE OR REPLACE TABLE raw_order_items AS SELECT * FROM order_items_df")

    # Criar tabelas tratadas
    con.execute("CREATE OR REPLACE TABLE treated_customers AS SELECT * FROM customers_df")
    con.execute("CREATE OR REPLACE TABLE treated_products AS SELECT * FROM products_df")
    con.execute("CREATE OR REPLACE TABLE treated_orders AS SELECT * FROM orders_df")
    con.execute("CREATE OR REPLACE TABLE treated_order_items AS SELECT * FROM order_items_df")

    # Criar tabela analítica com particionamento (simulado via coluna)
    con.execute("CREATE OR REPLACE TABLE analytics_consolidated AS SELECT * FROM consolidated_df")

    con.close()
    logging.info("Carga de dados concluída")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Pipeline ETL para E-commerce')
    parser.add_argument('--date-filter', type=str, help='Data mínima para reprocessamento (YYYY-MM-DD)', default=None)
    parser.add_argument('--force-reload', action='store_true', help='Forçar recarga mesmo se dados existirem')
    args = parser.parse_args()

    try:
        # Converter date_filter para datetime se fornecido
        date_filter = pd.to_datetime(args.date_filter) if args.date_filter else None

        # Extração
        customers_df, products_df, orders_df, order_items_df = extract_data(date_filter)

        # Transformação
        customers_df, products_df, orders_df, order_items_df, consolidated_df = transform_data(
            customers_df, products_df, orders_df, order_items_df)

        # Carga
        load_data(customers_df, products_df, orders_df, order_items_df, consolidated_df, args.force_reload)

        logging.info("Pipeline ETL concluído com sucesso")
        print("Pipeline ETL concluído.")
    except Exception as e:
        logging.error(f"Erro no pipeline ETL: {e}")
        print(f"Erro: {e}")

if __name__ == "__main__":
    # Extração
    customers_df, products_df, orders_df, order_items_df = extract_data()

    # Transformação
    customers_df, products_df, orders_df, order_items_df, consolidated_df = transform_data(
        customers_df, products_df, orders_df, order_items_df)

    # Carga
    load_data(customers_df, products_df, orders_df, order_items_df, consolidated_df)

    print("Pipeline ETL concluído.")