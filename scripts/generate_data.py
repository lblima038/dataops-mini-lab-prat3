import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker('pt_BR')  # Usar locale brasileiro para nomes e cidades

# Função para gerar customers
def generate_customers(n=3000):
    data = []
    for i in range(1, n+1):
        data.append({
            'customer_id': i,
            'customer_name': fake.name(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'signup_date': fake.date_between(start_date='-2y', end_date='today')
        })
    return pd.DataFrame(data)

# Função para gerar products
def generate_products(n=300):
    categories = ['Eletrônicos', 'Roupas', 'Livros', 'Casa', 'Esportes']
    data = []
    for i in range(1, n+1):
        data.append({
            'product_id': i,
            'product_name': f"{fake.word().capitalize()} {fake.word().capitalize()}",
            'category': random.choice(categories),
            'price': round(random.uniform(10, 1000), 2)
        })
    return pd.DataFrame(data)

# Função para gerar orders
def generate_orders(n=10000, customers_df=None):
    statuses = ['pending', 'shipped', 'delivered', 'cancelled']
    data = []
    for i in range(1, n+1):
        customer_id = random.randint(1, len(customers_df))
        data.append({
            'order_id': i,
            'customer_id': customer_id,
            'order_date': fake.date_between(start_date='-1y', end_date='today'),
            'status': random.choice(statuses)
        })
    return pd.DataFrame(data)

# Função para gerar order_items
def generate_order_items(n=20000, orders_df=None, products_df=None):
    data = []
    product_prices = products_df.set_index('product_id')['price'].to_dict()
    for i in range(1, n+1):
        order_id = random.randint(1, len(orders_df))
        product_id = random.randint(1, len(products_df))
        quantity = random.randint(1, 10)
        unit_price = product_prices[product_id]
        data.append({
            'order_item_id': i,
            'order_id': order_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': unit_price
        })
    return pd.DataFrame(data)

if __name__ == "__main__":
    # Gerar dados
    customers_df = generate_customers()
    products_df = generate_products()
    orders_df = generate_orders(customers_df=customers_df)
    order_items_df = generate_order_items(orders_df=orders_df, products_df=products_df)

    # Salvar como CSV
    customers_df.to_csv('data/raw/customers.csv', index=False)
    products_df.to_csv('data/raw/products.csv', index=False)
    orders_df.to_csv('data/raw/orders.csv', index=False)
    order_items_df.to_csv('data/raw/order_items.csv', index=False)

    print("Dados sintéticos gerados e salvos em data/raw/")