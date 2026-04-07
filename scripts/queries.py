import duckdb

db_path = 'data/ecommerce.db'

def run_queries():
    con = duckdb.connect(db_path)

    print("1. Faturamento total por mês:")
    result1 = con.execute("""
        SELECT strftime('%Y-%m', order_date) as month, SUM(total_item) as revenue
        FROM analytics_consolidated
        GROUP BY month
        ORDER BY month
    """).fetchall()
    for row in result1:
        print(f"Mês: {row[0]}, Receita: R$ {row[1]:.2f}")

    print("\n2. Faturamento por categoria:")
    result2 = con.execute("""
        SELECT category, SUM(total_item) as revenue
        FROM analytics_consolidated
        GROUP BY category
        ORDER BY revenue DESC
    """).fetchall()
    for row in result2:
        print(f"Categoria: {row[0]}, Receita: R$ {row[1]:.2f}")

    print("\n3. Quantidade de pedidos por estado:")
    result3 = con.execute("""
        SELECT state, COUNT(DISTINCT order_id) as orders_count
        FROM analytics_consolidated
        GROUP BY state
        ORDER BY orders_count DESC
    """).fetchall()
    for row in result3:
        print(f"Estado: {row[0]}, Pedidos: {row[1]}")

    print("\n4. Ticket médio por cliente (top 10):")
    result4 = con.execute("""
        SELECT customer_id, customer_name, SUM(total_item) / COUNT(DISTINCT order_id) as avg_ticket
        FROM analytics_consolidated
        GROUP BY customer_id, customer_name
        ORDER BY avg_ticket DESC
        LIMIT 10
    """).fetchall()
    for row in result4:
        print(f"Cliente: {row[1]} (ID: {row[0]}), Ticket Médio: R$ {row[2]:.2f}")

    print("\n5. Top 10 produtos mais vendidos:")
    result5 = con.execute("""
        SELECT product_name, SUM(quantity) as total_quantity
        FROM analytics_consolidated
        GROUP BY product_name
        ORDER BY total_quantity DESC
        LIMIT 10
    """).fetchall()
    for row in result5:
        print(f"Produto: {row[0]}, Quantidade Vendida: {row[1]}")

    con.close()

if __name__ == "__main__":
    run_queries()