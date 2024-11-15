import pandas as pd
import requests
import time
import random
import matplotlib.pyplot as plt
import os

def plot_price_hist():
    df_product = pd.read_csv('data/crawled_data.csv')
    plt.figure(figsize=(10, 6))
    plt.hist(df_product['price'].dropna(), bins=30, color='skyblue', edgecolor='black')
    plt.title('Price Distribution of Products')
    plt.xlabel('Price')
    plt.ylabel('Number of Products')

    os.makedirs('visualization', exist_ok=True)
    plt.savefig('visualization/price_distribution.png')
    return "Data visualization succeeded"


def plot_discount():
    df_product = pd.read_csv('data/crawled_data.csv')

    top5_products = df_product.sort_values(by='discount_rate', ascending=False).head(5)
    plt.figure(figsize=(10, 6))
    plt.bar(top5_products['product_name'], top5_products['discount_rate'], color='skyblue', edgecolor='black')
    plt.title('Top 5 Products with Highest Discount Rate', fontsize=14)
    plt.xlabel('Product Name', fontsize=12)
    plt.ylabel('Discount Rate (%)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()


    os.makedirs('visualization', exist_ok=True)
    plt.savefig('visualization/top5_discount.png')
    return "Data visualization succeeded"