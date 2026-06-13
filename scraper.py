import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime

BASE_URL = "https://www.dna-tools.co.il"

def get_categories():
    try:
        res = requests.get(BASE_URL, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        cats = []
        for a in soup.select("nav a, .menu a, .categories a"):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if text and "dna-tools" in href and len(text) > 2:
                cats.append({"name": text, "url": href})
        return cats[:20]
    except:
        return []

def get_products_from_page(url):
    try:
        res = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        products = []
        for item in soup.select(".product, .woocommerce-loop-product__link, li.product"):
            name_el = item.select_one("h2, .woocommerce-loop-product__title, .product-title")
            price_el = item.select_one(".price, .woocommerce-Price-amount")
            img_el = item.select_one("img")
            link_el = item.select_one("a")
            if name_el:
                products.append({
                    "name": name_el.get_text(strip=True),
                    "price": price_el.get_text(strip=True) if price_el else "",
                    "image": img_el.get("src", "") if img_el else "",
                    "url": link_el.get("href", "") if link_el else "",
                })
        return products
    except:
        return []

def get_new_products():
    urls = [
        f"{BASE_URL}/?orderby=date",
        f"{BASE_URL}/shop/?orderby=date",
        f"{BASE_URL}/?orderby=popularity",
    ]
    products = []
    for url in urls:
        products += get_products_from_page(url)
        time.sleep(1)
    seen = set()
    unique = []
    for p in products:
        if p["name"] not in seen and p["name"]:
            seen.add(p["name"])
            unique.append(p)
    return unique[:50]

def main():
    print("Starting daily scan of dna-tools.co.il...")

    data = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "categories": get_categories(),
        "products": get_new_products(),
        "total_products": 0,
        "status": "success"
    }

    data["total_products"] = len(data["products"])

    with open("products.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Done! Found {data['total_products']} products, {len(data['categories'])} categories.")

if __name__ == "__main__":
    main()
