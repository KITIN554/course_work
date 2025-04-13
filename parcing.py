import requests
from bs4 import BeautifulSoup
import json
import re
from urllib.parse import urljoin
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Список начальных ссылок на кодексы
CODEX_LINKS = [
    "https://www.zakonrf.info/gk/",
    "https://www.zakonrf.info/nk/",
    "https://www.zakonrf.info/apk/",
    "https://www.zakonrf.info/gpk/",
    "https://www.zakonrf.info/kas/",
    "https://www.zakonrf.info/jk/",
    "https://www.zakonrf.info/zk/",
    "https://www.zakonrf.info/koap/",
    "https://www.zakonrf.info/sk/",
    "https://www.zakonrf.info/tk/",
    "https://www.zakonrf.info/uik/",
    "https://www.zakonrf.info/uk/",
    "https://www.zakonrf.info/upk/",
    "https://www.zakonrf.info/budjetniy-kodeks/",
    "https://www.zakonrf.info/gradostroitelniy-kodeks/",
    "https://www.zakonrf.info/lesnoy-kodeks/",
]

# Заголовки для запросов
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Количество воркеров
WORKERS = 20

# Папка для сохранения JSON-файлов
OUTPUT_DIR = "codex_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Функция для очистки текста от лишних пробелов и переносов строк
def clean_text(text):
    return re.sub(r'\s+', ' ', text.strip())

# Функция для получения содержимого статьи
def get_article_content(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Ищем содержимое статьи
        content_div = soup.find('div', class_='law-element__body content-body')
        if content_div:
            paragraphs = content_div.find_all(['p', 'div'], recursive=True)
            content = []
            for p in paragraphs:
                if 'insertion' in p.get('class', []):
                    continue
                text = clean_text(p.get_text())
                if text:
                    content.append(text)
            return content if content else ["No content found"]
        return ["No content found"]
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return ["Error fetching content"]

# Функция для парсинга дерева ссылок
def parse_tree(url, base_url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        tree = soup.find('ul', class_='law-element__tree')
        if not tree:
            return []

        result = []
        for item in tree.find_all('li', class_='law-element__tree-item'):
            link = item.find('a')
            if link:
                href = link.get('href')
                title = clean_text(link.get_text())
                full_url = urljoin(base_url, href)

                if 'law-element__tree-item_st' in item.get('class', []):
                    content = get_article_content(full_url)
                    result.append({
                        "type": "article",
                        "title": title,
                        "url": full_url,
                        "content": content
                    })
                else:
                    children = parse_tree(full_url, base_url)
                    result.append({
                        "type": "section" if 'law-element__tree-item_r' in item.get('class', []) else "chapter",
                        "title": title,
                        "url": full_url,
                        "children": children
                    })

        return result
    except requests.RequestException as e:
        print(f"Error parsing {url}: {e}")
        return []

# Функция для парсинга одного кодекса
def parse_codex(codex_url):
    try:
        print(f"Starting parsing: {codex_url}")
        codex_name = codex_url.split('/')[-2] if codex_url.endswith('/') else codex_url.split('/')[-1]
        codex_data = {
            "codex": codex_name,
            "url": codex_url,
            "structure": parse_tree(codex_url, codex_url)
        }

        # Сохранение данных кодекса в отдельный файл
        output_file = os.path.join(OUTPUT_DIR, f"{codex_name}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(codex_data, f, ensure_ascii=False, indent=4)
        print(f"Saved data for {codex_name} to {output_file}")

        return codex_data
    except Exception as e:
        print(f"Failed to parse {codex_url}: {e}")
        return None

# Основная функция для парсинга всех кодексов с использованием воркеров
def parse_all_codexes():
    all_data = []

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        # Запускаем задачи парсинга для каждого кодекса
        future_to_url = {executor.submit(parse_codex, url): url for url in CODEX_LINKS}
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                codex_data = future.result()
                if codex_data:
                    all_data.append(codex_data)
            except Exception as e:
                print(f"Error processing {url}: {e}")

    # Сохранение всех данных в общий файл
    all_output_file = os.path.join(OUTPUT_DIR, "all_codexes.json")
    with open(all_output_file, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)
    print(f"Saved all data to {all_output_file}")

    return all_data

# Запуск парсинга
if __name__ == "__main__":
    start_time = time.time()
    parse_all_codexes()
    print(f"Parsing completed in {time.time() - start_time:.2f} seconds")