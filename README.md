# HomeSearcher — Агрегатор недвижимости Варшавы 🏠

Приложение для поиска жилья в Варшаве. Собирает объявления с **Otodom.pl** и **Gratka.pl**, оценивает близость к паркам, школам, садикам, магазинам и транспорту, показывает всё на интерактивной карте.

## Возможности

- 🏢 Квартиры и 🏠 дома — переключатель с фильтрацией
- 💰 Аренда и покупка — видны оба типа сделок
- 🗺️ Интерактивная карта (Leaflet + OpenStreetMap)
- 📊 Proximity-рейтинг (0–50 баллов): парки, школы, садики, магазины, транспорт
- 🔍 Фильтры: цена, площадь, комнаты, минимальный рейтинг, источник
- 🔄 **Живая загрузка** — подтягивание объявлений прямо из браузера (выбор источника)
- 🔗 Прямые ссылки на оригинальные объявления (Otodom / Gratka)
- 🌙 Тёмная / светлая тема
- 📱 Адаптивный дизайн (мобильный + десктоп)

## Быстрый старт

### 1. Установить зависимости

```bash
pip install -r requirements.txt
```

### 2. Запустить сервер (рекомендуется)

```bash
python serve.py               # http://localhost:8080
python serve.py --port 3000   # другой порт
```

Откроется веб-приложение с кнопкой 🔄 для загрузки свежих объявлений прямо из браузера.

### 2b. Или собрать данные вручную

```bash
# Скрапинг объявлений с Otodom + Gratka (Варшава)
python fetch_listings.py
python fetch_listings.py --source otodom    # только Otodom
python fetch_listings.py --source gratka    # только Gratka
python fetch_listings.py --max-pages 3      # ограничить страницы

# Обогатить данные proximity-рейтингом (OpenStreetMap Overpass API)
python enrich_listings.py
```

Затем откройте `index.html` в браузере. Данные загрузятся из `data/listings.json`.

## Скрипты

### `serve.py`

Локальный HTTP-сервер. Отдаёт фронтенд и предоставляет API для живой загрузки.

```bash
python serve.py                # http://localhost:8080
```

**API:**
- `GET /api/fetch?source=otodom|gratka|all&pages=3` — запустить скрапер
- `GET /api/status` — статус загрузки
- `GET /api/listings` — текущие объявления (JSON)
- `GET /api/enrich?batch=50` — запустить обогащение proximity

### `fetch_listings.py`

Скрапит Otodom.pl (`__NEXT_DATA__` JSON) и Gratka.pl (HTML). Геокодирует через Photon API.

```bash
python fetch_listings.py                    # все категории, до 10 страниц
python fetch_listings.py --max-pages 3      # максимум 3 страницы на категорию
python fetch_listings.py --source otodom    # только Otodom
python fetch_listings.py --source gratka    # только Gratka
python fetch_listings.py --no-geocode       # без геокодирования
```

Результат: `data/listings_raw.json`

### `enrich_listings.py`

Для каждого объявления запрашивает OpenStreetMap Overpass API и считает proximity score.

```bash
python enrich_listings.py              # обработать все необогащённые
python enrich_listings.py --batch 10   # обработать 10 штук
python enrich_listings.py --force      # пересчитать все
```

Результат: `data/listings.json`

## Proximity-рейтинг

Каждая категория оценивается от 0 до 10 баллов:

| Категория | Радиус поиска | Что ищет |
|-----------|---------------|----------|
| 🌳 Парки | 1 км | `leisure=park` |
| 🏫 Школы | 1 км | `amenity=school` |
| 👶 Садики | 1 км | `amenity=kindergarten` |
| 🛒 Магазины | 1–1.5 км | `shop=mall`, `shop=supermarket` |
| 🚌 Транспорт | 0.5–1.5 км | автобусы, трамваи, метро, ж/д |

**Итого: 0–50 баллов**. Чем больше объектов рядом и чем они ближе, тем выше балл.

## Технологии

- **Frontend**: Vanilla JS, Leaflet, MarkerCluster — один HTML файл
- **Backend**: Python (BeautifulSoup + lxml) — сбор данных + лёгкий HTTP сервер
- **Источники**: Otodom.pl, Gratka.pl (скрапинг HTML/JSON)
- **API**: OpenStreetMap Overpass (proximity), Photon/komoot (геокодирование)
- **Деплой**: `python serve.py` (локальный сервер)

## Структура

```
HomeSearcher/
├── serve.py              # HTTP сервер + API для живой загрузки
├── fetch_listings.py     # Скрапер Otodom + Gratka
├── enrich_listings.py    # Proximity scoring (Overpass API)
├── index.html            # Фронтенд (карта + фильтры + live fetch)
├── requirements.txt      # Python зависимости
├── README.md
└── data/
    ├── listings_raw.json # Сырые данные с Otodom + Gratka
    └── listings.json     # Обогащённые данные (финальные)
```
