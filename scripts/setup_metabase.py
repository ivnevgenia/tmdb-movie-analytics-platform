import requests
import time
import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MB_URL = "http://metabase:3000"
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "metabase_password123")
ADMIN_NAME = os.getenv("METABASE_ADMIN_NAME", "Admin")
SETUP_TOKEN = os.getenv("METABASE_SETUP_TOKEN")

WH_HOST = os.getenv("WH_HOST", "postgres")
WH_DB = os.getenv("WH_DB", "tmdb_warehouse")
WH_USER = os.getenv("WH_USER", "postgres")
WH_PASS = os.getenv("WH_PASSWORD", "postgres")


def wait_for_metabase():
    logger.info("Waiting for Metabase to be ready...")
    for i in range(60):
        try:
            res = requests.get(f"{MB_URL}/api/health")
            if res.status_code == 200:
                logger.info("Metabase is up!")
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


def setup_metabase():
    setup_token = SETUP_TOKEN
    if not setup_token:
        res = requests.get(f"{MB_URL}/api/setup/admin_token")
        data = res.json()
        if isinstance(data, dict):
            setup_token = data.get("setup_token")
        else:
            setup_token = data

    session_id = None
    try:
        login_res = requests.post(f"{MB_URL}/api/session",
                                  json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
        if login_res.status_code == 200:
            session_id = login_res.json()["id"]
            logger.info("Already setup, logged in successfully.")
        else:
            logger.info("Performing initial setup...")
            setup_data = {
                "token": setup_token,
                "user": {
                    "first_name": ADMIN_NAME,
                    "last_name": "User",
                    "email": ADMIN_EMAIL,
                    "password": ADMIN_PASSWORD
                },
                "prefs": {
                    "allow_tracking": False,
                    "site_name": "TMDb Movie Analytics"
                }
            }
            res = requests.post(f"{MB_URL}/api/setup", json=setup_data)
            res.raise_for_status()
            session_id = res.json()["id"]
    except Exception as e:
        logger.error(f"Setup error: {e}")
        raise

    headers = {"X-Metabase-Session": session_id}

    # Добавление / получение базы данных
    logger.info("Adding/Checking Postgres Warehouse...")
    res_db = requests.get(f"{MB_URL}/api/database", headers=headers)
    db_list = res_db.json()
    if isinstance(db_list, dict) and "data" in db_list:
        db_list = db_list["data"]
    elif not isinstance(db_list, list):
        db_list = []
    db_id = next((d["id"] for d in db_list if isinstance(d, dict) and d.get("name") == "TMDb Warehouse"), None)
    if not db_id:
        db_data = {
            "name": "TMDb Warehouse",
            "engine": "postgres",
            "details": {
                "host": WH_HOST,
                "port": 5432,
                "dbname": WH_DB,
                "user": WH_USER,
                "password": WH_PASS,
                "schema": "gold_tmdb",
                "ssl": False
            }
        }
        res = requests.post(f"{MB_URL}/api/database", json=db_data, headers=headers)
        db_id = res.json()["id"]

    # Очистка старых дашбордов и карточек
    logger.info("Cleaning up ALL old dashboards and cards...")
    res_dash = requests.get(f"{MB_URL}/api/dashboard", headers=headers)
    for d in res_dash.json():
        requests.delete(f"{MB_URL}/api/dashboard/{d['id']}", headers=headers)
    res_cards = requests.get(f"{MB_URL}/api/card", headers=headers)
    for c in res_cards.json():
        requests.delete(f"{MB_URL}/api/card/{c['id']}", headers=headers)

    # Создание нового дашборда с шириной full
    logger.info("Creating Final TMDb Movie Dashboard...")
    dash_data = {
        "name": "TMDb Movie Analytics Dashboard",
        "description": "Unified dashboard for movie insights",
        "width": "full"
    }
    res = requests.post(f"{MB_URL}/api/dashboard", json=dash_data, headers=headers)
    dash_id = res.json()["id"]
    logger.info(f"Dashboard created with ID: {dash_id}")

    # Получаем структуру дашборда, чтобы узнать ID первой вкладки (если есть)
    dash_res = requests.get(f"{MB_URL}/api/dashboard/{dash_id}", headers=headers)
    current_dash = dash_res.json()
    default_tab_id = None
    tabs = current_dash.get("tabs", [])
    if tabs:
        default_tab_id = tabs[0]["id"]
        logger.info(f"Using tab_id={default_tab_id}")

    # Функция для создания карточки с привязкой к дашборду и описанием
    def create_card_on_dashboard(name, sql, display="bar", viz_settings=None, description=None):
        card_payload = {
            "name": name,
            "dataset_query": {
                "database": db_id,
                "type": "native",
                "native": {"query": sql}
            },
            "display": display,
            "visualization_settings": viz_settings or {},
            "dashboard_id": dash_id,
        }
        if default_tab_id is not None:
            card_payload["dashboard_tab_id"] = default_tab_id
        if description:
            card_payload["description"] = description
        logger.info(f"Creating card '{name}' on dashboard...")
        c_res = requests.post(f"{MB_URL}/api/card", json=card_payload, headers=headers)
        c_res.raise_for_status()
        return c_res.json()["id"]

    logger.info("Creating all cards...")

    # 1. Total Movies in Database
    sql_total = "SELECT total_movies_in_db as \"Total Movies\" FROM gold_tmdb.daily_total_movies"
    card_total_id = create_card_on_dashboard(
        name="Total Movies in Database",
        sql=sql_total,
        display="scalar",
        description="Total number of movies in the database from both API and historical sources."
    )

    # 2. All-Time Highest Rated Movies
    sql_top_rated = """
        SELECT poster_url as "Poster", title as "Title", vote_average as "Rating", vote_count as "Votes"
        FROM gold_tmdb.daily_top_rated_movies
        LIMIT 10
    """
    viz_table = {"column_settings": {'["name","Poster"]': {"view_as": "image", "image_width": 60}}}
    card_top_rated_id = create_card_on_dashboard(
        name="All-Time Highest Rated Movies",
        sql=sql_top_rated,
        display="table",
        viz_settings=viz_table,
        description="Top 10 movies with the highest average rating (all-time) from TMDB top_rated list."
    )

    # 3. Currently Trending Movies
    sql_most_popular = """
        SELECT poster_url as "Poster", title as "Title", popularity as "Popularity", release_date as "Release Date"
        FROM gold_tmdb.daily_most_popular_movies
        LIMIT 10
    """
    card_most_popular_id = create_card_on_dashboard(
        name="Currently Trending Movies",
        sql=sql_most_popular,
        display="table",
        viz_settings=viz_table,
        description="Top 10 movies by popularity based on daily snapshot from TMDB."
    )

    # 4. Genre Distribution (All Time) – pie chart
    sql_genre_pie = "SELECT genre_name as \"Genre\" , movie_count as \"Movie Count\" FROM gold_tmdb.historical_genre_distribution"
    viz_pie = {
        "pie.show_legend": True,               # Show legend = true
        "pie.show_total": False,               # Show total = false
        "pie.show_labels": False,              # Show labels = false
        "pie.show_legend_percent": True,       # Show percentages in legend
        "pie.percent_visibility": "legend"     # Percentages in legend (not inside)
    }
    card_genre_pie_id = create_card_on_dashboard(
        name="Genre Distribution (All Time)",
        sql=sql_genre_pie,
        display="pie",
        viz_settings=viz_pie,
        description="Distribution of movies by genre across all time."
    )

    # 5. Average Rating by Genre (Historical) – row chart
    sql_hist_genres = """
        SELECT genre_name as "Genre", ROUND(CAST(AVG(avg_rating) AS NUMERIC), 2) as "Average Rating"
        FROM gold_tmdb.agg_historical_analysis
        WHERE avg_rating > 0
        GROUP BY genre_name
        ORDER BY "Average Rating" DESC
    """
    viz_row = {
        "graph.dimensions": ["Genre"],
        "graph.metrics": ["Average Rating"],
        "graph.y_axis.title_text": "",          # убрать подпись оси Y
        "graph.x_axis.title_text": "",          # убрать подпись оси X
        "graph.y_axis.min": 5,
        "graph.show_values": True,              # Show values on data points
        "graph.label_value_frequency": "all"    # all for all points
    }
    card_hist_genres_id = create_card_on_dashboard(
        name="Average Rating by Genre (Historical)",
        sql=sql_hist_genres,
        display="row",
        viz_settings=viz_row,
        description="Average movie rating for each genre based on historical data (2000–present)."
    )

    # 6. Movies Produced by Year (Historical) – bar chart
    sql_prod_vol = """
        SELECT release_year as "Year", SUM(movie_count) as "Movies Produced"
        FROM gold_tmdb.agg_historical_analysis
        GROUP BY release_year
        ORDER BY release_year ASC
    """
    viz_bar = {
        "graph.dimensions": ["Year"],
        "graph.metrics": ["Movies Produced"],
        "graph.x_axis.title_text": "Release Year",
        "graph.y_axis.title_text": "Number of Movies",
        "graph.x_axis.scale": "ordinal",        # Scale = ordinal
        "graph.x_axis.labels.rotate": 90        # Rotate labels 90 degrees
    }
    card_prod_vol_id = create_card_on_dashboard(
        name="Movies Produced by Year (Historical)",
        sql=sql_prod_vol,
        display="bar",
        viz_settings=viz_bar,
        description="Number of movies released per year from historical backfill data."
    )

    # Ждём, пока Metabase создаст dashcard записи
    logger.info("Waiting for dashcards to be created...")
    time.sleep(3)

    # Получаем актуальный дашборд с dashcards
    dash_res = requests.get(f"{MB_URL}/api/dashboard/{dash_id}", headers=headers)
    current_dash = dash_res.json()
    existing_dashcards = current_dash.get("dashcards", [])
    if not existing_dashcards:
        logger.error("No dashcards found after card creation!")
        return
    logger.info(f"Found {len(existing_dashcards)} dashcards.")

    # Позиции и размеры из нового JSON (dashboard 93)
    positions = [
        {"cardId": card_total_id, "row": 0, "col": 0, "sizeX": 24, "sizeY": 3},
        {"cardId": card_top_rated_id, "row": 3, "col": 12, "sizeX": 12, "sizeY": 11},
        {"cardId": card_most_popular_id, "row": 3, "col": 0, "sizeX": 12, "sizeY": 11},
        {"cardId": card_genre_pie_id, "row": 14, "col": 0, "sizeX": 14, "sizeY": 10},
        {"cardId": card_hist_genres_id, "row": 14, "col": 14, "sizeX": 10, "sizeY": 19},
        {"cardId": card_prod_vol_id, "row": 24, "col": 0, "sizeX": 14, "sizeY": 9}
    ]

    # Обновляем координаты в existing_dashcards
    pos_map = {p["cardId"]: p for p in positions}
    updated_dashcards = []
    for dc in existing_dashcards:
        card_id = dc["card_id"]
        if card_id in pos_map:
            p = pos_map[card_id]
            dc["row"] = p["row"]
            dc["col"] = p["col"]
            dc["size_x"] = p["sizeX"]
            dc["size_y"] = p["sizeY"]
        updated_dashcards.append(dc)

    # Подготовка payload для PUT /api/dashboard/{id}
    # Удаляем read-only поля
    readonly_fields = [
        "created_at", "updated_at", "last-edit-info", "last_used_param_values", "param_fields",
        "collection_position", "collection_authority_level", "entity_id", "creator_id", "public_uuid",
        "position", "last_viewed_at", "view_count", "dependency_analysis_version", "initially_published_at",
        "can_write", "can_restore", "can_delete", "archived_directly", "made_public_by_id",
        "embedding_type", "is_remote_synced", "moderation_reviews", "collection"
    ]
    payload = {k: v for k, v in current_dash.items() if k not in readonly_fields}
    payload["dashcards"] = updated_dashcards

    logger.info("Updating dashboard via PUT /api/dashboard/{}".format(dash_id))
    put_res = requests.put(f"{MB_URL}/api/dashboard/{dash_id}", json=payload, headers=headers)
    if put_res.status_code == 200:
        logger.info("Dashboard updated successfully!")
    else:
        logger.error(f"PUT failed: {put_res.status_code} - {put_res.text}")
        return

    # Финальная проверка
    time.sleep(1)
    dash_res = requests.get(f"{MB_URL}/api/dashboard/{dash_id}", headers=headers)
    final_dash = dash_res.json()
    logger.info(f"Final dashboard has {len(final_dash.get('dashcards', []))} cards.")
    logger.info(f"Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    logger.info("Metabase unified dashboard setup complete!")


if __name__ == "__main__":
    if wait_for_metabase():
        setup_metabase()