import os
import requests
import logging

class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"
    
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB_API_KEY not found in environment")
        self.session = requests.Session()
        self.session.params = {"api_key": self.api_key}

    def _get(self, endpoint, params=None):
        url = f"{self.BASE_URL}{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_trending_movies(self, time_window="day"):
        return self._get(f"/trending/movie/{time_window}")

    def get_popular_movies(self, page=1):
        return self._get("/movie/popular", params={"page": page})

    def get_top_rated_movies(self, page=1):
        return self._get("/movie/top_rated", params={"page": page})

    def get_movie_details(self, movie_id):
        return self._get(f"/movie/{movie_id}")

    def get_genres(self):
        return self._get("/genre/movie/list")

    def discover_movies(self, year, page=1):
        params = {
            "primary_release_year": year,
            "page": page,
            "sort_by": "popularity.desc"
        }
        return self._get("/discover/movie", params=params)
