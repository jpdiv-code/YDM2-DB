import requests
import time
import urllib.parse
import json
import os


class SafeRequester:
    def __init__(self, rate_limit, force_cache=False):
        self.rate_limit = rate_limit
        self.force_cache = force_cache
        self.request_timestamps = []

    def get(self, url):
        cache_filename = f"cache/{hash(url)}.json"
        if self.force_cache:
            if os.path.exists(cache_filename):
                with open(cache_filename, "r") as f:
                    return json.load(f)

        if len(self.request_timestamps) >= self.rate_limit:
            time_diff = self.request_timestamps[-1] - self.request_timestamps[0]
            if time_diff < 1:
                print(f"Rate limit reached, sleeping for {1 - time_diff} seconds")
                time.sleep(1 - time_diff)
            self.request_timestamps.pop(0)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        self.request_timestamps.append(time.time())

        if self.force_cache:
            os.makedirs(os.path.dirname(cache_filename), exist_ok=True)
            with open(cache_filename, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=True)

        return data


# ygoprodeck.com has a rate limit of 20 requests per second
safe_req = SafeRequester(rate_limit=19)


def get_archetypes_list():
    data = safe_req.get("https://db.ygoprodeck.com/api/v7/archetypes.php")
    archetypes = []
    for entry in data:
        archetypes.append(entry["archetype_name"])
    return archetypes


def get_archetype_card_ids(archetype):
    try:
        data = safe_req.get(
            f"https://db.ygoprodeck.com/api/v7/cardinfo.php?archetype={urllib.parse.quote_plus(archetype)}"
        )
    except:
        return []
    card_ids = []
    for card in data["data"]:
        card_ids.append(card["id"])
    return card_ids


def get_card_id_to_archetype_map():
    archetypes = get_archetypes_list()
    card_id_to_archetype = {}
    for archetype in archetypes:
        card_ids = get_archetype_card_ids(archetype)
        for card_id in card_ids:
            card_id_to_archetype[card_id] = archetype
    return card_id_to_archetype


def update_archetypes_in_cards(card_id_to_archetype):
    for filename in os.listdir("ydm_db/cards"):
        if not filename.endswith(".json"):
            continue

        with open(f"ydm_db/cards/{filename}", "r") as f:
            card = json.load(f)

        if "id" not in card:
            continue

        card_id = card["id"]

        if card_id not in card_id_to_archetype:
            continue

        card["archetype"] = card_id_to_archetype[card_id]

        with open(f"ydm_db/cards/{filename}", "w") as f:
            json.dump(card, f, indent=2, ensure_ascii=True)


def update_archetypes_in_sets(card_id_to_archetype):
    code_to_set = {}
    sets = []

    for filename in os.listdir("ydm_db/sets"):
        if not filename.endswith(".json"):
            continue

        with open(f"ydm_db/sets/{filename}", "r") as f:
            set_data = json.load(f)

        code_to_set[set_data["code"]] = {"filename": filename, "data": set_data}
        sets.append(set_data)

    # sort sets by pull type, so "composition" sets are processed after all the others
    sets.sort(key=lambda x: x["pull_type"], reverse=True)

    for set_data in sets:
        archetypes = set()

        if set_data["pull_type"] == "composition":
            for sub_set_code in set_data["sub_sets"]:
                if sub_set_code not in code_to_set:
                    continue
                sub_set_data = code_to_set[sub_set_code]["data"]
                if "archetypes" in sub_set_data:
                    archetypes.update(sub_set_data["archetypes"])
        else:
            for card in set_data["cards"]:
                if card["id"] in card_id_to_archetype:
                    archetypes.add(card_id_to_archetype[card["id"]])

        set_data["archetypes"] = list(archetypes)

        with open(f"ydm_db/sets/{code_to_set[set_data['code']]['filename']}", "w") as f:
            json.dump(set_data, f, indent=2, ensure_ascii=True)


card_id_to_archetype = get_card_id_to_archetype_map()
update_archetypes_in_cards(card_id_to_archetype)
update_archetypes_in_sets(card_id_to_archetype)
