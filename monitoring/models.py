import json
import os
from pathlib import Path
from django.utils import timezone

# === Пути ===
BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_DIR = BASE_DIR / "storage"
COMMENTS_PATH = STORAGE_DIR / "comments.json"
EXCLUSIONS_PATH = STORAGE_DIR / "exclusions.json"

# === Создание папки storage при запуске ===
if not STORAGE_DIR.exists():
    os.makedirs(STORAGE_DIR, exist_ok=True)

# === Вспомогательные функции ===
def _load_json(path):
    """Чтение JSON с безопасной обработкой ошибок"""
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def _save_json(path, data):
    """Сохранение JSON с перезаписью"""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# === API для комментариев ===
def add_comment(ptc_id, text, user_ip=None):
    data = _load_json(COMMENTS_PATH)
    comment = {
        "text": text,
        "ts": timezone.now().isoformat(),
        "ip": user_ip,
    }
    data.setdefault(str(ptc_id), []).append(comment)
    _save_json(COMMENTS_PATH, data)
    return True


def get_comments(ptc_id):
    data = _load_json(COMMENTS_PATH)
    return data.get(str(ptc_id), [])


# === API для исключений ===
def add_exclusion(ptc_id, param, until=None, tura=None, reason=None, user_ip=None):
    data = _load_json(EXCLUSIONS_PATH)
    exclusion = {
        "param": param,
        "until": until,
        "tura": tura,
        "reason": reason,
        "ts": timezone.now().isoformat(),
        "ip": user_ip,
    }
    data.setdefault(str(ptc_id), []).append(exclusion)
    _save_json(EXCLUSIONS_PATH, data)
    return True


def get_exclusions(ptc_id):
    data = _load_json(EXCLUSIONS_PATH)
    return data.get(str(ptc_id), [])
