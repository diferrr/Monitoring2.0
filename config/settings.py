import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv("SECRET_KEY", "default-key")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.getenv("DEBUG", "False") == "True"

ALLOWED_HOSTS = ['localhost', '127.0.0.1', '10.1.1.248', '10.3.1.29']

EDITORS_IPS = {'10.1.1.52', '10.3.1.29', '10.1.1.50', '10.1.1.69', '10.1.1.139', '10.1.1.230', '10.1.1.57'}

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    "monitoring",
    "mapapp",
    "pumps",
    'rest_framework',
    'monitoring_PTC.charts',
    'monitoring_PTC.termocom_charts',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],  # используем app templates (charts/templates, monitoring/templates)
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'


AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# Internationalization
# https://docs.djangoproject.com/en/5.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True

# >>> added: локальная TZ для графиков (конвертация времени на стороне приложения)
LOCAL_TIME_ZONE = 'Europe/Chisinau'

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.2/howto/static-files/

STATIC_URL = '/static/'
STATICFILES_DIRS = [
    BASE_DIR / "static",
]

# Default primary key field type
# https://docs.djangoproject.com/en/5.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# SQL Server connection settings
SQL_SERVER = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': '10.1.1.124',
    'DATABASE': 'TERMOCOM5',
    'UID': 'disp',
    'PWD': 'disp123',
}

LOVATI_SERVER = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'SERVER': '10.1.1.248',
    'DATABASE': 'LOVATI',
    'UID': 'disp',
    'PWD': 'disp123',
    'Trusted_Connection': 'no',
}

LOVATI_UID_COLUMNS_PRIORITY = [
    "id_lovati", "deveui", "dev_eui", "device_uid", "serial", "imei", "uid"
]

# >>> added: базовый URL новой страницы графика (для генерации ссылок из Monitoring)
LR_CHART_BASE = '/charts/chart/'

# >>> added: минимальная конфигурация DRF
REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': ['rest_framework.renderers.JSONRenderer'],
    'DEFAULT_PARSER_CLASSES': ['rest_framework.parsers.JSONParser'],
}

# >>> added: выбор БД для модуля charts (по умолчанию LOVATI; TERMOCOM5 не трогаем)
CHARTS_DB_SOURCE = os.getenv('CHARTS_DB_SOURCE', 'LOVATI')
CHARTS_DB = LOVATI_SERVER if CHARTS_DB_SOURCE.upper() == 'LOVATI' else SQL_SERVER

# ===== Pumps / PTC links =====
PTC_VIEW_URL_TEMPLATE = os.getenv(
    'PTC_VIEW_URL_TEMPLATE',
    'http://10.1.1.174/view/view_page.php?title={ptc}'
)
