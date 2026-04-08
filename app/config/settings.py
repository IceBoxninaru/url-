import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
APP_DIR = ROOT_DIR / "app"


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


SECRET_KEY = os.getenv(
    "DJANGO_SECRET_KEY",
    "django-insecure-local-dev-only-url-archive-app",
)
DEBUG = env_bool("DJANGO_DEBUG", True)
ALLOWED_HOSTS = [
    host.strip()
    for host in os.getenv("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost,testserver").split(",")
    if host.strip()
]
CSRF_TRUSTED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("DJANGO_CSRF_TRUSTED_ORIGINS", "").split(",")
    if origin.strip()
]


INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.postgres',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'resources',
    'tags',
    'snapshots',
    'jobs',
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
        'DIRS': [APP_DIR / 'templates'],
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


DB_BACKEND = os.getenv("DB_BACKEND")
if not DB_BACKEND:
    DB_BACKEND = "sqlite" if env_bool("USE_SQLITE", False) or not os.getenv("POSTGRES_HOST") else "postgresql"

if DB_BACKEND == "postgresql":
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.getenv('POSTGRES_DB', 'url_archive'),
            'USER': os.getenv('POSTGRES_USER', 'url_archive'),
            'PASSWORD': os.getenv('POSTGRES_PASSWORD', 'url_archive'),
            'HOST': os.getenv('POSTGRES_HOST', '127.0.0.1'),
            'PORT': os.getenv('POSTGRES_PORT', '5432'),
            'CONN_MAX_AGE': int(os.getenv('POSTGRES_CONN_MAX_AGE', '60')),
        }
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ROOT_DIR / 'db.sqlite3',
        }
    }


AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


LANGUAGE_CODE = 'ja'
TIME_ZONE = os.getenv("TZ", "Asia/Tokyo")

USE_I18N = True

USE_TZ = True


STATIC_URL = 'static/'
STATICFILES_DIRS = [APP_DIR / 'static']
STATIC_ROOT = ROOT_DIR / 'staticfiles'

STORAGE_ROOT = ROOT_DIR / 'storage'
STORAGE_URL = "/storage/"
HTML_STORAGE_ROOT = STORAGE_ROOT / 'html'
TEXT_STORAGE_ROOT = STORAGE_ROOT / 'text'
JSON_STORAGE_ROOT = STORAGE_ROOT / 'json'
SCREENSHOT_STORAGE_ROOT = STORAGE_ROOT / 'screenshots'
IMAGE_STORAGE_ROOT = STORAGE_ROOT / 'images'
VIDEO_STORAGE_ROOT = STORAGE_ROOT / 'videos'
CAPTURE_X_STORAGE_STATE_PATH = os.getenv("CAPTURE_X_STORAGE_STATE_PATH", "storage/auth/x.json")

CAPTURE_HTTP_TIMEOUT = int(os.getenv("CAPTURE_HTTP_TIMEOUT", "20"))
CAPTURE_HTTP_USER_AGENT = os.getenv(
    "CAPTURE_HTTP_USER_AGENT",
    "URLArchiveBot/1.0 (+https://localhost)",
)
CAPTURE_PLAYWRIGHT_TIMEOUT_MS = int(os.getenv("CAPTURE_PLAYWRIGHT_TIMEOUT_MS", "30000"))
CAPTURE_VIEWPORT_WIDTH = int(os.getenv("CAPTURE_VIEWPORT_WIDTH", "1440"))
CAPTURE_VIEWPORT_HEIGHT = int(os.getenv("CAPTURE_VIEWPORT_HEIGHT", "900"))
CAPTURE_MAX_IMAGES = int(os.getenv("CAPTURE_MAX_IMAGES", "8"))
CAPTURE_MAX_IMAGE_BYTES = int(os.getenv("CAPTURE_MAX_IMAGE_BYTES", "8388608"))
CAPTURE_MAX_VIDEOS = int(os.getenv("CAPTURE_MAX_VIDEOS", "4"))
CAPTURE_MAX_VIDEO_BYTES = int(os.getenv("CAPTURE_MAX_VIDEO_BYTES", str(10 * 1024 * 1024 * 1024)))
CAPTURE_FFPROBE_PATH = os.getenv("CAPTURE_FFPROBE_PATH", "/usr/bin/ffprobe").strip()
CAPTURE_VIDEO_DOMAINS = [
    item.strip().lower()
    for item in os.getenv(
        "CAPTURE_VIDEO_DOMAINS",
        "x.com,twitter.com,instagram.com",
    ).split(",")
    if item.strip()
]
CAPTURE_JS_FALLBACK_DOMAINS = [
    item.strip().lower()
    for item in os.getenv(
        "CAPTURE_JS_FALLBACK_DOMAINS",
        "x.com,twitter.com,instagram.com,threads.net,tiktok.com",
    ).split(",")
    if item.strip()
]
LINK_CHECK_HTTP_TIMEOUT = int(os.getenv("LINK_CHECK_HTTP_TIMEOUT", "8"))
LINK_CHECK_CACHE_SECONDS = int(os.getenv("LINK_CHECK_CACHE_SECONDS", "43200"))

AI_PROVIDER = os.getenv("AI_PROVIDER", "heuristic")
AI_MODEL = os.getenv("AI_MODEL", "")
AI_MAX_INPUT_CHARS = int(os.getenv("AI_MAX_INPUT_CHARS", "12000"))
AI_SUMMARY_MAX_CHARS = int(os.getenv("AI_SUMMARY_MAX_CHARS", "320"))

JOB_MAX_RETRIES = int(os.getenv("JOB_MAX_RETRIES", "3"))
JOB_RETRY_DELAYS_SECONDS = [
    int(part.strip())
    for part in os.getenv("JOB_RETRY_DELAYS_SECONDS", "60,600,3600").split(",")
    if part.strip()
]


DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
