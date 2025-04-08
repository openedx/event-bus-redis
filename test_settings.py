"""
These settings are here to use during tests, because django requires them.

In a real-world use case, apps in this project are installed into other
Django applications, so these settings will not be used.
"""

from os import getenv
from os.path import abspath, dirname, join


def root(*args):
    """
    Get the absolute path of the given path relative to the project root.
    """
    return join(abspath(dirname(__file__)), *args)


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": "default.db",
        "USER": "",
        "PASSWORD": "",
        "HOST": "",
        "PORT": "",
    }
}

INSTALLED_APPS = (
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.messages",
    "django.contrib.sessions",
    "edx_event_bus_redis",
    "openedx_events",
)

LOCALE_PATHS = [
    root("edx_event_bus_redis", "conf", "locale"),
]

LOGGING = {
    "version": 1,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
        }
    },
    "loggers": {
        "": {"handlers": ["console"], "level": "INFO", "propagate": True},
    },
}

ROOT_URLCONF = "edx_event_bus_redis.urls"

SECRET_KEY = "insecure-secret-key"

MIDDLEWARE = (
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
)

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": False,
        "OPTIONS": {
            "context_processors": [
                "django.contrib.auth.context_processors.auth",  # this is required for admin
                "django.contrib.messages.context_processors.messages",  # this is required for admin
                "django.template.context_processors.request",  # this is required for admin
            ],
        },
    }
]

EVENT_BUS_PRODUCER = getenv("EVENT_BUS_PRODUCER")
EVENT_BUS_REDIS_CONNECTION_URL = getenv("EVENT_BUS_REDIS_CONNECTION_URL")
EVENT_BUS_TOPIC_PREFIX = getenv("EVENT_BUS_TOPIC_PREFIX")
EVENT_BUS_CONSUMER = getenv("EVENT_BUS_CONSUMER")
EVENT_BUS_REDIS_STREAM_MAX_LEN = getenv("EVENT_BUS_REDIS_STREAM_MAX_LEN", 10_000)
