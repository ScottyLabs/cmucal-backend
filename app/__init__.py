# Initializes the Flask app, database, and JWT authentication.
import os
from flask import Flask,g
from werkzeug.middleware.proxy_fix import ProxyFix
from app.services.db import get_session

from app.env import load_env
ENV = load_env()

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from app.config import DevelopmentConfig, TestingConfig, ProductionConfig
from app.services.db import init_db

def create_app():
    app = Flask(__name__)

    if ENV == "production":
        app.config.from_object(ProductionConfig)
    elif ENV == "test":
        app.config.from_object(TestingConfig)
    else:
        app.config.from_object(DevelopmentConfig)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1) # tell Flask to trust Railway’s proxy headers

    init_db()

    @app.before_request
    def open_db():
        g.db = get_session()

    @app.teardown_request
    def close_db(exc):
        db = g.pop("db", None)
        if db:
            if exc:
                db.rollback()
            db.close()
            # print(f"[DB] Session closed {id(db)}")

    if not os.getenv("ALEMBIC_RUNNING"): # skip during Alembic
        from flask_cors import CORS
        from app.api.users import users_bp
        from app.api.organizations import orgs_bp
        from app.api.base import base_bp
        from app.api.google import google_bp
        from app.api.events import events_bp
        from app.api.schedule import schedule_bp
        from app.api.admin import admin_bp
        from app.cli import import_courses_command

        origins = [o.strip() for o in os.getenv(
            "CORS_ALLOWED_ORIGINS",
            "http://localhost:3000,https://cmucal.vercel.app,http://cmucal.com,https://cal.scottylabs.org"
        ).split(",")]

        CORS(app, resources={r"/api/*": {
            "origins": origins,
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization", "Clerk-User-Id"],
        }}, supports_credentials=True)


        # Register blueprints (modular routing)
        app.register_blueprint(users_bp, url_prefix="/api/users")
        app.register_blueprint(orgs_bp, url_prefix="/api/organizations")
        app.register_blueprint(google_bp, url_prefix="/api/google")
        app.register_blueprint(events_bp, url_prefix="/api/events")
        app.register_blueprint(schedule_bp, url_prefix="/api/schedule")
        app.register_blueprint(admin_bp, url_prefix="/api/admin")
        app.register_blueprint(base_bp)
        
        # Register CLI command
        app.cli.add_command(import_courses_command)

    return app