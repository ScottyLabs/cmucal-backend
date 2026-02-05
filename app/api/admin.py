# app/routes/admin.py

from flask import Blueprint, jsonify, request
import os

admin_bp = Blueprint("admin", __name__, url_prefix="/api/admin")
