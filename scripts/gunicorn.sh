#!/usr/bin/bash

# Define project directory
PROJECT_MAIN_DIR_NAME="sku-mapper-backend"

# Copy Gunicorn socket and service files
sudo cp "/home/igate/$PROJECT_MAIN_DIR_NAME/gunicorn/django-gunicorn.socket" "/etc/systemd/system/django-gunicorn.socket"
sudo cp "/home/igate/$PROJECT_MAIN_DIR_NAME/gunicorn/django-gunicorn.service" "/etc/systemd/system/django-gunicorn.service"

# Reload systemd to recognize new units
sudo systemctl daemon-reload

# Start and enable Django Gunicorn service and socket
sudo systemctl start django-gunicorn.socket
sudo systemctl enable django-gunicorn.socket
