#!/bin/bash
#See what ports are open/closed on gunicorn

sudo ss -tlnp | grep gunicorn

