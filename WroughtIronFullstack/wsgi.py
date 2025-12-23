"""
WSGI config for WroughtIronFullstack project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/howto/deployment/wsgi/
"""

import os
import sys

# Add src to python path to allow imports from wrought_iron
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'WroughtIronFullstack.settings')

application = get_wsgi_application()
