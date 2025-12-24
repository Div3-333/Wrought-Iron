from django.shortcuts import redirect
from django.conf import settings
from django.urls import reverse
import wrought_iron_core

class LicenseMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Exclude license page itself to avoid infinite loop
        if request.path.startswith('/license-required') or request.path.startswith('/static') or request.path.startswith('/admin'):
             return self.get_response(request)

        # Check session first to avoid repeated API calls
        if request.session.get('is_licensed'):
            return self.get_response(request)

        # Get license key from settings or a file
        license_key = getattr(settings, 'LICENSE_KEY', None)
        
        # If no key in settings, try reading from a file in persistent data dir
        if not license_key:
            try:
                # Use DATABASE_PATH directory for license.key
                license_file = settings.DATABASE_PATH.parent / 'license.key'
                if license_file.exists():
                    with open(license_file, 'r') as f:
                        license_key = f.read().strip()
            except Exception:
                pass
        
        if not license_key:
             return redirect('/license-required')

        # Verify using Rust core
        # In production, you might want to cache this result for X hours/days
        # For now, we set the session variable if valid.
        is_valid = wrought_iron_core.verify_license(license_key)
        
        if is_valid:
            request.session['is_licensed'] = True
            return self.get_response(request)
        else:
            return redirect('/license-required')
