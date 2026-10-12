"""Remember menu navigation only; never restore actions or private search text."""
import re
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit

from core.navigation import build_nav_groups

RESUME_FILTERS = {'sales_year', 'sales_month', 'sales_branch', 'months', 'page', 'sucursal', 'branch_id', 'tab', 'periodo', 'fecha', 'status', 'estatus', 'year', 'month'}


def mobile_resume_target(request):
    raw = request.COOKIES.get(f'erp_resume_{request.user.pk}', '')
    if not raw or len(raw) > 4096:
        return '/dashboard/'
    try:
        target = urlsplit(unquote(raw))
    except ValueError:
        return '/dashboard/'
    allowed = {
        urlsplit(item['url']).path
        for group in build_nav_groups(request.user, '')
        for item in group['items']
    }
    if target.scheme or target.netloc or target.path not in allowed:
        return '/dashboard/'
    filters = [(key, value) for key, value in parse_qsl(target.query)
               if key in RESUME_FILTERS and re.fullmatch(r'[A-Za-z0-9_-]{1,64}', value)]
    query = urlencode(filters)
    return target.path + ('?' + query if query else '') + '#erp-resume'
