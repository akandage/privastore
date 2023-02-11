from requests import Response

def get_error_code(r: Response):
    return dict(r.json()).get('error-code')