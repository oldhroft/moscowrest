from requests import Response, Session

def get_query(session: Session, query: str, **kwargs) -> Response:
    query = query.format(**kwargs)
    return session.get(query)