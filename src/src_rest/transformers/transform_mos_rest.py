from typing import TypedDict
from bs4 import BeautifulSoup
from bs4.element import Tag
from typing import Optional, List, cast


class ParsedItem(TypedDict):
    title: Optional[str]
    rating: Optional[int]
    cuisine: Optional[str]
    phone: Optional[str]
    link: Optional[str]
    city: Optional[str]
    address: Optional[str]


class ParsedData(ParsedItem):
    url: str
    dttm: str
    fname: str


def _extract_value(
    soup: BeautifulSoup, class_name, raise_error: bool = False
) -> Optional[str]:
    span = soup.find("span", class_=class_name)
    if isinstance(span, Tag):
        item = span.find("span", class_="value-title")
        if isinstance(item, Tag):
            return item.attrs["title"]
        elif raise_error:
            raise ValueError(f"Value-title not found")
        else:
            return None
    elif raise_error:
        raise ValueError(f"Span {class_name} not found, rebuild parsing!")
    else:
        return None


def parse_item(x: str) -> ParsedItem:

    soup = BeautifulSoup(x, "html.parser")

    fn_org = soup.find("span", "fn org")
    if isinstance(fn_org, Tag):
        title = fn_org.text
    else:
        raise ValueError("Title")

    rating_item = soup.find("span", class_="col col_2")
    if isinstance(rating_item, Tag):
        rating_raw = rating_item.find_all("i", class_="i-star orange")
        rating = len(rating_raw)

    cuisine_item = soup.find("span", class_="col col_3")
    if isinstance(cuisine_item, Tag):
        cuisine = cuisine_item.text

    phone = _extract_value(soup, "tel", True)
    link = _extract_value(soup, "permalink", True)
    city = _extract_value(soup, "locality", True)
    address = _extract_value(soup, "street-address", True)

    return {
        "title": title,
        "rating": rating,
        "cuisine": cuisine,
        "phone": phone,
        "link": link,
        "city": city,
        "address": address,
    }


def parse_data(data: dict, fname: str) -> List[ParsedData]:
    result: List[ParsedData] = []
    if not data["ok"]:
        return result

    for card in data["data"]["cards"]:
        item = {"dttm": data["dttm"], "url": data["url"], "fname": fname}
        item.update(parse_item(card))
        # Danger zone, https://github.com/python/mypy/issues/11753
        item_casted = cast(ParsedData, item)
        result.append(item_casted)

    return result
