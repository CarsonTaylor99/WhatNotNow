import httpx
from config import GRAPHQL_URL, COOKIE, CLIENT_VERSION

HEADERS = {
    "Content-Type": "application/json",
    "Cookie": COOKIE,
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/147.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.whatnot.com",
    "Referer": "https://www.whatnot.com/live",
    "x-client-version": CLIENT_VERSION,
}

EXPLORE_QUERY = """
query LiveStreamExplore($id: ID!) {
  liveStream(id: $id) {
    id
    explore {
      id
      objects {
        totalCount
        pageInfo {
          endCursor
          hasNextPage
        }
        edges {
          cursor
          node {
            ... on FeedEntity {
              object {
                ... on LiveStream {
                  id
                  title
                  status
                  activeViewers
                  startTime
                  user {
                    username
                    id
                  }
                  livestreamCategories {
                    label
                  }
                  thumbnail {
                    smallImage: url(width: 414, height: 640, format: WEBP, fit: COVER)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


async def get_streams(explore_id: str) -> dict:
    """Fetch currently live streams for a given explore/category ID."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                GRAPHQL_URL,
                json={
                    "operationName": "LiveStreamExplore",
                    "variables": {"id": explore_id},
                    "query": EXPLORE_QUERY,
                },
                headers=HEADERS,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"[fetcher] Error fetching streams: {e}")
        return {"streams": [], "hasNextPage": False, "endCursor": None, "totalCount": 0}

    explore = (
        data.get("data", {})
            .get("liveStream", {})
            .get("explore", {})
    )
    objects = explore.get("objects", {})

    streams = []
    for edge in objects.get("edges", []):
        obj = edge.get("node", {}).get("object", {})
        if not obj or obj.get("status") != "PLAYING":
            continue
        categories = obj.get("livestreamCategories") or [{}]
        thumb = (obj.get("thumbnail") or {}).get("smallImage", "")
        streams.append({
            "id":       obj["id"],
            "title":    obj.get("title", "Untitled"),
            "username": obj.get("user", {}).get("username", ""),
            "viewers":  obj.get("activeViewers", 0),
            "category": categories[0].get("label", ""),
            "thumbnail": thumb,
            "url":      f"https://www.whatnot.com/live/{obj['id']}",
        })

    return {
        "streams":     streams,
        "hasNextPage": objects.get("pageInfo", {}).get("hasNextPage", False),
        "endCursor":   objects.get("pageInfo", {}).get("endCursor"),
        "totalCount":  objects.get("totalCount", 0),
    }
