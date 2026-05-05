"""Phase 1 — fetch active Whatnot livestreams via the internal GraphQL API."""

import requests

GRAPHQL_URL = "https://api.whatnot.com/graphql"

_LIVESTREAMS_QUERY = """
query ActiveLivestreams($after: String, $first: Int) {
  livestreams(status: ACTIVE, after: $after, first: $first) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        title
        viewerCount
        url
        seller {
          username
        }
        category {
          name
        }
      }
    }
  }
}
"""


def fetch_streams(page_size: int = 50) -> list[dict]:
    """Return all active livestreams, paginating until exhausted."""
    streams: list[dict] = []
    cursor: str | None = None

    while True:
        variables: dict = {"first": page_size}
        if cursor:
            variables["after"] = cursor

        resp = requests.post(
            GRAPHQL_URL,
            json={"query": _LIVESTREAMS_QUERY, "variables": variables},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        connection = data["data"]["livestreams"]
        for edge in connection["edges"]:
            node = edge["node"]
            streams.append(
                {
                    "id": node["id"],
                    "title": node.get("title", ""),
                    "seller": node.get("seller", {}).get("username", ""),
                    "category": node.get("category", {}).get("name", ""),
                    "viewers": node.get("viewerCount", 0),
                    "url": node.get("url", ""),
                }
            )

        page_info = connection["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]

    return streams
