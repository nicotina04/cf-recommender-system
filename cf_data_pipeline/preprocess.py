from config import PROCESSED_DATA_DIR
import storage


def get_division_type(title: str) -> int:
    title = title.lower()
    if 'hello' in title or 'good bye' in title or 'goodbye' in title:
        return 5
    if 'div. 1 + div. 2' in title or 'global' in title:
        return 5
    if 'div. 1' in title:
        return 1
    if 'div. 2' in title or 'educational' in title:
        return 2
    if 'div. 3' in title:
        return 3
    if 'div. 4' in title:
        return 4
    return 5  # special name (actually tag to div.1 + div.2)

def get_tag_group_map():
    csv_path = PROCESSED_DATA_DIR / 'tag_group_map.csv'
    df = storage.load_csv(csv_path)
    return df.set_index('tag')['groups'].to_dict()

def normalize_tags(raw_tags: list[str], tag_map: dict[str, str]) -> list[str]:
    tags = set()
    for t in raw_tags:
        if t not in tag_map:
            tags.add("other")
        else:
            for group in tag_map[t].split(","):
                tags.add(group)
    return list(tags)
