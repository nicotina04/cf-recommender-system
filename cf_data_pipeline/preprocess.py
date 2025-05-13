from config import PROCESSED_DATA_DIR, DATA_PIPELINE_DIR
import storage


_problem_tags_list: list = None
_problem_tag_index_dict: dict = None

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

def get_tags():
    if problem_tags is not None:
        return problem_tags
    
    csv_path = PROCESSED_DATA_DIR / 'tags.csv'
    df = storage.load_csv(csv_path)
    problem_tags = set(df['tag'].tolist())
    return problem_tags

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

def get_problem_tag_list() -> list[str]:
    global _problem_tags_list
    if _problem_tags_list is None:
        with open(DATA_PIPELINE_DIR / 'problem_tags.txt', 'r') as f:
            _problem_tags_list = f.read().splitlines()

    return _problem_tags_list

def get_problem_tag_index_dict() -> dict[str, int]:
    global _problem_tag_index_dict
    if _problem_tag_index_dict is not None:
        return _problem_tag_index_dict
    
    _problem_tag_index_dict = dict()
    with open(PROCESSED_DATA_DIR / 'problem_tag_index.txt', 'r') as f:
        lines = f.read().splitlines()
        for i in range(len(lines)):
            _problem_tag_index_dict[lines[i]] = i 
    return _problem_tag_index_dict