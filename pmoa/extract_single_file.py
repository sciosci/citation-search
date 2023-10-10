import sys
from pprint import pprint

from article_has_citation import get_etree_with_path, Article

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python extract_single_file.py <file_path>')
        sys.exit(1)
    path = sys.argv[1]

    result = get_etree_with_path(path=path)
    article = Article(root=result)
    meta = article.build_meta_list_all_level()
    pprint(meta)
