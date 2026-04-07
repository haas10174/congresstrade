"""Generate static HTML from Flask app for GitHub Pages deployment."""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
os.chdir(os.path.dirname(__file__))

from flask_frozen import Freezer
from app import app, get_cached_data

app.config['FREEZER_DESTINATION'] = 'docs'
app.config['FREEZER_RELATIVE_URLS'] = True
app.config['FREEZER_BASE_URL'] = '/congresstrade/'

freezer = Freezer(app)

@freezer.register_generator
def politician():
    _, members = get_cached_data()
    for m in members:
        yield {'slug': m['slug']}

@freezer.register_generator
def index():
    for tab in ['trades', 'politicians', 'senate']:
        yield {'tab': tab}

if __name__ == '__main__':
    print("Freezing site to docs/ ...")
    freezer.freeze()
    print("Done! Static site in docs/")
