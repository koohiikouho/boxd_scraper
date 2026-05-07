import requests
from bs4 import BeautifulSoup
import math

url = "https://letterboxd.com/film/unmarry/reviews/"
headers = {"User-Agent": "Mozilla/5.0"}
resp = requests.get(url, headers=headers)
soup = BeautifulSoup(resp.text, 'html.parser')

# Let's try to find the review count
# On letterboxd reviews page, usually there's a filter or header that shows total reviews
# like <a href="/film/unmarry/reviews/" class="active" title="24 reviews">...</a>
for a in soup.find_all('a'):
    if 'title' in a.attrs and 'reviews' in a['title'].lower():
        print(a['title'])
        
# also let's look at the main navigation or tabs
nav = soup.find('ul', class_='nav')
if nav:
    print(nav.text)

