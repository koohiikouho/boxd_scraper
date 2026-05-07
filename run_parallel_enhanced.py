import os
import sys
import math
import multiprocessing
import time
import traceback
from typing import Tuple

from enhanced_letterboxd_scraper import EnhancedLetterboxdScraper


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_PAGES_LIMIT = 256  # Letterboxd returns null/empty results beyond page 256


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_task_urls(film_url: str) -> Tuple[str, str]:
    """
    Given a bare film URL like:
        https://letterboxd.com/film/a-hard-day-2021/
    return (latest_url, earliest_url):
        https://letterboxd.com/film/a-hard-day-2021/reviews/
        https://letterboxd.com/film/a-hard-day-2021/reviews/by/added-earliest/
    """
    base = film_url.rstrip('/')
    latest_url   = f"{base}/reviews/"
    earliest_url = f"{base}/reviews/by/added-earliest/"
    return latest_url, earliest_url


def compute_max_pages(review_count: int) -> int:
    """min( ceil((review_count / 12) / 2), MAX_PAGES_LIMIT )"""
    return min(math.ceil((review_count / 12) / 2), MAX_PAGES_LIMIT)


def derive_slug(film_url: str) -> str:
    return film_url.rstrip('/').split('/film/')[-1].split('/')[0]


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def scrape_task(url: str, output_file: str, max_pages: int, delay: int = 0) -> Tuple[bool, int, str]:
    """Worker function — runs in a separate process."""
    pid = multiprocessing.current_process().pid

    if delay > 0:
        print(f"[PID {pid}] Waiting {delay}s before starting...")
        time.sleep(delay)

    print(f"[PID {pid}] Starting: {output_file}  (max_pages={max_pages})")

    data_dir = os.path.join(os.getcwd(), f"chrome_data_{pid}")

    try:
        scraper = EnhancedLetterboxdScraper(headless=False, user_data_dir=data_dir)
        reviews = scraper.scrape_reviews(url, max_pages=max_pages)

        if reviews:
            scraper.save_to_csv(reviews, output_file)
            scraper.close()
            print(f"[PID {pid}] Saved {len(reviews)} reviews → {output_file}")
            return True, len(reviews), ""
        else:
            scraper.close()
            print(f"[PID {pid}] No reviews found for {output_file}")
            return False, 0, "No reviews found"

    except Exception as e:
        error_msg = f"Error in {output_file}: {str(e)}\n{traceback.format_exc()}"
        print(f"[PID {pid}] {error_msg}")
        return False, 0, error_msg


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """
    Usage:
        python run_parallel_enhanced.py <film_url> [review_count]

    Examples:
        python run_parallel_enhanced.py https://letterboxd.com/film/a-hard-day-2021/
        python run_parallel_enhanced.py https://letterboxd.com/film/a-hard-day-2021/ 9502
    """

    # ------------------------------------------------------------------
    # 1. Film URL
    # ------------------------------------------------------------------
    if len(sys.argv) >= 2:
        film_url = sys.argv[1].strip()
    else:
        film_url = input("Paste a Letterboxd film URL: ").strip()

    # ------------------------------------------------------------------
    # 2. Build both task URLs
    # ------------------------------------------------------------------
    latest_url, earliest_url = build_task_urls(film_url)
    slug = derive_slug(film_url)

    print("=" * 60)
    print("LETTERBOXD PARALLEL SCRAPER")
    print("=" * 60)
    print(f"Film     : {film_url}")
    print(f"Latest   : {latest_url}")
    print(f"Earliest : {earliest_url}")

    # ------------------------------------------------------------------
    # 3. Review count
    # ------------------------------------------------------------------
    if len(sys.argv) >= 3:
        review_count = int(sys.argv[2].replace(',', ''))
    else:
        raw = input("How many reviews does this film have? ").strip()
        review_count = int(raw.replace(',', ''))

    # ------------------------------------------------------------------
    # 4. Compute max_pages
    # ------------------------------------------------------------------
    max_pages = compute_max_pages(review_count)

    print(f"\nReview count : {review_count}")
    print(f"max_pages    : min(ceil(({review_count}/12)/2), {MAX_PAGES_LIMIT}) = {max_pages}")

    # ------------------------------------------------------------------
    # 5. Define tasks
    # ------------------------------------------------------------------
    tasks = [
        {
            'url':    latest_url,
            'output': f"latest_{slug}_reviews.csv",
            'delay':  0,
        },
        {
            'url':    earliest_url,
            'output': f"earliest_{slug}_reviews.csv",
            'delay':  10,   # stagger start to avoid Chrome collision
        },
    ]

    print("\nTasks:")
    for t in tasks:
        print(f"  [{t['output']}]  delay={t['delay']}s  url={t['url']}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # 6. Run in parallel
    # ------------------------------------------------------------------
    start_time = time.time()

    with multiprocessing.Pool(processes=2) as pool:
        args_list = [
            (t['url'], t['output'], max_pages, t['delay'])
            for t in tasks
        ]
        results = pool.starmap(scrape_task, args_list)

    end_time = time.time()

    # ------------------------------------------------------------------
    # 7. Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SCRAPING SUMMARY")
    print("=" * 60)

    total = 0
    for i, (success, count, error_msg) in enumerate(results):
        name = tasks[i]['output']
        if success:
            print(f"✓ {name}: {count} reviews")
            total += count
        else:
            short_err = error_msg[:120] + "..." if len(error_msg) > 120 else error_msg
            print(f"✗ {name}: FAILED — {short_err}")

    print(f"\nTotal reviews collected : {total}")
    print(f"Total time              : {end_time - start_time:.2f}s")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()