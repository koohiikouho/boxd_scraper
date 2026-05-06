import os
import random
from enhanced_letterboxd_scraper import EnhancedLetterboxdScraper
import multiprocessing
import time
import traceback
from typing import Tuple


def scrape_task(url: str, output_file: str, max_pages: int, delay: int = 0) -> Tuple[bool, int, str]:
    """
    Worker function for scraping

    Returns: (success, count, error_message)
    """
    pid = multiprocessing.current_process().pid
    
    if delay > 0:
        print(f"[PID {pid}] Waiting {delay} seconds before starting to avoid collision...")
        time.sleep(delay)
        
    print(f"[PID {pid}] Starting scrape: {output_file}")

    # Unique data dir for each process
    data_dir = os.path.join(os.getcwd(), f"chrome_data_{pid}")

    try:
        scraper = EnhancedLetterboxdScraper(headless=False, user_data_dir=data_dir)
        reviews = scraper.scrape_reviews(url, max_pages=max_pages)

        if reviews:
            scraper.save_to_csv(reviews, output_file)
            scraper.close()
            print(f"[PID {pid}] Successfully saved {len(reviews)} reviews to {output_file}")
            return True, len(reviews), ""
        else:
            scraper.close()
            print(f"[PID {pid}] No reviews found for {output_file}")
            return False, 0, "No reviews found"

    except Exception as e:
        error_msg = f"Error in {output_file}: {str(e)}\n{traceback.format_exc()}"
        print(f"[PID {pid}] {error_msg}")
        return False, 0, error_msg


def main():
    """Main function to run both scrapers in parallel"""

    # Base URL for the film reviews
    base_url = "https://letterboxd.com/film/unmarry/reviews/"

    # Define the scraping tasks
    tasks = [
        {
            'url': base_url,
            'max_pages': 49,
            'delay': 0
        },
        {
            'url': f"{base_url}by/added-earliest/",
            'max_pages': 49,
            'delay': 10
        }
    ]

    # Derive output names from URLs
    for task in tasks:
        film_name = task['url'].split('/film/')[1].split('/')[0]
        if 'added-earliest' in task['url']:
            task['output'] = f"earliest_{film_name}_reviews.csv"
        else:
            task['output'] = f"latest_{film_name}_reviews.csv"

    print("=" * 60)
    print("LETTERBOXD PARALLEL SCRAPER")
    print("=" * 60)

    start_time = time.time()

    # Create process pool
    with multiprocessing.Pool(processes=2) as pool:
        # Prepare arguments for each task
        args_list = [(task['url'], task['output'], task['max_pages'], task.get('delay', 0)) for task in tasks]

        # Execute in parallel
        results = pool.starmap(scrape_task, args_list)

    end_time = time.time()

    # Display results
    print("\n" + "=" * 60)
    print("SCRAPING SUMMARY")
    print("=" * 60)

    total_reviews = 0
    for i, (success, count, error_msg) in enumerate(results):
        task_name = tasks[i]['output']
        if success:
            print(f"✓ {task_name}: {count} reviews")
            total_reviews += count
        else:
            print(f"✗ {task_name}: FAILED - {error_msg[:100]}...")

    print(f"\nTotal reviews collected: {total_reviews}")
    print(f"Total time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    # Windows requires this for multiprocessing
    multiprocessing.freeze_support()
    main()