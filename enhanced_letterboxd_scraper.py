import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import List, Dict, Optional
import re
import undetected_chromedriver as uc


class EnhancedLetterboxdScraper:
    def __init__(self, headless=True, user_data_dir=None):
        """
        Initialize the enhanced Letterboxd scraper with click functionality
        :param headless: Run browser in headless mode
        :param user_data_dir: Path to unique user data directory for the Chrome instance
        """
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # Setup Chrome options
        chrome_options = uc.ChromeOptions()
        # Do NOT set chrome_options.headless — it can silently force headless in some uc versions.
        # Pass headless= only via the uc.Chrome() kwarg below.
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")

        import shutil
        import os

        # Initialize driver
        kwargs = {
            'options': chrome_options,
            'headless': headless,
            'user_data_dir': user_data_dir,
            'version_main': 147
        }

        # Prevent undetected_chromedriver from killing running drivers by copying the executable per PID
        base_driver = os.path.expanduser("~/.local/share/undetected_chromedriver/undetected_chromedriver")
        if os.path.exists(base_driver):
            pid_driver = f"{base_driver}_{os.getpid()}"
            shutil.copy2(base_driver, pid_driver)
            kwargs['driver_executable_path'] = pid_driver

        self.driver = uc.Chrome(**kwargs)

        self.wait = WebDriverWait(self.driver, 15)
        self.review_data = []

    def get_review_count(self, film_url: str) -> int:
        """
        Load the film page and extract the total review count directly from
        the live rendered DOM via Selenium (no BeautifulSoup / page_source).

        Letterboxd stores the count in the film nav tab:
            <li class="js-route-reviews">
              <a class="tooltip" title="9,502 reviews">Reviews</a>
            </li>

        :param film_url: Base film URL, e.g. https://letterboxd.com/film/foo/
        :return: Total number of reviews, or -1 if not found
        """
        import re as _re

        base = film_url.rstrip('/')
        # The review count is only visible on the /reviews/ page, not the base film page
        reviews_page = f"{base}/reviews/"
        self.logger.info(f"Fetching review count from: {reviews_page}")
        self.driver.get(reviews_page)
        time.sleep(4)

        # --- Strategy 1: poll until li.js-route-reviews a[title] is non-empty ---
        # The title attr is populated by JS *after* the element appears, so
        # presence_of_element_located isn't enough — we must poll for the value.
        try:
            def title_is_populated(driver):
                els = driver.find_elements(By.CSS_SELECTOR, 'li.js-route-reviews a')
                if els:
                    t = els[0].get_attribute('title') or ''
                    return t if _re.search(r'\d', t) else False
                return False

            title = WebDriverWait(self.driver, 20).until(title_is_populated)
            self.logger.info(f"Nav tab title attr: {repr(title)}")
            nums = _re.findall(r'[\d,]+', title)
            if nums:
                count = int(nums[0].replace(',', ''))
                self.logger.info(f"Review count from nav tab: {count}")
                return count
        except Exception as e:
            self.logger.warning(f"Strategy 1 (polling title) failed: {e}")

        # --- Strategy 2: JavaScript getAttribute directly ---
        try:
            title = self.driver.execute_script(
                "var el = document.querySelector('li.js-route-reviews a');"
                "return el ? el.getAttribute('title') : '';"
            ) or ''
            self.logger.info(f"JS getAttribute title: {repr(title)}")
            nums = _re.findall(r'[\d,]+', title)
            if nums:
                count = int(nums[0].replace(',', ''))
                self.logger.info(f"Review count from JS: {count}")
                return count
        except Exception as e:
            self.logger.warning(f"Strategy 2 (JS getAttribute) failed: {e}")

        # --- Strategy 3: XPath on any <a> whose title contains digits + "reviews" ---
        try:
            els = self.driver.find_elements(By.XPATH, "//a[@title]")
            for el in els:
                title = el.get_attribute('title') or ''
                if _re.search(r'\d[\d,]*\s*reviews?', title, _re.I):
                    nums = _re.findall(r'[\d,]+', title)
                    if nums:
                        count = int(nums[0].replace(',', ''))
                        self.logger.info(f"Review count from XPath sweep: {count}")
                        return count
        except Exception as e:
            self.logger.warning(f"Strategy 3 (XPath sweep) failed: {e}")

        # --- Strategy 4: page source via BeautifulSoup (last resort) ---
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            reviews_li = soup.find('li', class_='js-route-reviews')
            if reviews_li:
                a = reviews_li.find('a')
                if a:
                    title = a.get('title', '')
                    nums = _re.findall(r'[\d,]+', title)
                    if nums:
                        count = int(nums[0].replace(',', ''))
                        self.logger.info(f"Review count from BeautifulSoup: {count}")
                        return count
        except Exception as e:
            self.logger.warning(f"Strategy 4 (BeautifulSoup) failed: {e}")

        self.logger.warning("Could not determine review count from any strategy")
        return -1

    def scrape_reviews(self, url: str, max_pages: Optional[int] = None, max_reviews: Optional[int] = None) -> List[
        Dict]:
        """
        Scrape reviews from a Letterboxd movie page with full text expansion
        :param url: URL of the Letterboxd movie reviews page
        :param max_pages: Maximum number of pages to scrape
        :param max_reviews: Maximum number of reviews to scrape
        :return: List of dictionaries containing review data
        """
        try:
            self.logger.info(f"Starting to scrape reviews from: {url}")
            self.review_data = []

            # Navigate to the page
            self.driver.get(url)
            time.sleep(4)  # Wait for initial page load

            # Check if we're on the reviews page
            if "reviews" not in self.driver.current_url:
                self.logger.info("Navigating to reviews page...")
                try:
                    # Try multiple ways to find reviews link
                    reviews_selectors = [
                        "//a[contains(@href, '/reviews/')]",
                        "//a[contains(text(), 'Reviews')]",
                        "//li[@class='js-route-reviews']/a"
                    ]

                    for selector in reviews_selectors:
                        try:
                            reviews_link = self.driver.find_element(By.XPATH, selector)
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", reviews_link)
                            time.sleep(1)
                            self.driver.execute_script("arguments[0].click();", reviews_link)
                            time.sleep(3)
                            break
                        except:
                            continue
                except Exception as e:
                    self.logger.error(f"Could not navigate to reviews: {e}")

            page_count = 0

            while True:
                page_count += 1
                self.logger.info(f"Scraping page {page_count}")

                # Expand all truncated reviews on the current page
                self._expand_all_reviews()

                # Get page source and parse
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')

                # Extract reviews from current page
                page_reviews = self._extract_reviews_from_page(soup, page_count)
                self.review_data.extend(page_reviews)

                self.logger.info(
                    f"Found {len(page_reviews)} reviews on page {page_count}. Total: {len(self.review_data)}")

                # Check limits
                if max_reviews and len(self.review_data) >= max_reviews:
                    self.review_data = self.review_data[:max_reviews]
                    self.logger.info(f"Reached maximum reviews limit: {max_reviews}")
                    break

                if max_pages and page_count >= max_pages:
                    self.logger.info(f"Reached maximum pages limit: {max_pages}")
                    break

                # Go to next page
                if not self._go_to_next_page():
                    self.logger.info("No more pages found")
                    break

            self.logger.info(f"Scraping completed. Total reviews collected: {len(self.review_data)}")
            return self.review_data

        except Exception as e:
            self.logger.error(f"Error during scraping: {str(e)}")
            return self.review_data

    def _expand_all_reviews(self):
        """Click all 'more' links to expand truncated reviews on the current page using XPath"""
        try:
            # Use XPath to find all 'more' links more reliably
            more_xpaths = [
                "//a[contains(@class, 'reveal') and contains(@data-js-trigger, 'collapsible.expand')]",
                "//a[contains(@class, 'reveal') and contains(text(), 'more')]",
                "//a[@class='reveal']"
            ]

            expanded_count = 0
            for xpath in more_xpaths:
                try:
                    more_links = self.driver.find_elements(By.XPATH, xpath)
                    self.logger.info(f"Found {len(more_links)} 'more' links using XPath: {xpath}")

                    for i, link in enumerate(more_links):
                        try:
                            # Scroll to the link
                            self.driver.execute_script(
                                "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", link)
                            time.sleep(0.5)

                            # Check if it's still present and clickable
                            if link.is_displayed() and link.is_enabled():
                                # Click using JavaScript
                                self.driver.execute_script("arguments[0].click();", link)
                                expanded_count += 1

                                # Small delay to allow content to expand
                                time.sleep(0.4)

                        except (StaleElementReferenceException, NoSuchElementException):
                            continue
                        except Exception as e:
                            self.logger.debug(f"Error expanding link: {e}")
                            continue

                    # If we found links with this XPath, break
                    if more_links:
                        break

                except Exception as e:
                    self.logger.debug(f"Error with XPath {xpath}: {e}")
                    continue

            # Wait for all expansions to complete
            time.sleep(1)
            self.logger.info(f"Successfully expanded {expanded_count} reviews")

        except Exception as e:
            self.logger.error(f"Error expanding reviews: {e}")

    def _extract_reviews_from_page(self, soup: BeautifulSoup, page_num: int) -> List[Dict]:
        """
        Extract review data from a BeautifulSoup parsed page using improved selectors
        """
        reviews = []

        # Use XPath-style CSS selectors that match the actual HTML structure
        review_selectors = [
            'article.production-viewing.-viewing',
            'div.listitem article.production-viewing',
            'article.production-viewing',
            'div[data-js-treasure-hunt="index-target"]',
            '.viewing-list .listitem article'
        ]

        review_containers = []
        for selector in review_selectors:
            review_containers = soup.select(selector)
            if review_containers:
                self.logger.info(f"Found {len(review_containers)} reviews using selector: {selector}")
                break

        if not review_containers:
            self.logger.warning("No review containers found with primary selectors")
            # Try to find any article with review-like content
            review_containers = soup.find_all('article')
            if not review_containers:
                # Look for divs that might contain reviews
                review_containers = soup.find_all('div', class_=lambda x: x and 'listitem' in str(x))

        self.logger.info(f"Total review containers to parse: {len(review_containers)}")

        for i, container in enumerate(review_containers):
            try:
                review_data = self._parse_review_container(container)
                if review_data:
                    reviews.append(review_data)
            except Exception as e:
                self.logger.warning(f"Error parsing review container {i}: {str(e)}")
                continue

        return reviews

    def _parse_review_container(self, container) -> Optional[Dict]:
        """
        Parse individual review container with improved selectors for the actual HTML structure
        """
        try:
            # Initialize review dict
            review = {
                'username': '',
                'rating': '',
                'review_text': '',
                'date': '',
                'likes': '0',
                'comments': '0',
                'viewing_type': 'watch',
                'user_liked': False
            }

            # Extract user information - look for avatar link
            user_elem = container.find('a', class_='avatar')
            if user_elem:
                href = user_elem.get('href', '')
                if href:
                    # Extract username from URL
                    username = href.strip('/').split('/')[-1]
                    review['username'] = username

            # Extract rating from SVG aria-label: e.g. aria-label="★★★½"
            rating_svg = container.select_one('svg.glyph.-rating')
            if rating_svg:
                label = rating_svg.get('aria-label', '')
                if '★' in label:
                    stars = label.count('★')
                    review['rating'] = f"{stars}.5" if '½' in label else str(stars)

            # Extract date
            date_elem = container.find('time', class_='timestamp')
            if date_elem:
                review['date'] = date_elem.get('datetime', '') or date_elem.text.strip()

            # Extract review text - handle both expanded and collapsed text
            review_text = self._extract_full_review_text(container)
            review['review_text'] = review_text

            # Extract like count from data-count attribute on p.like-link-target
            like_p = container.select_one('p.like-link-target[data-count]')
            if like_p:
                review['likes'] = like_p.get('data-count', '0')

            # Extract comment count from the comments link label
            comment_label = container.select_one('a[href*="#comments"] span.label')
            if comment_label:
                review['comments'] = comment_label.text.strip() or '0'

            # Check if the viewer liked this review (filled heart SVG)
            if container.select_one('svg.inline-liked'):
                review['user_liked'] = True

            # Extract viewing context
            context_elem = container.find('span', class_='attribution-detail')
            if context_elem:
                context_text = context_elem.text.lower()
                if 'rewatched' in context_text:
                    review['viewing_type'] = 'rewatch'

            # Only return review if we have at least a username
            if review['username'] or review['review_text']:
                return review
            else:
                self.logger.debug("Skipping review - no username or text found")
                return None

        except Exception as e:
            self.logger.error(f"Error parsing review: {str(e)}")
            return None

    def _extract_full_review_text(self, container) -> str:
        """
        Extract full review text from the specific HTML structure
        """
        try:
            # Try to find the review body with the specific class from the HTML
            review_body = container.find('div', class_='body-text -prose -reset js-review-body')

            if not review_body:
                # Try alternative selectors
                review_body_selectors = [
                    'div.js-review-body',
                    'div.body-text',
                    'div.review-text'
                ]

                for selector in review_body_selectors:
                    review_body = container.select_one(selector)
                    if review_body:
                        break

            if review_body:
                # Check for collapsed text
                collapsed_div = review_body.find('div', class_='collapsed-text')

                if collapsed_div:
                    # Get all text from collapsed section
                    text_parts = []

                    # Get paragraphs
                    paragraphs = collapsed_div.find_all('p')
                    if paragraphs:
                        for p in paragraphs:
                            text_parts.append(p.text.strip())
                    else:
                        # Get all text
                        text_parts.append(collapsed_div.text.strip())

                    # Remove "more" link text if present
                    full_text = ' '.join(text_parts)
                    more_link = collapsed_div.find('a', class_='reveal')
                    if more_link:
                        more_text = more_link.text.strip()
                        if more_text in full_text:
                            full_text = full_text.replace(more_text, '').strip()

                    review_text = full_text
                else:
                    # Get all text directly
                    review_text = review_body.text.strip()

                # Clean up the text
                review_text = re.sub(r'\s+', ' ', review_text)  # Normalize whitespace
                review_text = review_text.replace('\n', ' ').replace('\r', ' ')

                return review_text.strip()

            return ""

        except Exception as e:
            self.logger.debug(f"Error extracting review text: {e}")
            return ""

    def _go_to_next_page(self) -> bool:
        """
        Navigate to the next page of reviews
        """
        try:
            # Try multiple XPath selectors for next button
            next_xpaths = [
                "//a[contains(@class, 'next') and not(contains(@class, 'disabled'))]",
                "//a[contains(text(), 'Newer')]",
                "//div[contains(@class, 'paginate-nextprev')]/a[not(contains(@class, 'disabled'))]"
            ]

            for xpath in next_xpaths:
                try:
                    next_buttons = self.driver.find_elements(By.XPATH, xpath)
                    for button in next_buttons:
                        if button.is_displayed() and button.is_enabled():
                            next_url = button.get_attribute('href')
                            if next_url:
                                self.logger.info(f"Navigating to next page: {next_url}")

                                # Scroll and click
                                self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                                time.sleep(1)
                                self.driver.execute_script("arguments[0].click();", button)

                                # Wait for page load
                                time.sleep(3)

                                # Wait for reviews to be present
                                try:
                                    self.wait.until(
                                        EC.presence_of_element_located(
                                            (By.XPATH,
                                             "//article[contains(@class, 'production-viewing')] | //div[contains(@class, 'listitem')]")
                                        )
                                    )
                                except TimeoutException:
                                    self.logger.warning("Timeout waiting for reviews, but continuing...")

                                return True
                except Exception as e:
                    self.logger.debug(f"Error with XPath {xpath}: {e}")
                    continue

            # If no next button found, check if we're on the last page
            try:
                next_disabled = self.driver.find_elements(By.XPATH,
                                                          "//a[contains(@class, 'next') and contains(@class, 'paginate-disabled')]")
                if next_disabled:
                    self.logger.info("On last page - no more pages to navigate")
            except:
                pass

            return False

        except Exception as e:
            self.logger.error(f"Error finding next page: {str(e)}")
            return False

    def save_to_csv(self, reviews_data: List[Dict], filename: str = 'letterboxd_reviews.csv') -> bool:
        """
        Save scraped reviews to a CSV file with only needed columns
        """
        if not reviews_data:
            self.logger.warning("No reviews to save")
            return False

        try:
            # Define CSV headers - only needed fields
            fieldnames = [
                'username',
                'rating',
                'review_text',
                'date',
                'likes',
                'comments',
                'viewing_type',
                'user_liked'
            ]

            # Filter reviews to only include the fields we want
            filtered_reviews = []
            for review in reviews_data:
                filtered_review = {}
                for field in fieldnames:
                    filtered_review[field] = review.get(field, '')
                filtered_reviews.append(filtered_review)

            # Save to CSV
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(filtered_reviews)

            self.logger.info(f"Saved {len(filtered_reviews)} reviews to {filename}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")
            return False

    def close(self):
        """Close the browser driver"""
        if self.driver:
            self.driver.quit()
            self.logger.info("Browser closed")


# Example usage and main function
def main():
    """Example usage of the enhanced scraper"""

    # Example URL
    movie_url = "https://letterboxd.com/film/sunshine-2024/reviews/by/activity/"

    print(f"\n{'=' * 60}")
    print(f"Scraping reviews from: {movie_url}")
    print(f"{'=' * 60}")

    # Create scraper instance
    scraper = EnhancedLetterboxdScraper(headless=False)  # Set headless=False to see the browser

    try:
        # Scrape reviews with limits
        reviews = scraper.scrape_reviews(
            movie_url,
            max_pages=2,  # Limit to 2 pages for testing
            max_reviews=30  # Limit to 30 reviews for testing
        )

        if reviews:
            # Display sample of scraped data
            print(f"\nSuccessfully scraped {len(reviews)} reviews!")

            # Show first review details
            if reviews:
                print(f"\n{'=' * 60}")
                print("SAMPLE REVIEW #1:")
                print(f"{'=' * 60}")
                sample = reviews[0]

                # Truncate long text for display
                max_display = 200
                review_text = sample.get('review_text', '')
                if len(review_text) > max_display:
                    review_text = review_text[:max_display] + "..."

                print(f"User: @{sample.get('username', '')}")
                print(f"Rating: {sample.get('rating', '')}")
                print(f"Date: {sample.get('date', '')}")
                print(f"Likes: {sample.get('likes', '0')}")
                print(f"Comments: {sample.get('comments', '0')}")
                print(f"Viewing Type: {sample.get('viewing_type', 'watch')}")
                print(f"User Liked: {sample.get('user_liked', False)}")
                print(f"\nReview Text:\n{review_text}")
                print(f"{'=' * 60}")

            # Generate filename from movie name
            movie_name = movie_url.split('/')[-3].replace('-', '_')
            filename = f"{movie_name}_reviews.csv"

            # Save to CSV
            if scraper.save_to_csv(reviews, filename):
                print(f"\n✓ Reviews saved to: {filename}")

        else:
            print("\n✗ No reviews were scraped.")

    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
    except Exception as e:
        print(f"\n✗ Error during scraping: {str(e)}")
    finally:
        # Always close the driver
        scraper.close()


def quick_scrape(url, output_file="reviews.csv", pages=3, reviews_limit=100):
    """
    Quick function to scrape reviews
    :param url: Letterboxd reviews URL
    :param output_file: Output CSV filename
    :param pages: Number of pages to scrape
    :param reviews_limit: Maximum number of reviews to collect
    """
    print(f"Starting quick scrape of {url}")
    scraper = EnhancedLetterboxdScraper(headless=True)

    try:
        reviews = scraper.scrape_reviews(url, max_pages=pages, max_reviews=reviews_limit)

        if reviews:
            scraper.save_to_csv(reviews, output_file)
            print(f"✓ Saved {len(reviews)} reviews to {output_file}")
        else:
            print("✗ No reviews found")

    finally:
        scraper.close()


if __name__ == "__main__":
    # Run the example
    main()

    # Or use the quick function:
    # quick_scrape(
    #     "https://letterboxd.com/film/sunshine-2024/reviews/by/activity/",
    #     "sunshine_reviews.csv",
    #     pages=5,
    #     reviews_limit=200
    # )