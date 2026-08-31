"""""This script uses Selenium to log into CompanyCam and MarketSharp, scrape the list of users from both platforms, and create a mapping of CompanyCam users to MarketSharp users based on exact name matches. 
""The resulting mapping is saved as a JSON file."""

import json
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

COMPANYCAM_EMAIL = "ryan@spicerbros.com"
COMPANYCAM_PASSWORD = "zexca4-nUhqyw-zovhog"
MARKETSHARP_CID = "4453"
MARKETSHARP_USERNAME = "rellis"
MARKETSHARP_PASSWORD = "Ryan123!"


def get_companycam_users(driver):
    users = set()
    try:
        print("[CompanyCam] Navigating to login page...")
        driver.get("https://app.companycam.com/signin")
        wait = WebDriverWait(driver, 20)
        print("[CompanyCam] Waiting for email input...")
        email_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#user_email_address")))
        print("[CompanyCam] Entering email...")
        email_input.clear()
        email_input.send_keys(COMPANYCAM_EMAIL)
        driver.save_screenshot("cc_after_email.png")
        print("[CompanyCam] Waiting for password input...")
        pass_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#user_password")))
        print("[CompanyCam] Entering password...")
        pass_input.clear()
        pass_input.send_keys(COMPANYCAM_PASSWORD)
        driver.save_screenshot("cc_after_password.png")
        # Handle cookie popup if present
        try:
            print("[CompanyCam] Checking for cookie popup...")
            cookie_btn = driver.find_element(By.CSS_SELECTOR, "button#onetrust-accept-btn-handler")
            if cookie_btn.is_displayed():
                print("[CompanyCam] Accepting cookies...")
                cookie_btn.click()
                WebDriverWait(driver, 10).until(
                    EC.invisibility_of_element_located((By.ID, "onetrust-group-container"))
                )
                print("[CompanyCam] Cookie popup dismissed.")
        except Exception:
            print("[CompanyCam] No cookie popup found.")
        print("[CompanyCam] Waiting for login button...")
        login_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input.ccb-blue.ccb-full")))
        print("[CompanyCam] Clicking login button...")
        login_btn.click()
        print("[CompanyCam] Waiting for navigation...")
        time.sleep(5)
        print("[CompanyCam] Navigating to users page...")
        driver.get("https://app.companycam.com/users")
        driver.save_screenshot("companycam_after_login.png")
        print("[CompanyCam] Waiting for user table...")
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-testid='users__table__name']")))
        # Robust scroll logic
        seen = set()
        scroll_attempts = 0
        max_scroll_attempts = 10
        while scroll_attempts < max_scroll_attempts:
            name_elements = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='users__table__name']")
            print(f"[CompanyCam] Found {len(name_elements)} user name elements after scroll {scroll_attempts+1}.")
            new_found = False
            for el in name_elements:
                cc_name = el.text.strip()
                if not cc_name or cc_name in seen:
                    continue
                # Check for deactivated status in the row
                try:
                    row = el.find_element(By.XPATH, "ancestor::tr")
                    # Look for a span with class 'sc-hKanyg' and text 'Deactivated'
                    deactivated_spans = row.find_elements(By.CSS_SELECTOR, "span.sc-hKanyg")
                    is_deactivated = False
                    for span in deactivated_spans:
                        if 'deactivated' in span.text.lower():
                            is_deactivated = True
                            break
                    if is_deactivated:
                        print(f"[CompanyCam] Skipping deactivated user: {cc_name}")
                        seen.add(cc_name)
                        continue
                    print(f"[CompanyCam] Adding active user: {cc_name}")
                    users.add(cc_name)
                    seen.add(cc_name)
                    new_found = True
                except Exception as ex:
                    print(f"[CompanyCam] Could not find ancestor row for {cc_name}, adding anyway. Error: {ex}")
                    users.add(cc_name)
                    seen.add(cc_name)
                    new_found = True
            # Scroll down
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            if not new_found:
                scroll_attempts += 1
            else:
                scroll_attempts = 0  # Reset if new users found
        print(f"[CompanyCam] Scraped {len(users)} active users.")
    except Exception as e:
        print(f"[CompanyCam] Error scraping users: {e}")
    return list(users)


def get_marketsharp_users(driver):
    users = set()
    try:
        print("[MarketSharp] Navigating to login page...")
        driver.get("https://www.marketsharpm.com/Login.aspx?ReturnUrl=%2FLogout.aspx")
        wait = WebDriverWait(driver, 20)
        print("[MarketSharp] Waiting for Company ID input...")
        cid_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#CompanyIDTextBox")))
        print("[MarketSharp] Entering Company ID...")
        cid_input.clear()
        cid_input.send_keys(MARKETSHARP_CID)
        driver.save_screenshot("ms_after_cid.png")
        print("[MarketSharp] Waiting for Username input...")
        user_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#UsernameTextBox")))
        print("[MarketSharp] Entering Username...")
        user_input.clear()
        user_input.send_keys(MARKETSHARP_USERNAME)
        driver.save_screenshot("ms_after_username.png")
        print("[MarketSharp] Waiting for Password input...")
        pass_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#PasswordTextBox")))
        print("[MarketSharp] Entering Password...")
        pass_input.clear()
        pass_input.send_keys(MARKETSHARP_PASSWORD)
        driver.save_screenshot("ms_after_password.png")
        print("[MarketSharp] Waiting for Login button...")
        login_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a#LoginButton.loginbutton")))
        print("[MarketSharp] Clicking Login button...")
        login_btn.click()
        print("[MarketSharp] Waiting for login to complete...")
        time.sleep(5)
        print("[MarketSharp] Navigating to Employee Maintenance page...")
        driver.get("https://www2.marketsharpm.com/Admin/EmployeeMaintenance.aspx")
        driver.save_screenshot("marketsharp_after_login.png")
        print("[MarketSharp] Waiting for user table...")
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "td.dxgv")))
        page_num = 1
        while True:
            print(f"[MarketSharp] Scraping page {page_num} of users...")
            time.sleep(2)
            username_elements = driver.find_elements(By.CSS_SELECTOR, "td.dxgv")
            for el in username_elements:
                text = el.text.strip()
                if text:
                    users.add(text)
            # Try to click next page
            try:
                next_btn = driver.find_element(By.XPATH, "//a[contains(@class, 'dxp-num') and text()='{0}']".format(page_num+1))
                if next_btn.is_displayed():
                    print(f"[MarketSharp] Clicking next page {page_num+1}...")
                    next_btn.click()
                    page_num += 1
                    time.sleep(2)
                else:
                    print("[MarketSharp] No more pages.")
                    break
            except Exception:
                print("[MarketSharp] No next page button found.")
                break
        print(f"[MarketSharp] Scraped {len(users)} users.")
    except Exception as e:
        print(f"[MarketSharp] Error scraping users: {e}")
    return list(users)


def match_cc_to_ms(cc_users, ms_users):
    mapping = {}
    ms_usernames_lower = {u.lower() for u in ms_users}
    for cc_name in cc_users:
        parts = cc_name.strip().split()
        if len(parts) >= 2:
            first, last = parts[0], parts[-1]
            ms_guess = (first[0] + last).lower()
            # Find a MarketSharp username with matching last name (case-insensitive)
            for ms_user in ms_users:
                if ms_user.lower() == ms_guess and last.lower() in ms_user.lower():
                    mapping[cc_name] = ms_user
                    break
            else:
                # fallback: just use the guess if it exists
                if ms_guess in ms_usernames_lower:
                    mapping[cc_name] = ms_guess
        # else: skip if not enough parts
    return mapping


def update_and_sort_mapping(new_mapping, json_path):
    # Load existing mapping if present
    if os.path.exists(json_path):
        with open(json_path, "r") as f:
            try:
                existing = json.load(f)
            except Exception:
                existing = {}
    else:
        existing = {}
    # Update and sort
    existing.update(new_mapping)
    sorted_mapping = dict(sorted(existing.items(), key=lambda x: x[0].lower()))
    with open(json_path, "w") as f:
        json.dump(sorted_mapping, f, indent=4)
    print(f"Mapping saved to {json_path}")


def main():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        print("Scraping CompanyCam users...")
        cc_users = get_companycam_users(driver)
        print(f"Found {len(cc_users)} CompanyCam users.")
        print("Scraping MarketSharp users...")
        ms_users = get_marketsharp_users(driver)
        print(f"Found {len(ms_users)} MarketSharp users.")
        # Map CC name to MS username using first initial + last name, case-insensitive
        mapping = match_cc_to_ms(cc_users, ms_users)
        update_and_sort_mapping(mapping, "data/companycam_to_marketsharp_user_map.json")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()

