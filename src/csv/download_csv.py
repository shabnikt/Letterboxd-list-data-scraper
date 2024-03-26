import os
from os.path import join, dirname
import shutil
import fnmatch
import zipfile
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


def load_from_boxd():
    url = "https://letterboxd.com/data/export"

    username = os.getenv('LETTERBOXD_USERNAME', None)
    password = os.getenv('LETTERBOXD_PASSWORD', None)

    driver = webdriver.Chrome()

    driver.get(url)

    username_field = driver.find_element(By.ID, "field-username")
    password_field = driver.find_element(By.ID, "field-password")
    submit_button = driver.find_element(By.CSS_SELECTOR, "button.standalone-flow-button.-inline.-action.-activity-indicator")

    username_field.send_keys(username)
    password_field.send_keys(password)
    submit_button.click()

    wait = WebDriverWait(driver, 4)
    try:
        wait.until(EC.presence_of_element_located((By.ID, "file-picker-input")))
    except Exception as e:
        print('Quit webdriver')

    driver.quit()


def unpack_zip(zip_file_path, project_dir):
    folder_name = join(project_dir, 'letterboxd-data')
    os.makedirs(folder_name, exist_ok=True)

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(folder_name)


def move_files(project_dir):
    directory = os.getenv('DOWNLOAD_DIRECTORY', None)
    for file in os.listdir(directory):
        if fnmatch.fnmatch(file, 'letterboxd-*.zip'):
            matching_file = os.path.join(directory, file)
            break
    else:
        matching_file = None

    if matching_file:
        print(f"Found file: {matching_file}")
        destination_path = os.path.join(project_dir, 'letterboxd-data.zip')
        try:
            shutil.move(matching_file, destination_path)
            print(f"File successfully moved to {destination_path}")
        except Exception as e:
            print(f"Error moving file: {e}")
        unpack_zip(destination_path, project_dir)
        os.remove(destination_path)
    else:
        print("No file matching the specified pattern was found")


def create_list_ignore(project_dir):
    folder_path = join(project_dir, 'letterboxd-data\\lists')

    file_names = os.listdir(folder_path)
    file_names = [os.path.splitext(file)[0] for file in file_names if file.endswith('.csv')]

    output_file_path = join(project_dir, 'ignore_list.txt')
    shutil.rmtree(output_file_path, ignore_errors=True)
    print(f"Create {output_file_path}")
    with open(output_file_path, 'w') as output_file:
        for file_name in file_names:
            output_file.write(file_name + '\n')


def download_csv():
    project_dir = dirname(dirname(dirname(__file__)))

    dotenv_path = join(project_dir, 'letterboxd.env')
    load_dotenv(dotenv_path)

    shutil.rmtree(join(project_dir, 'letterboxd-data'), ignore_errors=True)

    load_from_boxd()
    move_files(project_dir)
    if os.getenv('CREATE_LIST_IGNORE', None):
        create_list_ignore(project_dir)


if __name__ == "__main__":
    download_csv()
