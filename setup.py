
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='drive_etl_tools',
    version='3.0.1',
    author='John R',
    author_email='JohnREngineer@gmail.com',
    description='Tools for doing automated ETL for sheets and files in Google Drive.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/JohnREngineer/drive_etl_tools',
    project_urls = {
        "Bug Tracker": "https://github.com/mike-huls/JohnREngineer/drive_etl_tools"
    },
    license='https://www.fsf.org/licensing/licenses/agpl-3.0.html',
    packages=['drive_etl_tools'],
    install_requires=['pydrive2','gspread','pandas'],
)