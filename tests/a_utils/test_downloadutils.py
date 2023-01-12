from secfsdstools.a_utils.downloadutils import UrlDownloader
import os

test_download_url = 'https://www.sec.gov/dera/data/financial-statement-data-sets.html'


def test_download_to_string():
    downloader = UrlDownloader('my@test.com')

    download_content = downloader.get_url_content(test_download_url).text

    # we expect a html file
    assert "<html" in download_content
    assert "<body" in download_content


def test_download_to_zip():
    file_name = "testzipfile"
    written_file = None
    try:
        downloader = UrlDownloader('my@test.com')

        written_file = downloader.download_url_to_file(test_download_url, file_name)

        assert os.path.exists(written_file)

    finally:
        if written_file:
            os.remove(written_file)
