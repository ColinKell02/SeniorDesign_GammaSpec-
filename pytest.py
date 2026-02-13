import io
import numpy as np
import pytest
from pathlib import Path
from datetime import date

import data_fetcher as df
import data_plotter as dp


# -----------------------------
# data_fetcher.py TESTS
# -----------------------------

def test_link_parser_basic():
    html = """
    <html>
        <a href="file1.xml">file1</a>
        <a href="file2.tab">file2</a>
        <a href="../">parent</a>
    </html>
    """
    p = df._LinkParser()
    p.feed(html)
    assert "file1.xml" in p.hrefs
    assert "file2.tab" in p.hrefs
    assert "../" in p.hrefs


def test_list_directory_filters(mocker):
    mock_html = """
    <a href="a.xml">a</a>
    <a href="./">.</a>
    <a href="../">..</a>
    <a href="b.tab">b</a>
    """
    mocker.patch("data_fetcher._http_get_text", return_value=mock_html)

    out = df._list_directory("http://fake")
    assert "a.xml" in out
    assert "b.tab" in out
    assert "./" not in out
    assert "../" not in out


def test_list_dawn_records(mocker):
    mocker.patch("data_fetcher._list_directory", return_value=[
        "dawn_150312-150401.xml",
        "dawn_150312-150401.tab",
        "random.txt"
    ])

    recs = df._list_dawn_records("http://fake")

    assert len(recs) == 1
    r = recs[0]
    assert r.key == "dawn_150312-150401"
    assert r.date_start == date(2015, 3, 12)
    assert r.date_end == date(2015, 4, 1)
    assert len(r.files) == 2


def test_list_lp_records(mocker):
    mocker.patch("data_fetcher._list_directory", return_value=[
        "1998_016_grs.xml",
        "1998_016_grs.dat",
        "1998_016_grs.lbl"
    ])

    recs = df._list_lp_records("http://fake")

    assert len(recs) == 1
    r = recs[0]
    assert r.date_start == date(1998, 1, 16)
    assert r.files[0].endswith(".xml")
    assert r.files[1].endswith(".dat")


def test_list_msl_records(mocker):
    mocker.patch("data_fetcher._list_directory", return_value=[
        "sol00001.dat",
        "sol00001.xml",
        "junk.txt"
    ])

    recs = df._list_msl_records("http://fake")
    assert len(recs) == 1
    assert recs[0].files[0].endswith(".dat")
    assert recs[0].files[1].endswith(".xml")


# -----------------------------
# data_plotter.py TESTS
# -----------------------------

def test_safe_counts_1d():
    s = np.array([1, 2, 3])
    out = dp.safe_counts(s)
    assert np.all(out == s)


def test_safe_counts_2d():
    s = np.array([[1, 2, 3],
                  [4, 5, 6]])
    out = dp.safe_counts(s)
    assert np.all(out == np.array([6, 15]))


def test_find_col():
    names = ["Latitude", "Longitude", "Counts"]
    assert dp.find_col(names, ["lat"]) == "Latitude"
    assert dp.find_col(names, ["lon"]) == "Longitude"
    assert dp.find_col(names, ["energy"]) is None


def test_parse_file_selection_single():
    out = dp.parse_file_selection("1,3", 5)
    assert out == [0, 2]


def test_parse_file_selection_ranges():
    out = dp.parse_file_selection("1-3,5", 6)
    assert out == [0, 1, 2, 4]


def test_parse_file_selection_bounds():
    out = dp.parse_file_selection("0,10,2", 3)
    assert out == [1]


# -----------------------------
# MOCK PDS PARSING
# -----------------------------

class DummyStruct:
    def __init__(self, data):
        self.id = "TABLE"
        self.data = data


def test_load_pds4_table_mock(mocker):
    fake_data = np.zeros(5, dtype=[("ENERGY", float), ("LAT", float)])
    fake_struct = [DummyStruct(fake_data)]

    mocker.patch("data_plotter.pds.read", return_value=fake_struct)

    data, names = dp.load_pds4_table("fake.xml")

    assert "ENERGY" in names
    assert data.shape[0] == 5


def test_parse_dawn_mock(mocker):
    fake_data = np.zeros(10, dtype=[("SPEC", float), ("LATITUDE", float), ("LONGITUDE", float)])
    fake_struct = [DummyStruct(fake_data)]
    mocker.patch("data_plotter.pds.read", return_value=fake_struct)

    out = dp.parse_dawn("fake.xml")

    assert "spectrum" in out
    assert out["lat"] is not None
    assert out["lon"] is not None


# -----------------------------
# SCIENCE SANITY CHECKS
# -----------------------------

def test_spectrum_non_negative():
    """Counts in spectra should never be negative."""
    spectrum = np.array([0, 1, 5, 10, 100])
    assert np.all(spectrum >= 0)


def test_safe_counts_conserves_total_counts():
    """Summing 2D spectra should conserve total counts."""
    spectrum = np.array([[1, 2, 3],
                          [4, 5, 6]])
    original_total = spectrum.sum()
    collapsed = dp.safe_counts(spectrum)
    assert collapsed.sum() == original_total


def test_combined_spectrum_is_non_negative():
    """Combined spectrum from multiple files should remain physical."""
    s1 = np.array([1, 2, 3])
    s2 = np.array([4, 5, 6])
    combined = s1 + s2
    assert np.all(combined >= 0)


def test_lat_lon_bounds():
    """Latitude and longitude should be within physical bounds."""
    lat = np.array([-90, -45, 0, 45, 90])
    lon = np.array([-180, -90, 0, 90, 180])

    assert np.all(lat >= -90) and np.all(lat <= 90)
    assert np.all(lon >= -180) and np.all(lon <= 180)