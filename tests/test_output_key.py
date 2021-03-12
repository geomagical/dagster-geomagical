from unittest.mock import patch, mock_open

from dagster_geomagical.output_key import output_key_for


@patch("builtins.open", mock_open(read_data=b"fake"))
def test_output_key_for():
    assert (
        output_key_for("1234/foo.png", "ignored.png")
        == "1234/foo_c053ecf9ed41df0311b9df13cc6c3b6078d2d3c2.png"
    )
    assert (
        output_key_for("1234/foo.png", "ignored.png", ["simple"])
        == "1234/foo_simple_c053ecf9ed41df0311b9df13cc6c3b6078d2d3c2.png"
    )
    assert (
        output_key_for("1234/foo.png", "ignored.png", ["simple", "other"])
        == "1234/foo_simple+other_c053ecf9ed41df0311b9df13cc6c3b6078d2d3c2.png"
    )
