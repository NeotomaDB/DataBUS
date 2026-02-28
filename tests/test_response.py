"""Tests for the Response base class."""
import pytest
from DataBUS import Response


def test_response_init():
    r = Response()
    assert r.valid == []
    assert r.message == []
    assert r.id_int is None
    assert r.id_list == []
    assert r.validAll is False  # empty list → False


def test_response_valid_all_true():
    r = Response()
    r.valid = [True, True, True]
    assert r.validAll is True


def test_response_valid_all_false():
    r = Response()
    r.valid = [True, False, True]
    assert r.validAll is False


def test_response_str_contains_messages():
    r = Response()
    r.valid.append(True)
    r.message.append("All good")
    text = str(r)
    assert "All good" in text


def test_response_str_shows_false():
    r = Response()
    r.valid.append(False)
    r.message.append("Something failed")
    text = str(r)
    assert "FALSE" in text or "False" in text
