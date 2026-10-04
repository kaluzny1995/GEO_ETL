import pytest


def test_unknown_attribute_raises_attribute_error():
    import factory

    attribute = "NoSuchFactory"
    with pytest.raises(AttributeError, match="has no attribute"):
        getattr(factory, attribute)
