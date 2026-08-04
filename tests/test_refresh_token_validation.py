import pytest

from abacus.api.errors import ApiError
from abacus.services.identity import _parse_refresh_token


@pytest.mark.parametrize(
    "token",
    [
        "missing-separator",
        "not-a-uuid." + "x" * 40,
        "00000000-0000-0000-0000-000000000001.short",
    ],
)
def test_refresh_token_parser_fails_closed(token: str) -> None:
    with pytest.raises(ApiError) as error:
        _parse_refresh_token(token)

    assert error.value.status_code == 401
    assert error.value.code == "invalid_refresh_token"
