"""Generic utilities to use in other utilities"""
from typing import get_type_hints


def _validate_inputs(function, **kwargs):
    """Check if all inputs are of the correct type"""
    hints = get_type_hints(function)

    for k, v in kwargs.items():
        if k in hints.keys():
            if not isinstance(v, hints[k]):
                raise TypeError(
                    f"'{k}' must be of type '{hints[k].__name__}', not {type(v).__name__}"
                )
