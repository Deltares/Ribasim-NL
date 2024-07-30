def snake_to_pascal_case(snake_case: str) -> str:
    """Convert snake_case to PascalCase"""
    words = snake_case.split("_")
    return "".join(i.title() for i in words)


def pascal_to_snake_case(pascal_case: str) -> str:
    """Convert PascalCase to snake_case"""
    return "".join(["_" + i.lower() if i.isupper() else i for i in pascal_case]).lstrip("_")
