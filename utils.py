def str_to_float(value: str) -> float:
    if isinstance(value, str):
        try:
            value = value.replace(",", ".")
            value = value.replace(" ", "")
            return float(value)
        except ValueError:
            return None
    return value
