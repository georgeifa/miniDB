def break_when_ascending(operator,left,right):
    switcher = {
        "==": left < right,
        "<": left >= right,
        "<=": left > right,
        ">": left <= right,
        ">=": left < right,
    }

    try:
        return switcher[operator]
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False
