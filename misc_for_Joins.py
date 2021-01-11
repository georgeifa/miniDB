def break_when_ascending(operator,left,right):
    switcher = {#   check the  operator and compare the 2 values
        "==": left < right,
        "<": left >= right,
        "<=": left > right,
        ">": left <= right,
        ">=": left < right,
    }

    try:
        return switcher[operator] # return the result of the comparison
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False


def break_when_descending(operator,left,right): # works the same way as the above but it is for a descending sorted table
    switcher = {
        "==": left >= right,
        "<": left < right,
        "<=": left <= right,
        ">": left > right,
        ">=": left >= right,
    }

    try:
        return switcher[operator]
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False
