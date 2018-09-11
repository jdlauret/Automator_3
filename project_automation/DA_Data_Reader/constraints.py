def level_5(cell, sheet):
    """
    level_5 evaluates a string for a Level 5 fail
    :param cell: Contents of the cell to evaluate
    :param sheet: The sheet the cell belongs to
    :return: Boolean
    """
    if sheet == 'old fs':
        check = cell.lower()
        try:
            if check.index('level 5') > -1 and check.index('fail') > -1:
                return True
        except:
            return False
    return False


def level_4(cell, sheet):
    """
    level_4 evaluates a string for a Level 4 fail
    :param cell: Contents of the cell to evaluate
    :param sheet: The sheet the cell belongs to
    :return: Boolean
    """
    if sheet == 'old fs':
        check = cell.lower()
        try:
            if check.index('level 4') > -1 and check.index('fail') > -1:
                return True
        except:
            return False
    return False


def level_3(cell, sheet):
    """
    level_3 evaluates a string for a Level 3 fail
    :param cell: Contents of the cell to evaluate
    :param sheet: The sheet the cell belongs to
    :return: Boolean
    """
    if sheet == 'old fs':
        check = cell.lower()
        try:
            if check.index('level 3') > -1 and check.index('fail') > -1:
                return True
        except:
            return False
    return False


def level_2(cell, sheet):
    """
    level_2 evaluates a string for a Level 2 fail
    :param cell: Contents of the cell to evaluate
    :param sheet: The sheet the cell belongs to
    :return: Boolean
    """
    if sheet == 'old fs':
        check = cell.lower()
        try:
            if check.index('level 3') > -1 and check.index('fail') > -1:
                return True
        except:
            return False
    return False


def level_1(cell, sheet):
    """
    level_1 evaluates a string for a Level 1 fail
    :param cell: Contents of the cell to evaluate
    :param sheet: The sheet the cell belongs to
    :return: Boolean
    """
    if sheet == 'old fs':
        check = cell.lower()
        try:
            if check.index('level 3') > -1 and check.index('fail') > -1:
                return True
        except:
            return False
    return False


def pass_check(cell, sheet):
    """
    level_1 evaluates a string for a Level 1 fail
    :param cell: Contents of the cell to evaluate
    :param sheet: The sheet the cell belongs to
    :return: Boolean
    """
    if sheet == 'old fs':
        check = cell.lower()
        try:
            if check.index('pass') > -1:
                return True
        except:
            return False
    return False
