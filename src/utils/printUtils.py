from colored import fg, bg, attr


def printWithClr(inp: str, fontClr: str, backClr: str = None) -> None:
    if backClr in [None, '']:
        print('{0}{2}{1}'.format(fg(fontClr), attr('reset'), inp))
        return
    print('{0}{1}{3}{2}'.format(fg(fontClr), bg(backClr), attr('reset'), inp))
