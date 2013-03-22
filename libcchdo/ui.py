TERMCOLOR_ESCAPE = '\x1b\x5b'


color_number = dict((x, i) for i, x in enumerate([
    'black', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan', 'white',
    'gray']))


def termcolor(color, bold=False, bg=False):
    """Generate a termcolor escape code."""
    number = color_number.get(color.lower(), 0)
    if number is 0:
        return '{0}0m'.format(TERMCOLOR_ESCAPE)
    if bg:
        number += 40
    else:
        number += 30
    if bold:
        boldnum = '1'
    else:
        boldnum = '0'
    return '{0}{1};{2}m'.format(TERMCOLOR_ESCAPE, boldnum, number)


TERMCOLOR = {
    'WHITE': termcolor('white'),
    'BOLDWHITE': termcolor('white', True),
    'BOLDRED': termcolor('red', True),
    'BOLDYELLOW': termcolor('yellow', True),
    'RED': termcolor('red'),
    'GREEN': termcolor('green'),
    'YELLOW': termcolor('yellow'),
    'BLUE': termcolor('blue'),
    'CYAN': termcolor('cyan'),
    'CLEAR': termcolor('reset'),
}
