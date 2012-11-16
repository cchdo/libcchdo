'''Utility functions for MATLAB writing.

'''

from datetime import datetime
import decimal

from libcchdo.log import LOG


def convert_value(name, value, ):
    """Make a fancy global value.

    A libcchdo.model.datafile.DataFile has several global value types.
    This function tries to convert things into basic types.

    Parameters:
        name (str) - the name of the global that is being passed (see
                below). Used to determine if a global is stringly-typed
                when it looks numeric.
        value (?) - The value to convert. Can be one of the following:
                - str -- converted to int if it seems numeric, but not if
                        the global is stringly-typed.
                - unicode -- converted to str, stripping the characters
                        that can't be encoded in UTF-8.
                - decimal.Decimal -- converted to float.
                Everything else is converted to None (a very broad
                rejection policy).

    Returns:
        various things (see above).

    """

    # List of stringly-typed globals that look like integers.
    LOOKS_LIKE_INT_BUT_ACTUALLY_STR = ("DATE", "TIME", )

    if value is None:
        return None

    elif type(value) is str:
        if value.isdigit():
            # Distinguish between the stringly-typed number-looking things
            # and the numeric things.
            if name in LOOKS_LIKE_INT_BUT_ACTUALLY_STR:
                return str(value)
            else:
                result = None
                try:
                    result = int(value)
                except ValueError:
                    try:
                        result = float(value)
                    except ValueError:
                        return value
                return result
        else:
            return str(value)

    # Special characters!
    elif type(value) is unicode:
        # Filter characters that can't be converted into UTF-8. Don't know
        # if it's necessary for writing to MAT, but since MATLAB structs
        # use string keys, I'll do it to be safe. Things won't look pretty,
        # but they'll fit.
        is_str_able = lambda c: ord(c) < 128
        str_able_chrs = filter(is_str_able, [c for c in value])
        return str(''.join(str_able_chrs))

    # For some reason scipy.io can't handle decimal.Decimal objects. We'll
    # just have to float() them.
    # FIXME this causes loss of precision.
    elif type(value) is decimal.Decimal:
        return float(value)

    elif type(value) in (int, float):
        return value

    elif type(value) is datetime:
        return value

    else:
        # Huh?
        LOG.error('%r: %r' % (name, value, ))
        return None


