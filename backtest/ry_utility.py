import sys
import pandas as pd
import datetime as dt

typeAbbreviation = {
    'tstamp': type(pd.Timestamp('11:30:00')),
    'dttime':type(dt.time(11,30)),    
    'dtdate':type(dt.date(2015,1,1)),
    'dt':type(dt.datetime(2015,1,1,1,1,1)),
    'df':type(pd.DataFrame()),
    'b':type(True),
    'str':type('brandon')
       
}





'''
    类型检查装饰器  
    >>> NONE, MEDIUM, STRONG = 0, 1, 2
    >>>
    >>> @accepts(int, int, int)
    ... def average(x, y, z):
    ...     return (x + y + z) / 2
    ...
    >>> average(5.5, 10, 15.0)
    TypeWarning:  'average' method accepts (int, int, int), but was given
    (float, int, float)
    15.25
'''
def accepts(*types, **kw):
    """ Function decorator. Checks that inputs given to decorated function
    are of the expected type.

    Parameters:
    types -- The expected types of the inputs to the decorated function.
             Must specify type for each parameter.
    kw    -- Optional specification of 'debug' level (this is the only valid
             keyword argument, no other should be given).
             debug = ( 0 | 1 | 2 )

    """
    if not kw:
        # default level: MEDIUM
        debug = 1
    else:
        debug = kw['debug']
    try:
        def decorator(f):
            def newf(*args):
                if debug == 0:
                    return f(*args)
                assert len(args) == len(types)
                argtypes = tuple(map(type, args))
                if argtypes != types:
                    msg = info(f.__name__, types, argtypes, 0)
                    if debug == 1:
                        print (sys.stderr, 'TypeWarning: ', msg)
                    elif debug == 2:
                        raise TypeError(msg)
                return f(*args)
            newf.__name__ = f.__name__
            return newf
        return decorator
    except KeyError as err:
        raise KeyError(err + "is not a valid keyword argument")
    except TypeError as err:
        raise TypeError(err)

def info(fname, expected, actual, flag):
    """ Convenience function returns nicely formatted error/warning msg. """
    format = lambda types: ', '.join([str(t).split("'")[1] for t in types])
    expected, actual = format(expected), format(actual)
    msg = "'%s' method " % fname \
          + ("accepts", "returns")[flag] + " (%s), but " % expected\
          + ("was given", "result is")[flag] + " (%s)" % actual
    return msg
