import contextlib

@contextlib.contextmanager
def nest_test(name):
    print('Entering', name)
    yield name
    print('Exiting', name)

with nest_test('outer') as n1, nest_test('inner nested in ' + n1):
    pass
