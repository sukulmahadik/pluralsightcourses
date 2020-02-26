def foo():
    raise ValueError('lalala')

with foo():
    print('in block')
    pass
