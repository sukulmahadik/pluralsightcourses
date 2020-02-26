[(x, y) for x in range(10) for y in range(x)]

result = []
for x in range(10):
    for y in range(x):
        result.append((x, y))
