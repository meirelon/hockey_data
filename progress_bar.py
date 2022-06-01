import sys


def report(i, n):
    j = (i + 1) / n
    sys.stdout.write('\r')
    sys.stdout.write("[%-20s] %d%%" % ('=' * int(20 * j), 100 * j))
    sys.stdout.flush()


if __name__ == "__main__":
    l1 = range(2, 1000, 2)
    n = len(l1)
    l2 = [(report(i, n), x + i)[1] for i, x in enumerate(l1)]
