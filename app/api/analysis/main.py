import sys

if len(sys.argv) < 2:
    print('Missing scenario argument', file=sys.stderr)
    sys.exit(1)

# Read input from stdin
mapreduce_input = ''.join([line for line in sys.stdin])

if sys.argv[1] == 'exercise':
    print(mapreduce_input)
    pass
else:
    print('Bad scenario argument', file=sys.stderr)
    sys.exit(1)
