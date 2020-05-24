import sys
import json
import pandas as pd
import scipy.stats as stats


def analysissentiment(ierjson, ieojson, mapreducejson):
    # create an empty pandas dataframe
    df = pd.DataFrame(columns=['SA2_id', 'ierscore',
                               'pop', 'ieoscore', 'sentiment'])

    for ierkey in ierjson:
        ierdata = ierkey['key']
        df = df.append(
            [{'SA2_id': ierkey['id'], 'ierscore': ierdata[1], 'pop': ierdata[0]}])
        df.sort_values(by='SA2_id', ascending=False, inplace=True)

    for ieokey in ieojson:
        ieodata = ieokey['key']
        try:
            df.loc[(df.SA2_id == ieokey['id']), 'ieoscore'] = ieodata[1]
        except:
            # print("area not exist", ieokey['id'], file=sys.stderr)
            pass

    for mapkey in mapreducejson:
        try:
            happypercentage = (
                mapkey['positive'])/(mapkey['positive'] + mapkey['neutral'] + mapkey['negative'])
            df.loc[(df.SA2_id == mapkey['area']),
                   'sentiment'] = happypercentage
        except:
            pass

    # print(df.head(5), file=sys.stderr)
    # df.to_csv('sentiment_ieo_ier.csv')
    df.dropna(subset=['sentiment'], inplace=True)
    iercorrelation = stats.pearsonr(df['sentiment'], df['ierscore'])
    ieocorrelation = stats.pearsonr(df['sentiment'], df['ieoscore'])
    print(iercorrelation, ieocorrelation)
    return iercorrelation, ieocorrelation


if len(sys.argv) < 2:
    print('Missing scenario argument', file=sys.stderr)
    sys.exit(1)

# Read input from stdin
stdinput = [line for line in sys.stdin]
ier = stdinput[0]
ieo = stdinput[1]
mapreduce_output = stdinput[2]


if sys.argv[1] == 'sentiment':
    ierjson = json.loads(ier)
    ieojson = json.loads(ieo)
    mapreducejson = json.loads(mapreduce_output)
    analysissentiment(ierjson, ieojson, mapreducejson)
else:
    print('Bad scenario argument', file=sys.stderr)
    sys.exit(1)
