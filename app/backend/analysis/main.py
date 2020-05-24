import sys
import json
import pandas as pd


def analysis_sentiment(ier_json, ieo_json, mapreduce_json):
    area_codes, ier_scores = [], []
    for row in ier_json:
        area_codes.append(row['id'])
        ier_scores.append(row['key'][1])
    df_ier = pd.DataFrame(columns=['ier_score'], data=ier_scores, index=area_codes).sort_index()

    area_codes, ieo_scores = [], []
    for row in ieo_json:
        area_codes.append(row['id'])
        ieo_scores.append(row['key'][1])
    df_ieo = pd.DataFrame(columns=['ieo_score'], data=ieo_scores, index=area_codes).sort_index()

    area_codes, sentiments = [], []
    for row in mapreduce_json:

        area_codes.append(row['area'])
        if 'negative' not in row:
            row['negative'] = 0
        if 'neutral' not in row:
            row['neutral'] = 0
        if 'positive' not in row:
            row['positive'] = 0
        total = row['positive'] + row['negative'] + row['neutral']
        sentiments.append([row['negative'] / total if row['negative'] else 0,
                           row['neutral'] / total if row['neutral'] else 0,
                           row['positive'] / total if row['positive'] else 0])

    df_sentiments = pd.DataFrame(columns=['negative_ratio', 'neutral_ratio', 'positive_ratio'],
                                 data=sentiments, index=area_codes).sort_index()
    negative_ier = get_pearson_corr(df_sentiments['negative_ratio'], df_ier['ier_score'])
    neutral_ier = get_pearson_corr(df_sentiments['neutral_ratio'], df_ier['ier_score'])
    positive_ier = get_pearson_corr(df_sentiments['positive_ratio'], df_ier['ier_score'])
    negative_ieo = get_pearson_corr(df_sentiments['negative_ratio'], df_ieo['ieo_score'])
    neutral_ieo = get_pearson_corr(df_sentiments['neutral_ratio'], df_ieo['ieo_score'])
    positive_ieo = get_pearson_corr(df_sentiments['positive_ratio'], df_ieo['ieo_score'])
    print(negative_ier, neutral_ier, positive_ier, negative_ieo, neutral_ieo, positive_ieo)
    return negative_ier, neutral_ier, positive_ier, negative_ieo, neutral_ieo, positive_ieo


def get_pearson_corr(a, b):
    pearson_corr = pd.DataFrame([a, b]).T.corr(method='pearson')
    return pearson_corr.iloc[0, 1]


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Missing scenario argument', file=sys.stderr)
        sys.exit(1)

    # Read input from stdin
    std_input = [line for line in sys.stdin]
    ier = std_input[0]
    ieo = std_input[1]
    mapreduce_output = std_input[2]

    if sys.argv[1] == 'sentiment':
        ier_json = json.loads(ier)
        ieo_json = json.loads(ieo)
        mapreduce_json = json.loads(mapreduce_output)
        analysis_sentiment(ier_json, ieo_json, mapreduce_json)

    else:
        print('Bad scenario argument', file=sys.stderr)
        sys.exit(1)