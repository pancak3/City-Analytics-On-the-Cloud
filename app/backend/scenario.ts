import {Router, Request, Response, NextFunction} from 'express';
import {PythonShell} from 'python-shell';
import nano from './app';

const router = Router();

// fetches geojson, and caches it
let _geojson: any;
const fetch_geojson = (): Promise<any> => {
    return new Promise((resolve, reject) => {
        if (_geojson) {
            return resolve(_geojson);
        }

        // fetch all docs in areas db
        const area = nano.db.use('areas');
        area.list({include_docs: true})
            .then((body) => {
                _geojson = body.rows
                    // get documents
                    .map((r) => r.doc)
                    // remove areas 0, 1 (which has no geojson)
                    .filter(
                        (r: any) =>
                            r['_id'] !== '0' &&
                            r['_id'] !== '1' &&
                            r['_id'] != 'australia' &&
                            r['_id'] != 'out_of_australia'
                    );
                return resolve(_geojson);
            })
            .catch((err) => reject(err));
    });
};

// get all geojson
router.get('/geojson', async (req, res, next) => {
    try {
        return res.json(await fetch_geojson());
    } catch (err) {
        return next(err);
    }
});

// count by area (no filter keyword)
// sample output: {"area1": count}
router.get(
    '/counts',
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const status = nano.db.use('statuses');

            const tweet_count_area = await status.view(
                'api-global',
                'count-area',
                {
                    group: true,
                    reduce: true,
                    stale: 'ok',
                }
            );

            const ret: { [area: string]: any } = {};
            for (const area of tweet_count_area.rows) {
                ret[area.key] = area.value;
            }
            return res.json(ret);
        } catch (err) {
            return next(err);
        }
    }
);

// keyword count for all areas
// sample output: {"area1": count}
router.get(
    '/keyword/all',
    async (req: Request, res: Response, next: NextFunction) => {
        const keyword = req.query.keyword;

        try {
            const status = nano.db.use('statuses');

            const keyword_areas = await status.view('api-global', 'keyword', {
                start_key: [keyword],
                end_key: [keyword, {}],
                group: true,
                reduce: true,
                stale: 'ok',
            });

            const ret: { [area: string]: any } = {};
            for (const row of keyword_areas.rows) {
                ret[row.key[1]] = row.value;
            }
            return res.json(ret);
        } catch (err) {
            return next(err);
        }
    }
);

// tweets by area and keyword
// sample output: [text1, text2]
router.get(
    '/keyword/:area',
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const status = nano.db.use('statuses');

            const keyword = req.query.keyword;
            const area = req.params.area;

            // selection of tweets with keyword (e.g. first 10)
            const tweets: any = keyword
                ? await status.partitionedView(area, 'api', 'keyword', {
                    include_docs: true,
                    key: keyword,
                    group: false,
                    reduce: false,
                    limit: 5,
                    stale: 'ok',
                })
                : // no keyword
                await status.partitionedView(area, 'api', 'doc', {
                    include_docs: true,
                    limit: 5,
                    stale: 'ok',
                });
            return res.json(tweets.rows.map((r: any) => r.value));
        } catch (err) {
            return next(err);
        }
    }
);

// hashtags count for all areas
// sample output: {['hashtag_top1': 9], ... }
router.get(
    '/hashtags/all',
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const status = nano.db.use('statuses');

            const hashtags = await status.view('api-global', 'hashtags', {
                reduce: false,
                stale: 'ok',
            });
            return res.json(hashtags_freq(hashtags));
        } catch (err) {
            return next(err);
        }
    }
);

// hashtags count for all areas
// sample output: {['hashtag_top1': 9], ... }
router.get(
    '/hashtags/:area',
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const status = nano.db.use('statuses');
            const area = req.params.area;

            const hashtags = await status.partitionedView(area, 'api', 'hashtags', {
                reduce: false,
                stale: 'ok',
            });
            return res.json(hashtags_freq(hashtags));
        } catch (err) {
            return next(err);
        }
    }
);

// Takes rows res from couch view results
// Returns top 20 freq
// Ex:[["melbourne",298],["australia",261], ...]
const hashtags_freq = (hashtags: any): any => {
    const freq: { [hashtag: string]: any } = {};
    for (const row of hashtags.rows) {
        if (freq[row.key]) {
            freq[row.key] = freq[row.key] + 1;
        } else {
            freq[row.key] = 1;
        }
    }
    // https://stackoverflow.com/questions/25500316
    // Create items array
    const ret = Object.keys(freq).map(function (key) {
        return [key, freq[key]];
    });

    // Sort the array based on the second element
    ret.sort(function (first, second) {
        return second[1] - first[1];
    });

    return ret.slice(0, 10);
};

// Takes:
// [{ key: [ '10050', 0 ], value: 9 },
// { key: [ '10050', 1 ], value: 15 }]
// Combines to:
// [{ area: '10050', 0: 9, 1: 15 }]
const view_bool_process = (rows: any): any => {
    const transformed: any = {};
    for (const row of rows) {
        if (!transformed[row.key[0]]) {
            transformed[row.key[0]] = {
                area: row.key[0],
            };
        }

        transformed[row.key[0]] = {
            ...transformed[row.key[0]],
            [row.key[1]]: row.value,
        };
    }

    return Object.keys(transformed).map((d) => transformed[d]);
};

// fetch ier data
let _ier: any;
const fetch_ier = (): Promise<any> => {
    return new Promise((resolve, reject) => {
        if (_ier) {
            return resolve(_ier);
        }

        // fetch all docs in areas db
        const ier = nano.db.use('aurin_ier');
        ier.view('analysis', 'general')
            .then((body) => {
                _ier = body.rows;
                return resolve(_ier);
            })
            .catch((err) => reject(err));
    });
};

// fetch ieo data
let _ieo: any;
const fetch_ieo = (): Promise<any> => {
    return new Promise((resolve, reject) => {
        if (_ieo) {
            return resolve(_ieo);
        }

        // fetch all docs in areas db
        const ieo = nano.db.use('aurin_ieo');
        ieo.view('analysis', 'general')
            .then((body) => {
                _ieo = body.rows;
                return resolve(_ieo);
            })
            .catch((err) => reject(err));
    });
};

// fetch ieo/ier data
let _ieo_ier: any;
const fetch_ieo_ier = async () => {
    if (_ieo_ier) return _ieo_ier;

    const [aurin_ieo, aurin_ier] = await Promise.all([
        fetch_ieo(),
        fetch_ier(),
    ]);

    // combine ieo/ier from views
    const ieo_ier: any = {};
    for (const ier of aurin_ier) {
        const area = ier['id'];
        const ier_pop = ier['key'][0];
        const ier_score = ier['key'][1];
        ieo_ier[area] = {ier_pop, ier_score};
    }
    for (const ieo of aurin_ieo) {
        const area = ieo['id'];
        const ieo_pop = ieo['key'][0];
        const ieo_score = ieo['key'][1];
        if (!ieo_ier[area]) {
            ieo_ier[area] = {ieo_pop, ieo_score};
        } else {
            ieo_ier[area] = {...ieo_ier[area], ieo_pop, ieo_score};
        }
    }

    // normalise values
    // todo: merge into above
    const ier_values = aurin_ier
        .map((ier: any) => ier['key'][1])
        .filter((i: number) => i !== 0);
    const ier_min = Math.min(...ier_values);
    const ier_max = Math.max(...ier_values);

    const ieo_values = aurin_ieo
        .map((ieo: any) => ieo['key'][1])
        .filter((i: number) => i !== 0);
    const ieo_min = Math.min(...ieo_values);
    const ieo_max = Math.max(...ieo_values);

    for (const area of Object.keys(ieo_ier)) {
        ieo_ier[area].ieo_normalised = ieo_ier[area].ieo_score
            ? (ieo_ier[area].ieo_score - ieo_min) / (ieo_max - ieo_min)
            : 0;
        ieo_ier[area].ier_normalised = ieo_ier[area].ier_score
            ? (ieo_ier[area].ier_score - ier_min) / (ier_max - ier_min)
            : 0;
    }
    _ieo_ier = ieo_ier;
    return _ieo_ier;
};

// sentiment all areas
router.get(
    '/sentiment',
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const status = nano.db.use('statuses');

            // sentiments counts [+, n, -]
            const sentimentPromise = status.view('api-global', 'sentiment', {
                group: true,
                reduce: true,
                stale: 'ok',
            });

            const [sentiment, areas] = await Promise.all([
                sentimentPromise,
                fetch_ieo_ier(),
            ]);

            // group sentiments by area
            const transformed: any = view_bool_process(sentiment.rows);

            // correlation analysis
            const [aurin_ieo, aurin_ier] = await Promise.all([
                fetch_ieo(),
                fetch_ier(),
            ]);
            const analysis_result: string = await analyse(
                ['sentiment'],
                JSON.stringify(aurin_ier) +
                '\n' +
                JSON.stringify(aurin_ieo) +
                '\n' +
                JSON.stringify(transformed)
            );
            const analysis_result_cleaned = analysis_result
                .split(' ')
                .map((value) =>
                    value.replace(',', '').replace('(', '').replace(')', '')
                );

            // Merge sentiments and aurin scores
            for (const sentiment_area of transformed) {
                if (areas[sentiment_area.area]) {
                    areas[sentiment_area.area] = {
                        ...areas[sentiment_area.area],
                        ...sentiment_area,
                    };
                    delete sentiment_area.area;
                }
            }
            return res.json({ areas, correlation: analysis_result_cleaned });
        } catch (err) {
            return next(err);
        }
    }
);

const analyse = (args: string[], stdin: string): Promise<string> => {
    return new Promise((resolve, reject) => {
        const pyshell = new PythonShell('analysis/main.py', {
            mode: 'text',
            args: ['sentiment'],
        });

        pyshell.on('message', (message) => {
            return resolve(message);
        });
        pyshell.send(stdin);
        pyshell.end((err, code) => {
            if (err) return reject(err);
        });
    });
};

export default router;
