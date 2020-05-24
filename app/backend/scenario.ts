import { Router, Request, Response, NextFunction } from 'express';
import { PythonShell } from 'python-shell';
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
        area.list({ include_docs: true })
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
                console.log(body);
                _ier = body.rows;
                return resolve(_ier);
            })
            .catch((err) => reject(err));
    });
};

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

// sentiment all areas
router.get(
    '/sentiment',
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const status = nano.db.use('statuses');

            const sentimentPromise = status.view('api-global', 'sentiment', {
                group: true,
                reduce: true,
                stale: 'ok',
            });

            const [sentiment, aurin_ieo, aurin_ier] = await Promise.all([
                sentimentPromise,
                fetch_ieo(),
                fetch_ier(),
            ]);

            // transform into area
            const transformed = view_bool_process(sentiment.rows);

            const pyshell = new PythonShell('analysis/main.py', {
                mode: 'text',
                args: ['sentiment'],
            });

            pyshell.on('message', (message) => {
                // console.log(message);
            });
            pyshell.send(
                JSON.stringify(aurin_ier) +
                    '\n' +
                    JSON.stringify(aurin_ieo) +
                    '\n' +
                    JSON.stringify(transformed)
            );
            pyshell.end((err, code) => {
                if (err) throw err;
                console.log(code);
            });
        } catch (err) {
            return next(err);
        }
    }
);

export default router;
