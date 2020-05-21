import { Router, Request, Response } from 'express';
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
        area.list({ include_docs: true }).then((body) => {
            _geojson = body.rows
                // get documents
                .map((r) => r.doc)
                // remove areas 0, 1 (which has no geojson)
                .filter((r: any) => r['_id'] !== '0' && r['_id'] !== '1');
            return resolve(_geojson);
        });
    });
};

// get all geojson
router.get('/geojson', async (req, res) => {
    return res.json(await fetch_geojson());
});

// count by area (no filter keyword)
// sample output: {"area1": count}
router.get('/counts', async (req: Request, res: Response) => {
    const status = nano.db.use('statuses');

    const tweet_count_area = await status.view('api-global', 'count-area', {
        group: true,
        reduce: true,
    });

    const ret: { [area: string]: any } = {};
    for (const area of tweet_count_area.rows) {
        ret[area.key] = area.value;
    }
    return res.json(ret);
});

// keyword for all areas
// sample output: {"area1": count}
router.get('/keyword/all', async (req: Request, res: Response) => {
    const keyword = req.query.keyword;
    const status = nano.db.use('statuses');

    const keyword_areas = await status.view('api-global', 'keyword', {
        start_key: [keyword],
        end_key: [keyword, {}],
        group: true,
        reduce: true,
    });

    const ret: { [area: string]: any } = {};
    for (const row of keyword_areas.rows) {
        ret[row.key[1]] = row.value;
    }
    return res.json(ret);
});

// keyword by area
// sample output: [text1, text2]
router.get('/keyword/:area', async (req: Request, res: Response) => {
    const status = nano.db.use('statuses');

    const keyword = req.query.keyword;
    const area = req.params.area;

    // selection of tweets with keyword (e.g. first 10)
    const tweets = await status.partitionedView(area, 'api', 'keyword', {
        key: keyword,
    });
    return res.json(tweets.rows.map((r) => r.value));
});

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

// exercise
router.get('/exercise', async (req: Request, res: Response) => {
    const status = nano.db.use('statuses');

    // exercise view
    const exercise = await status.view('api-global', 'exercise', {
        group: true,
        reduce: true,
    });
    // transform into area
    const transformed = view_bool_process(exercise.rows);

    const pyshell = new PythonShell('analysis/main.py', {
        mode: 'text',
        args: ['exercise'],
    });

    pyshell.on('message', (message) => {
        console.log(message);
    });
    pyshell.send(JSON.stringify(transformed));
    pyshell.end((err, code) => {
        if (err) throw err;
        console.log(code);
    });
});

export default router;
