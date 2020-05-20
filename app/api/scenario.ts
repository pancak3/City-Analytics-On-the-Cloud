import { Router, Request, Response, json } from 'express';
import { PythonShell } from 'python-shell';
import nano from './app';

const router = Router();

// count by area (no filter keyword)
router.get('/count', (req: Request, res: Response) => {
    const status = nano.db.use('statuses');

    return res.json({});
});

// keyword for all areas
router.get('/keyword/:keyword/all', async (req: Request, res: Response) => {
    const keyword = req.params.keyword;
    const status = nano.db.use('statuses');

    const keyword_areas = await status.view('api-global', 'keyword', {
        start_key: [keyword],
        end_key: [keyword, {}],
        group: true,
        reduce: true,
    });
    const processed = keyword_areas.rows.map((row) => {
        return { area: row.key[1], value: row.value };
    });
    return res.json(processed);
});

// keyword by area
router.get('/keyword/:keyword/:area', (req: Request, res: Response) => {
    const status = nano.db.use('statuses');

    const keyword = req.params.keyword;
    const area = req.params.area;

    // selection of tweets with keyword (e.g. first 10)

    // const tweets = await status.partitionedView(area, )

    return res.json({});
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
        args: ['exercise']
    });

    pyshell.on('message', (message) => {
        console.log(message);
    })
    pyshell.send(JSON.stringify(transformed));
    pyshell.end((err, code) => {
        if (err) throw err;
        console.log(code);
    })
});

export default router;
