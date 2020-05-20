import express, { Request, Response } from 'express';
const router = express.Router();
import nano from './app';

// Database status
router.get('/', async (req: Request, res: Response) => {
    // @ts-ignore
    const info = await nano.request({ path: '/' });
    return res.send(info);
});

// Gets and returns info about dbs
router.get('/dbs', async (req: Request, res: Response) => {
    const all_dbs = await nano.db.list();
    const db_info = [];
    for (const db of all_dbs) {
        const db_res = await nano.db.get(db);
        db_info.push(db_res);
    }
    return res.send(
        db_info.map((info) => {
            return {
                name: info.db_name,
                size: info.sizes.active,
                count: info.doc_count,
            };
        })
    );
});

// Gets users count with mapreduce
router.get('/stats', async (req, res) => {
    const users = nano.db.use('users');
    const user_view = await users.view('api-global', 'count', {
        include_docs: false,
    });
    const user_count = user_view['rows'][0]['value'];

    const status = nano.db.use('statuses');
    const status_view = await status.view('api-global', 'count', {
        include_docs: false,
    });
    const status_count = status_view['rows'][0]['value'];

    return res.send({ user: user_count, status: status_count });
});

// Day ordering
const day_order: { [day: string]: number } = {
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6,
    Sun: 7,
};

// General status (weekday/hour)
router.get('/general', async (req, res) => {
    const status = nano.db.use('statuses');

    // day of week
    const day_call = status.view('api-global', 'weekday', {
        reduce: true,
        group: true,
    });
    // hours
    const hour_call = status.view('api-global', 'hour', {
        reduce: true,
        group: true,
    });

    // perform call
    const [dayofweek, hours] = await Promise.all([day_call, hour_call]);

    return res.send({
        weekday: dayofweek.rows.sort(
            (a, b) => day_order[a.key] - day_order[b.key]
        ),
        hours: hours.rows,
    });
});

export default router;
