import fs from 'fs';
import { ServerScope } from "nano";

async function putAreas(nano: ServerScope): Promise<void> {
    // Read area file
    const file_path = 'data/VictoriaSuburb(ABS-2011)Geojson.json';
    const areas_file = fs.readFileSync(file_path, 'utf8');

    // Parse area file
    const areas_json = JSON.parse(areas_file).features;
    for (const i in areas_json) {
        areas_json[i]['_id'] = i.toString();
        areas_json[i]['area_name'] =
            areas_json[i]['properties']['feature_name'];
    }

    // Get all dbs
    const dbs = await nano.db.list();

    // If area db exists, delete
    if (dbs.includes('areas')) {
        await nano.db.destroy('areas');
    }

    // Create areas db
    await nano.db.create('areas');

    // Put area documents
    const areas = nano.db.use('areas');
    const doc_0 = {_id: '0', area_name: "No Where"};
    const doc_1 = {_id: '1', area_name: "Out of Victoria"};
    await areas.insert(doc_0);
    await areas.insert(doc_1);

    for (const doc of areas_json) {
        await areas.insert({ ...doc, _id: `${Number(doc._id) + 2}` });
    }
    console.log(`[-] Created ${areas_json.length} areas.`);
}

async function updateAreas(nano: ServerScope): Promise<void> {
    await putAreas(nano);
}

export default updateAreas;
