const fs = require('fs');
const path = require('path');
const axios = require('axios');
const util = require('util');
const readDir = util.promisify(fs.readdir);
// Environment for local debug
if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config();
}

// DB setup
const db_prot = process.env.DB_PROT || 'http';
const db_username = process.env.DB_USERNAME;
const db_password = process.env.DB_PASSWORD;
const db_port = process.env.DB_PORT || 5984;
const db_host = process.env.DB_HOST || 'couchdb';
const base_url = `${db_prot}://${db_username}:${db_password}@${db_host}:${db_port}`;


function readMapReduce(file_path) {
    const map = fs.readFileSync(path.join(file_path, 'map.js'), 'utf8').substring(12);
    const reduce = fs.readFileSync(path.join(file_path, 'reduce.js'), 'utf8').substring(15);
    return {map, reduce}
}

async function getFileViews() {
    const views_path = path.join(process.cwd(), 'couch');
    let file_api_views = [];

    const ddoc_names = await readDir(views_path);
    for (const ddoc_name of ddoc_names) {
        const ddoc_path = path.join(views_path, ddoc_name);
        const stats = fs.statSync(ddoc_path);
        if (stats.isDirectory()) {
            const view_names = await readDir(ddoc_path);
            file_api_views[ddoc_name] = {};
            for (const view_name of view_names) {
                file_api_views[ddoc_name][view_name] = readMapReduce(path.join(ddoc_path, view_name));
            }
        }
    }
    return file_api_views;
}

async function createDDoc(ddoc_name, views) {
    const put_url = `${base_url}/${ddoc_name}/_design/api`;
    const put_data =
        {
            "_id": "_design/api",
            "views": views,
            "language": "javascript"
        }
    const create_ddoc_res = await axios.put(put_url, put_data);
    if (create_ddoc_res.status !== 201) {
        console.log(create_ddoc_res);
        process.exit(0);
    } else {
        console.log(`[-] Created ${ddoc_name}/_design/api/\n\t${JSON.stringify(put_data)}`);
    }
}

async function getCouchDDocs(fileViews) {

    let users_api_views;
    let statuses_api_views;

    const users_ddocs_res = await axios.get(`${base_url}/users/_all_docs?inclusive_end=false&start_key=%22_design%22&end_key=%22_design0%22`);
    let user_ddocs = new Set();
    for (const view of users_ddocs_res.data.rows) {
        user_ddocs.add(view.id);
    }

    if (!user_ddocs.has('_design/api')) {
        await createDDoc('users', fileViews['users']);
    }

    const users_api_res = await axios.get(`${base_url}/users/_design/api`);
    users_api_views = users_api_res.data;


    const statuses_ddocs_res = await axios.get(`${base_url}/statuses/_all_docs?inclusive_end=false&start_key=%22_design%22&end_key=%22_design0%22`);
    let statuses_ddocs = new Set();
    for (const view of statuses_ddocs_res.data.rows) {
        statuses_ddocs.add(view.id);
    }
    if (!statuses_ddocs.has('_design/api')) {
        await createDDoc('statuses', fileViews['statuses']);
    }
    const statuses_api_res = await axios.get(`${base_url}/statuses/_design/api`);
    statuses_api_views = statuses_api_res.data;

    return {'users': users_api_views, 'statuses': statuses_api_views}
}

async function updateView(doc_name, couch_doc, view_name, view) {
    const put_url = `${base_url}/${doc_name}/_design/api`;

    couch_doc.views[view_name] = view;
    const update_res = await axios.put(put_url, couch_doc);
    if (update_res.status !== 201) {
        console.log(update_res);
        process.exit(0);
    } else {
        // console.log(`[-] Updated ${doc_name}/_design/api/_view/${view_name}:\n\t${JSON.stringify(view)}`);
        console.log(`[-] Updated ${doc_name}/_design/api/_view/${view_name}`);
    }


}

async function checkSingleView(doc_name, view_name, file_view, couch_ddocs) {

    const couch_view = couch_ddocs[doc_name].views[view_name];
    if (JSON.stringify(couch_view) !== JSON.stringify(file_view)) {
        await updateView(doc_name, couch_ddocs[doc_name], view_name, file_view);
    }

}

async function updateViews(file_views, couch_ddocs) {
    //
    for (let [file_ddoc_name, file_ddoc] of Object.entries(file_views)) {
        for (let [view_name, view] of Object.entries(file_ddoc)) {
            await checkSingleView(file_ddoc_name, view_name, view, couch_ddocs);
        }
    }
    return true;
}

async function checkViews() {
    const fileViews = await getFileViews();
    const couchDDocs = await getCouchDDocs(fileViews);
    await updateViews(fileViews, couchDDocs);
    console.log("[-] Checked all views.")
    return true;
}

module.exports = checkViews;
