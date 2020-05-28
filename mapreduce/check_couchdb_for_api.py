"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import os
import json
import logging
import traceback
import sys
from pprint import pformat
from time import asctime, localtime, time
from tqdm import tqdm
from utils.database import CouchDB
from utils.logger import get_logger
from utils.aurin import save_aurin_data

logger = get_logger('AreasUpdater', logging.DEBUG)

couch = CouchDB()
date_time = asctime(localtime(time()))

backup_path = os.path.join(os.path.join("ddocs_backup", date_time))

count = 1
while os.path.exists(backup_path):
    backup_path = backup_path[:backup_path.find('-')]
    backup_path += str(count)
    count += 1
    if count > 10:
        logger.error("Cannot generate a valid  backup path")
        exit()


class View:
    def __init__(self, map_, reduce):
        self.map = map_
        self.reduce = reduce


def read_view_from_file(path):
    map_file = open(os.path.join(path, 'map.js'))
    map_func = map_file.read()
    map_file.close()
    map_func = map_func[12:]

    reduce_file = open(os.path.join(path, 'reduce.js'))
    reduce_func = reduce_file.read()
    reduce_file.close()
    reduce_func = reduce_func[15:]
    if reduce_func[0] == '_':
        reduce_func = reduce_func[:reduce_func.find(';')]
    return View(map_func, reduce_func)


def calc_bbox_of_polygon(polygon):
    min_x, min_y, max_x, max_y = polygon[0][0], polygon[0][1], polygon[0][0], polygon[0][1]
    for (x, y) in polygon:
        if x < min_x:
            min_x = x
        elif x > max_x:
            max_x = x

        if y < min_y:
            min_y = y
        elif y > max_y:
            max_y = y
    return [min_x, min_y, max_x, max_y]


def read_areas(path):
    areas_collection = {}
    filenames = os.listdir(path)
    for filename in filenames:
        abs_path = os.path.join(path, filename)
        if os.path.isfile(abs_path):
            with open(abs_path) as f:
                areas_f = json.loads(f.read())
                areas = areas_f['features']
                f.close()
                state_name = filename[:filename.find('.')]
                areas_collection[state_name] = areas
    return areas_collection


def preprocess(data_path):
    new_sa2_2016 = read_areas(data_path)

    states = [{"hit": 0, "state_name": None, "areas": []} for _ in range(len(new_sa2_2016))]
    # calc bbox for each area and store it in its state
    count = 0
    for key, areas in new_sa2_2016.items():
        states[count]["state_name"] = key
        area_id = 0
        for area in areas:
            if 'geometry' in area:
                states[count]["areas"].append(area)
                area_id += 1
            # use the left value to record its hit times
        count += 1

    return states


def calc_bbox_of_polygon(polygon):
    min_x, min_y, max_x, max_y = polygon[0][0], polygon[0][1], polygon[0][0], polygon[0][1]
    for (x, y) in polygon:
        if x < min_x:
            min_x = x
        elif x > max_x:
            max_x = x

        if y < min_y:
            min_y = y
        elif y > max_y:
            max_y = y
    return [min_x, min_y, max_x, max_y]


def centroid(polygon):
    x_sum, y_sum = 0, 0
    for coordinates in polygon:
        x_sum += coordinates[0]
        y_sum += coordinates[1]
    return round(x_sum / len(polygon), 5), round(y_sum / len(polygon), 5)


def update_areas():
    areas_json = []
    bulk = []
    if "areas" in couch.client.all_dbs():
        couch.client["areas"].delete()
    if "areas" not in couch.client.all_dbs():
        states = preprocess("data/SA2(2016)Level12")
        for state in states:
            del state['hit']
            for area in state['areas']:
                area["bboxes"] = []
                area["centroids"] = []
                for i, polygons in enumerate(area['geometry']['coordinates']):
                    area["bboxes"].append([])
                    area["centroids"].append([])
                    for j, polygon in enumerate(polygons):
                        area["bboxes"][i].append([])
                        area["centroids"][i].append([])
                        bbox = calc_bbox_of_polygon(polygon)
                        area["bboxes"][i][j].append(bbox)
                        area["centroids"][i][j].append(centroid(polygon))

                area['properties']['state_name'] = state['state_name']
                area['properties']['centroid'] = area["centroids"][0][0][0]
                areas_json.append(area)

        couch.client.create_database("areas", partitioned=False)
        no_where = {"_id": "australia",
                    "sa2_2016_lv12_code": "australia",
                    "sa2_2016_lv12_name": "In Australia But No Specific Location"}

        out_of_vitoria = {"_id": "out_of_australia",
                          "sa2_2016_lv12_code": "out_of_australia",
                          "sa2_2016_lv12_name": "Out of Australia"}

        couch.client["areas"].create_document(no_where)
        couch.client["areas"].create_document(out_of_vitoria)

        for area in tqdm(areas_json, unit=' areas'):
            area["_id"] = area['properties']['feature_code']
            bulk.append(area)
            if len(bulk) > 20:
                couch.client["areas"].bulk_docs(bulk)
                bulk = []
    if len(bulk):
        couch.client["areas"].bulk_docs(bulk)
    logger.info("Wrote {} areas in to couchdb".format(len(areas_json)))


def save_js(path, content):
    f = open(path, "w+")
    f.write(content)
    f.close()


def backup_design_docs(ddocs_to_backup):
    try:
        for db_name, ddoc_names in ddocs_to_backup.items():
            os.mkdir(os.path.join(backup_path, db_name))
            for ddoc_name in ddoc_names:
                ddoc_json_str = json.dumps(couch.client[db_name]['_design/' + ddoc_name])
                ddoc_json = json.loads(ddoc_json_str)
                del ddoc_json['_rev']
                ddoc_json_str = json.dumps(ddoc_json)
                save_js(os.path.join(backup_path, db_name, ddoc_name + ".json"), ddoc_json_str)
        logger.info("Backup done {}:\n{}".format(backup_path, pformat(ddocs_to_backup, depth=4)))
    except KeyError as e:
        logger.warning(str(e) + ' not exist')


def create_ddoc(db_name, local_ddoc_json_str):
    local_ddoc_json = json.loads(local_ddoc_json_str)
    couch.client[db_name].create_document(local_ddoc_json)
    logger.info("Created design doc:\n{}".format(pformat(local_ddoc_json)))


def update_ddoc(ddoc, local_ddoc_json_str):
    local_ddoc_json = json.loads(local_ddoc_json_str)
    for view_name, view_json_str in local_ddoc_json['views'].items():
        if view_name not in ddoc.views \
                or ddoc.views[view_name] != view_json_str:
            ddoc.views[view_name] = view_json_str
            ddoc.save()
            logger.info(
                "Updated design doc {}/{}:\n{}".format(ddoc.document_url, view_name, pformat(view_json_str)))


def check_all_dbs(ddocs_to_check):
    for db_name, ddoc_names in ddocs_to_backup.items():
        for ddoc_name, v in ddoc_names.items():
            f = open(os.path.join("couch", db_name, ddoc_name + '.json'))
            ddocs_to_check[db_name][ddoc_name] = f.read()
            f.close()
            if '_design/' + ddoc_name not in couch.client[db_name]:
                create_ddoc(db_name, ddocs_to_check[db_name][ddoc_name])
            else:
                update_ddoc(couch.client[db_name]['_design/' + ddoc_name],
                            ddocs_to_check[db_name][ddoc_name])
    logger.info("Finished design docs check")


if __name__ == '__main__':
    ddocs_to_backup = {
        'statuses': {
            'api': '',
            'api-global': '',
            'more': '',
            'more-global': '',
            'indicative': ''
        },
        'users': {
            'api-global': '',
            'tasks': ''
        },
        'aurin_homelessness': {
            'analysis': ''
        },
        'aurin_ier': {
            'analysis': ''
        },
        'aurin_ieo': {
            'analysis': ''
        }
    }

    try:
        if not os.path.exists('ddocs_backup'):
            os.mkdir('ddocs_backup')
        os.mkdir(backup_path)

        backup_design_docs(ddocs_to_backup)
        check_all_dbs(ddocs_to_backup)
        update_areas()
        save_aurin_data()

    except Exception:
        traceback.print_exc(file=sys.stdout)
