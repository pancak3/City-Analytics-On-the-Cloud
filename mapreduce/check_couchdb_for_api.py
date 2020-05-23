import os
import json
import logging
import traceback
import sys
from time import asctime, localtime, time
from tqdm import tqdm
from utils.database import CouchDB
from utils.logger import get_logger
from cloudant.design_document import DesignDocument, Document

logger = get_logger('AreasUpdater', logging.DEBUG)

couch = CouchDB()
date_time = asctime(localtime(time()))

backup_path = os.path.join(os.path.join("views_backup", date_time))

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


def centroid(bbox):
    return round((bbox[0] + bbox[2]) / 2, 5), round((bbox[1] + bbox[3]) / 2, 5)


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
                        area["centroids"][i][j].append(centroid(bbox))

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


def check_db(combined_db_name):
    os.mkdir(os.path.join("views_backup", date_time, combined_db_name))
    (db_name, partitioned) = combined_db_name.split('.')
    partitioned = True if partitioned == "partitioned" else False
    if db_name not in couch.client.all_dbs():
        couch.client.create_database(db_name, partitioned=partitioned)
        logger.info("Created database: {}".format(db_name))

    for ddoc_name in os.listdir(os.path.join("couch", combined_db_name)):
        if os.path.isdir(os.path.join("couch", combined_db_name, ddoc_name)):
            check_ddoc_in_db(combined_db_name, ddoc_name)


def check_ddoc_in_db(combined_db_name, combined_ddoc_name):
    os.mkdir(os.path.join("views_backup", date_time, combined_db_name, combined_ddoc_name))
    db_name = combined_db_name.split('.')[0]
    (ddoc_name, partitioned) = combined_ddoc_name.split('.')
    partitioned = True if partitioned == "partitioned" else False
    design_doc = Document(couch.client[db_name], '_design/' + ddoc_name)
    if not design_doc.exists():
        design_doc = DesignDocument(couch.client[db_name], '_design/' + ddoc_name, partitioned=partitioned)
        design_doc.save()
        logger.info("Created design document: {}".format(ddoc_name))

    for view_name in os.listdir(os.path.join("couch", combined_db_name, combined_ddoc_name)):
        if os.path.isdir(os.path.join("couch", combined_db_name, combined_ddoc_name, view_name)):
            check_a_single_view(combined_db_name, combined_ddoc_name, view_name)


def update_view(db_name, ddoc_name, view_name, local_view, partitioned):
    partitioned = True if partitioned == "partitioned" else False
    design_doc = couch.client[db_name]['_design/' + ddoc_name]
    design_doc.add_view(view_name, local_view.map, local_view.reduce, partitioned=partitioned)
    design_doc.save()
    logger.info("Updated view: {}/_design/{}/_view/{}".format(db_name, ddoc_name, view_name))


def save_js(path, content):
    f = open(path, "w+")
    f.write(content)
    f.close()


def check_a_single_view(combined_db_name, combined_ddoc_name, view_name):
    os.mkdir(os.path.join("views_backup", date_time, combined_db_name, combined_ddoc_name, view_name))

    db_name = combined_db_name.split('.')[0]
    (ddoc_name, partitioned) = combined_ddoc_name.split('.')
    partitioned = True if partitioned == "partitioned" else False
    local_view = read_view_from_file(os.path.join("couch", combined_db_name, combined_ddoc_name, view_name))
    design_doc = couch.client[db_name]['_design/' + ddoc_name]

    # backup

    if view_name in design_doc.views and 'reduce' in design_doc.views[view_name]:
        map_func = "const map = " + design_doc.views[view_name]['map']

        if design_doc.views[view_name]['reduce'][0] == '_':
            reduce_func = "const reduce = " + design_doc.views[view_name]['reduce'] + ';\n'
        else:
            reduce_func = "const reduce = " + design_doc.views[view_name]['reduce']
        save_js(os.path.join("views_backup",
                             date_time, combined_db_name, combined_ddoc_name, view_name, 'reduce.js'), reduce_func)

        save_js(os.path.join("views_backup",
                             date_time, combined_db_name, combined_ddoc_name, view_name, 'map.js'), map_func)

    if view_name in design_doc.views:
        if design_doc.views[view_name]['map'] != local_view.map \
                or ('reduce' in design_doc.views[view_name]
                    and design_doc.views[view_name]['reduce'] != local_view.reduce):
            design_doc.delete_view(view_name)
            design_doc.save()
            update_view(db_name, ddoc_name, view_name, local_view, partitioned)
    else:
        update_view(db_name, ddoc_name, view_name, local_view, partitioned)


def check_all_dbs():
    for db_name in os.listdir("couch"):
        if os.path.isdir(os.path.join("couch", db_name)):
            check_db(db_name)
    logger.info("Views full backup is under {}".format(backup_path))


if __name__ == '__main__':
    try:
        if not os.path.exists('views_backup'):
            os.mkdir('views_backup')
        os.mkdir(backup_path)
        update_areas()
        # check_all_dbs()
    except Exception:
        traceback.print_exc(file=sys.stdout)
