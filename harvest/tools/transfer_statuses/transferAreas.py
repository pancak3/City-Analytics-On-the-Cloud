import json
import os
from cloudant.client import Cloudant
from tqdm import tqdm
from database import CouchDB


def rank_areas(areas_in_states, state_idx, area_idx):
    areas_in_states[0][state_idx]['areas'][area_idx]['hit'] += 1
    source_idx = area_idx
    while area_idx and \
            areas_in_states[0][state_idx]['areas'][area_idx - 1]['hit'] < \
            areas_in_states[0][state_idx]['areas'][source_idx]['hit']:
        area_idx -= 1
    # switch
    if source_idx != area_idx:
        tmp = areas_in_states[0][state_idx]['areas'][area_idx]
        areas_in_states[0][state_idx]['areas'][area_idx] = areas_in_states[0][state_idx]['areas'][source_idx]
        areas_in_states[0][state_idx]['areas'][source_idx] = tmp

    areas_in_states[0][state_idx]['hit'] += 1
    source_idx = state_idx
    while state_idx and \
            areas_in_states[0][state_idx - 1]['hit'] < \
            areas_in_states[0][source_idx]['hit']:
        state_idx -= 1
    # switch
    if source_idx != state_idx:
        tmp = areas_in_states[0][state_idx]
        areas_in_states[0][state_idx] = areas_in_states[0][source_idx]
        areas_in_states[0][source_idx] = tmp


def retrieve_statuses_areas(doc_json, areas_in_states):
    doc = json.loads(doc_json)
    del doc_json
    del doc["_rev"]

    if 'lga2016_area_code' in doc:
        del doc['lga2016_area_code']

    if 'lga2016_area_name' in doc:
        del doc['lga2016_area_name']

    if 'abs2016_area_code' in doc:
        del doc['abs2016_area_code']

    if 'abs2016_area_name' in doc:
        del doc['abs2016_area_name']

    doc['sa2_2016_lv12_code'] = 'australia'
    doc['sa2_2016_lv12_name'] = 'Australia'
    if 'coordinates' in doc and doc['coordinates'] is not None and doc['coordinates']['type'] == 'Point':

        doc['sa2_2016_lv12_code'] = 'out_of_australia'
        doc['sa2_2016_lv12_name'] = 'Out of Australia'
        point_x, point_y = doc['coordinates']['coordinates']

        for i in range(len(areas_in_states[0])):
            # state
            if point_x < areas_in_states[0][i]['bbox'][0] or point_y < areas_in_states[0][i]['bbox'][1] \
                    or point_x > areas_in_states[0][i]['bbox'][2] or point_y > areas_in_states[0][i]['bbox'][3]:
                continue
            for j in range(len(areas_in_states[0][i]['areas'])):
                # areas
                is_inside = False
                for k in range(len(areas_in_states[0][i]['areas'][j]["coordinates"])):
                    # polygons
                    for m in range(len(areas_in_states[0][i]['areas'][j]["coordinates"][k])):
                        # coordinates of one polygon
                        min_x, min_y, max_x, max_y = areas_in_states[0][i]['areas'][j]["bboxes"][k]
                        if point_x < min_x or point_y < min_y or point_x > max_x or point_y > max_y:
                            continue
                        w = - 1
                        n = 0
                        length = len(areas_in_states[0][i]['areas'][j]["coordinates"][k][m])
                        while n < length:
                            if (areas_in_states[0][i]['areas'][j]["coordinates"][k][m][n][1] > point_y) \
                                    != (areas_in_states[0][i]['areas'][j]["coordinates"][k][m][w][1] > point_y) \
                                    and point_x < \
                                    (areas_in_states[0][i]['areas'][j]["coordinates"][k][m][w][0] -
                                     areas_in_states[0][i]['areas'][j]["coordinates"][k][m][n][0]) * \
                                    (point_y - areas_in_states[0][i]['areas'][j]["coordinates"][k][m][n][1]) / \
                                    (areas_in_states[0][i]['areas'][j]["coordinates"][k][m][w][1] -
                                     areas_in_states[0][i]['areas'][j]["coordinates"][k][m][n][1]) \
                                    + areas_in_states[0][i]['areas'][j]["coordinates"][k][m][n][0]:
                                is_inside = not is_inside
                            n += 1
                            w += 1

                if is_inside:
                    doc['sa2_2016_lv12_code'] = areas_in_states[0][i]['areas'][j]['feature_code']
                    doc['sa2_2016_lv12_name'] = areas_in_states[0][i]['areas'][j]['feature_name']
                    doc['sa2_2016_lv12_state'] = areas_in_states[0][i]['state_name']
                    doc['_id'] = doc['sa2_2016_lv12_code'] + ':' + doc['id_str']
                    rank_areas(areas_in_states, i, j)
                    return doc

    return doc


def transfer_abs2011_to_lga2016(areas_in_states, source_db, target_db):
    bulk = []
    for doc in tqdm(source_db, total=source_db.doc_count(), desc="Updating doc"):
        new_doc = retrieve_statuses_areas(json.dumps(doc), areas_in_states)
        if 'sa2_2016_lv12_code' in new_doc and new_doc['sa2_2016_lv12_code'] not in {'australia', 'out_of_australia'}:
            if new_doc['_id'] not in target_db:
                bulk.append(new_doc)
                if len(bulk) > 500:
                    target_db.bulk_docs(bulk)
                    bulk = []


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


def calc_bbox_of_state(areas):
    bboxes = []
    for area in areas:
        for bbox in area['bboxes']:
            bboxes.append(bbox)

    min_x, min_y, max_x, max_y = bboxes[0][0], bboxes[0][1], bboxes[0][2], bboxes[0][3]
    for (p0, p1, p2, p3) in bboxes:
        if p0 < min_x:
            min_x = p0
        if p2 > max_x:
            max_x = p2
        if p1 < min_y:
            min_y = p1
        if p3 > max_y:
            max_y = p3

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


def preprocess():
    new_sa2_2016 = read_areas("data")

    states = [{"hit": 0, "state_name": None, "bbox": [], "areas": []} for _ in range(len(new_sa2_2016))]
    # calc bbox for each area and store it in its state
    count = 0
    for key, areas in new_sa2_2016.items():
        states[count]["state_name"] = key
        area_id = 0
        for area in areas:
            if 'geometry' in area:
                states[count]["areas"].append({'hit': 0,
                                               "feature_code": area['properties']["feature_code"],
                                               'feature_name': area['properties']['feature_name'],
                                               'coordinates': area['geometry']['coordinates'],
                                               'bboxes': []})
                for i, polygons in enumerate(area['geometry']['coordinates']):
                    for j, polygon in enumerate(polygons):
                        states[count]["areas"][area_id]["bboxes"].append(
                            calc_bbox_of_polygon(polygon))
                area_id += 1
            # use the left value to record its hit times
        count += 1

    for key, v in enumerate(states):
        states[key]["bbox"] = calc_bbox_of_state(v['areas'])

    return states


if __name__ == '__main__':
    areas_in_states = [preprocess()]
    source_couch = Cloudant('admin', 'password', url='http://127.0.0.1:5983/', connect=True)
    source_couch.connect()

    target_couch = Cloudant('admin', 'password', url='http://127.0.0.1:5984/', connect=True)
    target_couch.connect()

    transfer_abs2011_to_lga2016(areas_in_states, source_couch['statuses'], target_couch['statuses'])
