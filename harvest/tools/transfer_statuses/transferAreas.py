import json
from database import CouchDB


def retrieve_statuses_areas(doc_json, areas_collection):
    doc = json.loads(doc_json)
    if "area_code" not in doc:
        return False, None
    del doc_json
    del doc["_rev"]

    doc['abs2011_area_code'] = doc['area_code']
    doc['abs2011_area_name'] = doc['area_name']

    del doc['area_code']
    del doc['area_name']
    doc['lga2016_area_code'] = 'australia'
    doc['lga2016_area_name'] = 'Australia'
    if doc['coordinates'] is not None and doc['coordinates']['type'] == 'Point':

        doc['lga2016_area_code'] = 'out_of_australia'
        doc['lga2016_area_name'] = 'Out of Australia'
        point_x, point_y = doc['coordinates']['coordinates']

        for areas in areas_collection:
            for location in areas:
                geometry = location['geometry']
                if geometry['type'] == "MultiPolygon":
                    is_inside = False
                    for polygons in geometry["coordinates"]:
                        for polygon in polygons:
                            min_x, max_x = polygon[0][0], polygon[0][0]
                            min_y, max_y = polygon[0][1], polygon[0][1]
                            for x, y in polygon[1:]:
                                if x < min_x:
                                    min_x = x
                                elif x > max_x:
                                    max_x = x
                                if y < min_y:
                                    min_y = y
                                elif y > max_y:
                                    max_y = y
                            if point_x < min_x or point_x > max_x \
                                    or point_y < min_y or point_y > max_y:
                                continue
                            j = len(polygon) - 1
                            for i in range(len(polygon)):
                                if (polygon[i][1] > point_y) != (polygon[j][1] > point_y) \
                                        and point_x < \
                                        (polygon[j][0] - polygon[i][0]) * \
                                        (point_y - polygon[i][1]) / \
                                        (polygon[j][1] - polygon[i][1]) \
                                        + polygon[i][0]:
                                    is_inside = not is_inside
                    if is_inside:
                        doc['lga2016_area_code'] = location["properties"]["feature_code"]
                        doc['lga2016_area_name'] = location["properties"]["feature_name"]
                        doc['_id'] = doc['lga2016_area_code'] + doc['_id'][doc['_id'].find(':'):]
                        return True, doc
        return False, doc

    else:
        return False, doc


def transfer_abs2011_to_lga2016():
    # f = open("data/VictoriaSuburb(ABS-2011)Geojson.json")
    # abs2011_file_json = json.loads(f.read())
    # f.close()
    # abs2011 = abs2011_file_json["features"]

    f = open("all.json")
    lga2016_file_json = json.loads(f.read())
    f.close()
    lga2016 = lga2016_file_json

    couch = CouchDB()
    transfer_statues = couch.client["transfer-statues"]
    statuses = couch.client["statuses"]
    for doc in statuses:
        if doc['_id'][0] != '_':
            doc.delete()
    for doc in transfer_statues:
        flag, new_doc = retrieve_statuses_areas(json.dumps(doc), lga2016)
        if flag and new_doc['lga2016_area_code'] not in {"no_where", "australia"}:
            if new_doc['_id'] not in statuses:
                statuses.create_document(new_doc)
            else:
                statuses[new_doc['_id']].delete()
                statuses.create_document(new_doc)
            print("[*] transferred {}".format(new_doc["_id"]))
    return


if __name__ == '__main__':
    transfer_abs2011_to_lga2016()
    pass
