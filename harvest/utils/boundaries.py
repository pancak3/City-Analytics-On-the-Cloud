import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Boundaries')
logger.setLevel(logging.DEBUG)


class Area:
    def __init__(self, code, coordinates):
        self.code = code
        self.coordinates = coordinates


class Boundaries:
    def __init__(self, filepath):
        try:
            file = open(filepath)
            self.json = json.loads(file.read())
        except IOError:
            logger.warning("[*] {} invalid.".format(filepath))
        finally:
            self.areas = {}
            self.bds = []
            self.to_dict()

    def to_dict(self):
        for feature in self.json['features']:
            self.areas[feature["properties"]['feature_name']] = Area(feature["properties"]['feature_code'],
                                                                     feature['geometry']['coordinates'])
        for area in self.areas.items():
            for polygons in area[1].coordinates:
                for polygon in polygons:
                    self.bds.append(polygon)


if __name__ == '__main__':
    bds_melbourne = Boundaries("../data/MelbourneGeojson.json")
